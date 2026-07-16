# ------------------ Step 03: PRA Subcatchments ------------------------- #
#
# Purpose :
#     Delineate hydrologically meaningful subcatchment units from the DEM
#     using flow-routing and basin-extraction algorithms (WhiteboxTools).
#     These subcatchments are later used to segment PRA polygons into
#     terrain-consistent units.
#
# Inputs :
#     - DEM (from [MAIN])
#
# Outputs :
#     - subcatchments.tif / subcatchments_smoothed.tif
#     - subcatchments.shp / subcatchments_smoothed.shp
#       (depending on smoothing and configuration settings)
#
# Config :
#     [praSUBCATCHMENTS]
#         • streamThreshold
#         • minLength
#         • smoothingWindowSize
#         • weightedSlopeFlow
#
# Consumes :
#     - DEM raster
#
# Provides :
#     - Subcatchment polygons required for:
#         • Step 05 (PRA Segmentation)
#         • Subcatchment-aware PRA refinement in downstream steps
#
# Author :
#     Christoph Hesselbach
#
# Institution :
#     Austrian Research Centre for Forests (BFW)
#     Department of Natural Hazards | Snow and Avalanche Unit
#
# Date & Version :
#   2025-11 - 1.0
#
# ----------------------------------------------------------------------- #


import os
import time
import logging
import warnings
from typing import cast
import pathlib

import geopandas as gpd
from shapely.validation import make_valid
from whitebox.whitebox_tools import WhiteboxTools

import avaframe.in1Data.getInput as getInput
import avaframe.in3Utils.cfgUtils as cfgUtils

import ati
from ati.mod0Helper.dataUtils import timeIt, relPath

# ------------------ Setup ------------------ #

log = logging.getLogger(__name__)
wbt = WhiteboxTools()

logging.getLogger("pyogrio").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", category=RuntimeWarning, module=r"pyogrio\..*")

# ------------------ Utility Functions ------------------ #


def ensureDir(path):
    os.makedirs(path, exist_ok=True)
    return path


def buildPath(dirPath, *names):
    return os.path.join(dirPath, *map(str, names))


def parseIntList(val):
    if isinstance(val, (list, tuple)):
        return [int(v) for v in val]
    val = str(val).strip()
    if "," in val:
        return [int(v.strip()) for v in val.split(",") if v.strip()]
    return [int(val)]


def fixInvalidGeometries(shpPath: str) -> None:
    """Fix invalid geometries in-place for a Shapefile."""
    log.debug("Fixing invalid geometries: %s", os.path.basename(shpPath))
    gdf = cast(gpd.GeoDataFrame, gpd.read_file(shpPath))
    gdf["geometry"] = gdf.geometry.apply(make_valid)
    gdf.to_file(shpPath)  # type: ignore[attr-defined]


def runWhiteboxTool(label, outputPath, tool, *args):
    """Run a Whitebox tool and fail at the operation that did not create its output."""
    messages = []
    returnCode = tool(*args, callback=messages.append)
    outputPath = pathlib.Path(outputPath)
    if returnCode != 0 or not outputPath.exists():
        details = "\n".join(str(message) for message in messages[-10:]).strip()
        message = (
            f"WhiteboxTools {label} failed (return code {returnCode}); "
            f"expected output was not created: {outputPath}"
        )
        if details:
            message = f"{message}\nWhiteboxTools output:\n{details}"
        raise RuntimeError(message)
    return outputPath


def resolveDemPath(demValue, inputDir):
    cand = demValue.strip()
    if os.path.isabs(cand) and os.path.exists(cand):
        return cand
    p1 = buildPath(inputDir, cand)
    if os.path.exists(p1):
        return p1
    root, ext = os.path.splitext(cand)
    if not ext:
        p2 = buildPath(inputDir, cand + ".tif")
        if os.path.exists(p2):
            return p2
    raise FileNotFoundError(f"DEM not found: {cand} under inputDir: {inputDir}")


# ------------------ Hydro Prep ------------------ #


def prepHydroGrids(demPath, outDir, weightedSlopeFlow=False):
    """Fill DEM, derive flow direction/accumulation, and slope."""
    filledDem = buildPath(outDir, "filled_DEM.tif")
    flowDir = buildPath(outDir, "flow_direction.tif")
    flowAcc = buildPath(outDir, "flow_accumulation.tif")
    slopeTif = buildPath(outDir, "slope.tif")

    with timeIt("Hydro prep (fill/d8/fac/slope)"):
        runWhiteboxTool("FillDepressions", filledDem, wbt.fill_depressions, demPath, filledDem)
        runWhiteboxTool("D8Pointer", flowDir, wbt.d8_pointer, filledDem, flowDir)
        runWhiteboxTool("D8FlowAccumulation", flowAcc, wbt.d8_flow_accumulation, filledDem, flowAcc)
        runWhiteboxTool("Slope", slopeTif, wbt.slope, filledDem, slopeTif)

    if weightedSlopeFlow:
        log.debug("Using slope-weighted flow accumulation")
        weightedFlow = buildPath(outDir, "weighted_flow.tif")
        runWhiteboxTool("Multiply", weightedFlow, wbt.multiply, flowAcc, slopeTif, weightedFlow)
        return filledDem, flowDir, weightedFlow, "_weighted"

    log.debug("Using unweighted flow accumulation")
    return filledDem, flowDir, flowAcc, "_unweighted"


# ------------------ Parameter Run ------------------ #


def runParamSet(
    flowDir,
    flowAccToUse,
    outDir,
    streamThreshold,
    minLength,
    smoothingWindowSize,
    flowSuffix,
):
    """Run subcatchment delineation and export shapefiles."""
    log.info(
        "...streamThr=%s, minLen=%s, smooth=%s, weightedFlow=%s",
        streamThreshold,
        minLength,
        smoothingWindowSize,
        flowSuffix,
    )

    # Output paths
    subcatchTif = buildPath(
        outDir,
        f"subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.tif",
    )
    subcatchShp = buildPath(
        outDir,
        f"subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp",
    )
    smoothSubcatchTif = buildPath(
        outDir,
        f"smooth_subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.tif",
    )
    smoothSubcatchShp = buildPath(
        outDir,
        f"subcatchments_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp",
    )
    nonSmoothSubcatchShp = buildPath(
        outDir,
        f"subcatchments_non_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp",
    )

    # 1) Extract streams
    streamsTif = buildPath(outDir, f"streams_{streamThreshold}.tif")
    with timeIt("Extract streams"):
        runWhiteboxTool(
            "ExtractStreams", streamsTif, wbt.extract_streams, flowAccToUse, streamsTif, streamThreshold
        )

    # 2) Flow-based watershed delineation
    junctionsTif = buildPath(outDir, f"junctions_{streamThreshold}.tif")
    with timeIt("Watershed delineation"):
        runWhiteboxTool(
            "StreamLinkIdentifier",
            junctionsTif,
            wbt.stream_link_identifier,
            flowDir,
            streamsTif,
            junctionsTif,
        )
        runWhiteboxTool("Watershed", subcatchTif, wbt.watershed, flowDir, junctionsTif, subcatchTif)

    # 3) Raster→Vector (raw)
    with timeIt("Raster→Vector (raw)"):
        runWhiteboxTool(
            "RasterToVectorPolygons (raw)",
            subcatchShp,
            wbt.raster_to_vector_polygons,
            subcatchTif,
            subcatchShp,
        )
        fixInvalidGeometries(subcatchShp)

    # 4) Smooth + Vectorize
    with timeIt("Smooth + Vectorize"):
        runWhiteboxTool(
            "MajorityFilter",
            smoothSubcatchTif,
            wbt.majority_filter,
            subcatchTif,
            smoothSubcatchTif,
            smoothingWindowSize,
        )
        runWhiteboxTool(
            "RasterToVectorPolygons (smoothed)",
            smoothSubcatchShp,
            wbt.raster_to_vector_polygons,
            smoothSubcatchTif,
            smoothSubcatchShp,
        )
        fixInvalidGeometries(smoothSubcatchShp)

    # 5) Non-smoothed vector
    with timeIt("Non-smoothed vector"):
        runWhiteboxTool(
            "RasterToVectorPolygons (non-smoothed)",
            nonSmoothSubcatchShp,
            wbt.raster_to_vector_polygons,
            subcatchTif,
            nonSmoothSubcatchShp,
        )
        fixInvalidGeometries(nonSmoothSubcatchShp)

    log.info("Saved subcatchments: %s", os.path.basename(smoothSubcatchShp))


# ------------------ Main ------------------ #


def runSubcatchments(cfg, workFlowDir=None, avaDir=None):
    """Step 03: Subcatchment delineation."""
    tAll = time.perf_counter()

    if workFlowDir is not None:
        cairosDir = workFlowDir["cairosDir"]
        inputDir = pathlib.Path(workFlowDir["inputDir"])
        outputDir = pathlib.Path(workFlowDir["praSubcatchmentsDir"])
    elif avaDir is not None:
        avaDir = pathlib.Path(avaDir)
        cairosDir = avaDir
        inputDir = avaDir / "Inputs"
        outputDir = avaDir / "Work" / "praSubcatchments"
    else:
        message = "A workflowDir or an avaDir needs to be provided."
        log.error(message)
        raise ValueError(message)
    os.makedirs(outputDir, exist_ok=True)
    # make outputDir absolute
    outputDir = outputDir.resolve()

    if cfg["MAIN"].getboolean("customPaths"):
        demName = cfg["MAIN"]["DEM"]
        demPath = inputDir / demName
    else:
        demPath = getInput.getDEMPath(avaDir)

    relDemPath = relPath(demPath, cairosDir)

    subcCfg = cfg["praSUBCATCHMENTS"]
    streamThresholdList = parseIntList(subcCfg.get("streamThreshold", "500"))
    minLengthList = parseIntList(subcCfg.get("minLength", "200"))
    smoothingWindowSizeList = parseIntList(subcCfg.get("smoothingWindowSize", "5"))
    weightedSlopeFlow = subcCfg.getboolean("weightedSlopeFlow", fallback=False)

    log.info(
        "Step 03: Subcatchments using DEM=./%s, streamThr=%s, minLen=%s, smooth=%s, weightedFlow=%s",
        relDemPath,
        ",".join(map(str, streamThresholdList)),
        ",".join(map(str, minLengthList)),
        ",".join(map(str, smoothingWindowSizeList)),
        weightedSlopeFlow,
    )

    _, flowDir, flowAccToUse, flowSuffix = prepHydroGrids(
        demPath.resolve(), outputDir, weightedSlopeFlow=weightedSlopeFlow
    )

    for thr in streamThresholdList:
        for ml in minLengthList:
            for sw in smoothingWindowSizeList:
                runParamSet(flowDir, flowAccToUse, outputDir, thr, ml, sw, flowSuffix)

    log.info("Step 03: Subcatchments complete in %.2fs", time.perf_counter() - tAll)


if __name__ == "__main__":
    # get main config file for avalanche dir
    modPath = pathlib.Path(ati.__file__).resolve().parent
    cfgNameFile = modPath / "atiCfg.ini"
    cfgMain = cfgUtils.getGeneralConfig(nameFile=cfgNameFile)

    # get praDelineation config file
    cfg = cfgUtils.getModuleConfig(modPath / "mod1Release" / "mod1Release")

    runSubcatchments(cfg, avaDir=cfgMain["MAIN"]["avalancheDirectory"])
