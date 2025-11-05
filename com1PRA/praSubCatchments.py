# ------------------ Step 03: PRA Subcatchments ------------------ #
# Purpose: Delineate subcatchments from DEM using WhiteboxTools.
# Inputs:  [MAIN] DEM
# Outputs: subcatchments_xxx.tif/.shp (smoothed and non-smoothed)
# Config:  [praSUBCATCHMENTS]
# Consumes: DEM
# Provides: subcatchment shapefiles for later PRA segmentation

import os
import sys
import time
import pathlib
import logging
import warnings
import contextlib
from typing import cast

import geopandas as gpd
from shapely.validation import make_valid
from whitebox.whitebox_tools import WhiteboxTools

from in1Utils.dataUtils import timeIt, relPath

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

@contextlib.contextmanager
def suppressWbtOutput():
    """Silence WhiteboxTools stdout."""
    old_stdout = sys.stdout
    try:
        sys.stdout = open(os.devnull, "w")
        yield
    finally:
        try:
            sys.stdout.close()
        finally:
            sys.stdout = old_stdout

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
    flowDir   = buildPath(outDir, "flow_direction.tif")
    flowAcc   = buildPath(outDir, "flow_accumulation.tif")
    slopeTif  = buildPath(outDir, "slope.tif")

    with timeIt("Hydro prep (fill/d8/fac/slope)"):
        with suppressWbtOutput():
            wbt.fill_depressions(demPath, filledDem)
            wbt.d8_pointer(filledDem, flowDir)
            wbt.d8_flow_accumulation(filledDem, flowAcc)
            wbt.slope(filledDem, slopeTif)

    if weightedSlopeFlow:
        log.debug("Using slope-weighted flow accumulation")
        weightedFlow = buildPath(outDir, "weighted_flow.tif")
        with suppressWbtOutput():
            wbt.multiply(flowAcc, slopeTif, weightedFlow)
        return filledDem, flowDir, weightedFlow, "_weighted"

    log.debug("Using unweighted flow accumulation")
    return filledDem, flowDir, flowAcc, "_unweighted"

# ------------------ Parameter Run ------------------ #

def runParamSet(flowDir,
                flowAccToUse,
                outDir,
                streamThreshold,
                minLength,
                smoothingWindowSize,
                flowSuffix):
    """Run subcatchment delineation and export shapefiles."""
    log.info("...streamThr=%s, minLen=%s, smooth=%s, weightedFlow=%s",
             streamThreshold, minLength, smoothingWindowSize, flowSuffix)

    # Output paths
    subcatchTif          = buildPath(outDir, f"subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.tif")
    subcatchShp          = buildPath(outDir, f"subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp")
    smoothSubcatchTif    = buildPath(outDir, f"smooth_subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.tif")
    smoothSubcatchShp    = buildPath(outDir, f"subcatchments_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp")
    nonSmoothSubcatchShp = buildPath(outDir, f"subcatchments_non_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp")

    # 1) Extract streams
    with timeIt("Extract streams"):
        with suppressWbtOutput():
            wbt.extract_streams(flowAccToUse, buildPath(outDir, f"streams_{streamThreshold}.tif"), streamThreshold)

    # 2) Flow-based watershed delineation
    with timeIt("Watershed delineation"):
        with suppressWbtOutput():
            wbt.stream_link_identifier(flowDir, buildPath(outDir, f"streams_{streamThreshold}.tif"), buildPath(outDir, f"junctions_{streamThreshold}.tif"))
            wbt.watershed(flowDir, buildPath(outDir, f"junctions_{streamThreshold}.tif"), subcatchTif)

    # 3) Raster→Vector (raw)
    with timeIt("Raster→Vector (raw)"):
        with suppressWbtOutput():
            wbt.raster_to_vector_polygons(subcatchTif, subcatchShp)
        fixInvalidGeometries(subcatchShp)

    # 4) Smooth + Vectorize
    with timeIt("Smooth + Vectorize"):
        with suppressWbtOutput():
            wbt.majority_filter(subcatchTif, smoothSubcatchTif, smoothingWindowSize)
            wbt.raster_to_vector_polygons(smoothSubcatchTif, smoothSubcatchShp)
        fixInvalidGeometries(smoothSubcatchShp)

    # 5) Non-smoothed vector
    with timeIt("Non-smoothed vector"):
        with suppressWbtOutput():
            wbt.raster_to_vector_polygons(subcatchTif, nonSmoothSubcatchShp)
        fixInvalidGeometries(nonSmoothSubcatchShp)

    log.info("Saved subcatchments: %s", os.path.basename(smoothSubcatchShp))

# ------------------ Main ------------------ #

def runSubcatchments(cfg, workFlowDir):
    """Step 03: Subcatchment delineation."""
    tAll = time.perf_counter()

    inputDir  = workFlowDir["inputDir"]
    outputDir = ensureDir(workFlowDir["praSubcatchmentsDir"])
    cairosDir = workFlowDir["cairosDir"]

    demValue   = cfg["MAIN"]["DEM"]
    demPathAbs = resolveDemPath(demValue, inputDir)
    relDemPath = relPath(demPathAbs, cairosDir)

    subcCfg = cfg["praSUBCATCHMENTS"]
    streamThresholdList     = parseIntList(subcCfg.get("streamThreshold", "500"))
    minLengthList           = parseIntList(subcCfg.get("minLength", "200"))
    smoothingWindowSizeList = parseIntList(subcCfg.get("smoothingWindowSize", "5"))
    weightedSlopeFlow       = subcCfg.getboolean("weightedSlopeFlow", fallback=False)

    log.info(
        "Step 03: Subcatchments using DEM=./%s, streamThr=%s, minLen=%s, smooth=%s, weightedFlow=%s",
        relDemPath,
        ",".join(map(str, streamThresholdList)),
        ",".join(map(str, minLengthList)),
        ",".join(map(str, smoothingWindowSizeList)),
        weightedSlopeFlow,
    )

    _, flowDir, flowAccToUse, flowSuffix = prepHydroGrids(
        demPathAbs, outputDir, weightedSlopeFlow=weightedSlopeFlow
    )

    for thr in streamThresholdList:
        for ml in minLengthList:
            for sw in smoothingWindowSizeList:
                runParamSet(flowDir, flowAccToUse, outputDir, thr, ml, sw, flowSuffix)

    log.info("Step 03: Subcatchments complete in %.2fs", time.perf_counter() - tAll)
