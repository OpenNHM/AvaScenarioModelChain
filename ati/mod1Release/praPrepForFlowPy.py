# ------------------ Step 07: PRA Preparation for FlowPy ---------------- #
#
# Purpose :
#     Convert enriched PRA polygons (from Step 06) into deterministic,
#     FlowPy-ready input datasets. This includes:
#         1) Grouping PRA features by elevation band and size class into
#            unique ID-based GeoJSONs
#         2) Rasterizing selected PRA attributes (area, PRA ID, etc.)
#            according to configuration options
#         3) Optionally deriving additional PRA boundary rasters for
#            improved FlowPy computational efficiency
#
# Inputs :
#     - Step 06 outputs:
#           *-ElevBands-Sized.geojson
#     - DEM (from [MAIN])
#     - BOUNDARY polygon (from [MAIN])
#
# Outputs :
#     Main model chain: ./08_praPrepForFlowPy/
#     AvaFrame-directory workflow: <avaDir>/Work/praPrepForFlowPy/
#         <band-size>_praID.geojson
#         <band-size>_<attribute>.tif
#         (optional) <band-size>_boundary.tif
#
# Config :
#     [praPREPFORFLOWPY]   Rasterization options and boundary settings
#     [praASSIGNELEV]      Elevation band definitions
#     [praSEGMENTATION]    Size class configuration
#     [praSUBCATCHMENTS]   Subcatchment metadata (optional)
#
# Consumes :
#     - GeoJSONs produced in Step 06 (size + elevation classified PRAs)
#
# Provides :
#     - Raster- and vector-prepared PRA datasets required for:
#         • Step 08 (Make Big Data Structure)
#         • FlowPy parameter generation and simulation
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
import re
import glob
import time
import logging
import numpy as np
import geopandas as gpd
import pathlib

import avaframe.in1Data.getInput as getInput
import avaframe.in3Utils.cfgUtils as cfgUtils


import ati
from ati.mod0Helper import dataUtils
from ati.mod0Helper.cfgUtils import loadElevationBands
from ati.mod0Helper.dataUtils import relPath, timeIt

# ------------------ Logging setup ------------------ #

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)

# ------------------ Minimal helpers ------------------ #


# ------------------ I/O discovery helpers ------------------ #


def _findStep07Inputs(
    praAssignElevSizeDir,
    assignElevSize,
    sizeFilter,
):
    """Locate flat Step 06 outputs for the selected workflow input type."""
    inDir = pathlib.Path(praAssignElevSizeDir)
    if assignElevSize:
        pattern = "*-ElevBands-Sized.geojson"
    else:
        pattern = f"*sizeF{int(sizeFilter)}.geojson"
    inFiles = sorted(glob.glob(os.path.join(inDir, pattern)))
    return inDir, inFiles


# ------------------ Rasterization ------------------ #


def _rasterizeAllVectors(outDir, demPath, boundPath, cfg, cairosDir):
    """Rasterize GeoJSONs in outDir per [praPREPFORFLOWPY] flags."""
    sect = cfg["praPREPFORFLOWPY"]
    enablePRA = sect.getboolean("enableRasterizePRA", fallback=False)
    enableID = sect.getboolean("enableRasterizeID", fallback=False)
    compress = sect.getboolean("compressOutputs", fallback=True)

    if not (enablePRA or enableID):
        log.info("Step 07: Rasterization disabled by INI flags.")
        return 0, 0

    modePRA = sect.get("rasterizeModePRA", fallback="attribute").lower()
    attributePRA = sect.get("rasterizeAttributePRA", fallback="praAreaM")
    attributeID = sect.get("rasterizeAttributeID", fallback="praID")
    allTouched = False

    # Boundary projected to DEM CRS
    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demCrs = demProfile["crs"]
    if boundPath != "":
        boundaryGdfDEM = dataUtils.readBoundaryInDemCrs(boundPath, demCrs)
    else:
        boundaryGdfDEM = None

    vecFiles = sorted(glob.glob(os.path.join(outDir, "*.geojson")))
    if not vecFiles:
        log.warning("Step 07: No vector files to rasterize in ./%s", relPath(outDir, cairosDir))
        return 0, 0

    nOk = nFail = 0
    for vPath in vecFiles:
        try:
            with timeIt(f"rasterize({os.path.basename(vPath)})"):
                gdf = gpd.read_file(vPath)

                # PRA raster
                if enablePRA and attributePRA in gdf.columns:
                    tifPath = os.path.splitext(vPath)[0] + f"-{attributePRA}.tif"
                    dataUtils.rasterizeGeojsonToTif(
                        gdf=gdf,
                        demPath=demPath,
                        outPath=tifPath,
                        mode=modePRA,
                        attribute=attributePRA,
                        classField="",
                        allTouched=allTouched,
                        compress=compress,
                        boundaryGdfDEM=boundaryGdfDEM,
                    )
                    log.debug("    saved raster: ./%s", relPath(tifPath, cairosDir))
                    nOk += 1

                # PRA ID raster
                if enableID and "praID" in gdf.columns:
                    gdf["praID_val"] = gdf["praID"].astype(int)
                    tifPraID = os.path.splitext(vPath)[0] + f"-{attributeID}.tif"
                    dataUtils.rasterizeGeojsonToTif(
                        gdf=gdf,
                        demPath=demPath,
                        outPath=tifPraID,
                        mode="attribute",
                        attribute="praID_val",
                        classField="",
                        allTouched=allTouched,
                        compress=compress,
                        boundaryGdfDEM=boundaryGdfDEM,
                    )
                    log.debug("    saved raster (praID): ./%s", relPath(tifPraID, cairosDir))
                    nOk += 1

        except Exception:
            nFail += 1
            log.exception("Step 07: Rasterization failed for ./%s", relPath(vPath, cairosDir))

    return nOk, nFail


# ------------------ Boundary derivation ------------------ #


def _derivePraBoundaries(outDir, cfg, cairosDir):
    """Derive boundary-only rasters from TIFFs in outDir."""
    sect = cfg["praPREPFORFLOWPY"]
    derive = sect.getboolean("deriveBoundaries", fallback=False)
    compress = sect.getboolean("compressOutputs", fallback=True)
    if not derive:
        return 0, 0

    try:
        from scipy.ndimage import convolve
    except Exception:
        log.error("Step 07: deriveBoundaries=True but SciPy unavailable; skipping.")
        return 0, 0

    inTifs = sorted(glob.glob(os.path.join(outDir, "*.tif")))
    if not inTifs:
        log.warning("Step 07: No input rasters for boundary derivation in ./%s", relPath(outDir, cairosDir))
        return 0, 0

    edgeKernel = np.array([[1, 1, 1], [1, 0, 1], [1, 1, 1]], dtype=np.uint8)

    nOk = nFail = 0
    for tif in inTifs:
        try:
            with timeIt(f"boundary({os.path.basename(tif)})"):
                arr, prof = dataUtils.readRaster(tif, return_profile=True)
                nodata = prof.get("nodata", -9999)
                mask = (arr != nodata) & (arr != 0)
                neighbors = convolve(mask.astype(np.uint8), edgeKernel, mode="constant", cval=0)
                boundaryMask = mask & (neighbors < 8) & (neighbors > 0)

                out = np.full(arr.shape, nodata, dtype=arr.dtype)
                out[boundaryMask] = arr[boundaryMask]

                outPath = os.path.splitext(tif)[0] + "_praBound.tif"
                dataUtils.saveRaster(
                    tif, outPath, out, dtype=arr.dtype, nodata=nodata, compress=("LZW" if compress else None)
                )
                log.debug("    saved boundary raster: ./%s", relPath(outPath, cairosDir))
                nOk += 1
        except Exception:
            nFail += 1
            log.exception("Step 07: Boundary derivation failed for ./%s", relPath(tif, cairosDir))

    return nOk, nFail


# ------------------ Main driver ------------------ #


def runPraPrepForFlowPy(cfg, workFlowDir=None, avaDir=None):
    """Step 07: prepare PRA polygons for FlowPy by band/size splitting and rasterization."""
    tAll = time.perf_counter()

    # --- Config parameters ---
    sizeFilter = cfg["praSEGMENTATION"].getfloat("sizeFilter", fallback=500.0)

    # --- Directories ---
    if workFlowDir is not None:
        inputDir = pathlib.Path(workFlowDir["inputDir"])
        cairosDir = workFlowDir["cairosDir"]
        praAssignElevSizeDir = pathlib.Path(workFlowDir["praAssignElevSizeDir"])
        praPrepForFlowPyDir = workFlowDir["praPrepForFlowPyDir"]
        cfg["praPREPFORFLOWPY"]["assignElevSize"] = "True"
    elif avaDir is not None:
        avaDir = pathlib.Path(avaDir)
        inputDir = avaDir / "Inputs"
        praAssignElevSizeDir = avaDir / "Work" / "PraAssignElevSize"
        if not praAssignElevSizeDir.exists():
            praAssignElevSizeDir = avaDir / "Work" / "praSegmentation"
            cfg["praPREPFORFLOWPY"]["assignElevSize"] = "False"
        else:
            cfg["praPREPFORFLOWPY"]["assignElevSize"] = "True"
        cairosDir = avaDir
        praPrepForFlowPyDir = avaDir / "Work" / "praPrepForFlowPy"
    else:
        message = "A workflowDir or an avaDir needs to be provided."
        log.error(message)
        raise ValueError(message)

    os.makedirs(praPrepForFlowPyDir, exist_ok=True)

    # --- DEM & BOUNDARY ---
    if cfg["MAIN"].getboolean("customPaths"):
        demName = cfg["MAIN"]["DEM"]
        demPath = inputDir / demName
    else:
        demPath = getInput.getDEMPath(avaDir)

    boundPath = inputDir / cfg["MAIN"].get("BOUNDARY", "").strip()
    if not boundPath.is_file():
        boundPath = ""
    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demCrs = demProfile["crs"]

    # --- Log parameters ---
    log.info("Step 07: Start PRA → FlowPy preparation...")
    log.info("Input: ./%s", relPath(praAssignElevSizeDir, cairosDir))
    log.info("Output: ./%s", relPath(praPrepForFlowPyDir, cairosDir))
    log.info("DEM: ./%s, Boundary: ./%s", relPath(demPath, cairosDir), relPath(boundPath, cairosDir))

    # Write directly into the canonical Step 07 directory. This was already
    # the behavior of the AvaFrame-directory workflow.
    outDir = praPrepForFlowPyDir

    # --- Find Step 06 inputs ---
    inDir, inFiles = _findStep07Inputs(
        praAssignElevSizeDir,
        cfg["praPREPFORFLOWPY"].getboolean("assignElevSize"),
        sizeFilter,
    )
    if not inFiles:
        log.error("Step 07: No Step 06 inputs found in ./%s", relPath(inDir, cairosDir))
        log.info("Step 07 aborted (no inputs): %.2fs", time.perf_counter() - tAll)
        return

    # --- Elevation bands ---
    if cfg["praPREPFORFLOWPY"].getboolean("assignElevSize"):
        elevBands = loadElevationBands(cfg)
        elevBandLabels = [lab for lab, _rng in elevBands]
    else:
        elevBandLabels = ["0000-8848"]
    # --- Step 2: filter/write per (band,size) vectors ---
    nOkVec, nFailVec, totalPolys, zeroFeatureFiles = dataUtils.filterAndWriteForFlowPy(
        inFiles,
        outDir,
        elevBandLabels,
        cairosDir,
        sizeClassesToKeep=(2, 3, 4, 5),
        cfg=cfg,
    )
    if zeroFeatureFiles:
        for fPath, combos in zeroFeatureFiles.items():
            log.warning(
                "Step 07: zero-feature combos in ./%s: %s", relPath(fPath, cairosDir), ", ".join(combos)
            )

    # --- Step 2b: rename outputs (remove '-ElevBands-Sized') ---
    for f in glob.glob(os.path.join(outDir, "*-ElevBands-Sized-*")):
        newName = re.sub(r"-ElevBands-Sized", "", f)
        if newName != f:
            try:
                os.rename(f, newName)
                log.debug("Renamed output: %s -> %s", os.path.basename(f), os.path.basename(newName))
            except Exception:
                log.exception("Failed to rename %s", f)

    # --- Step 3: rasterization ---
    nOkRas, nFailRas = _rasterizeAllVectors(outDir, demPath, boundPath, cfg, cairosDir)

    # --- Step 4: optional boundaries ---
    nOkBound, nFailBound = _derivePraBoundaries(outDir, cfg, cairosDir)

    # --- Summary ---
    log.info(
        "Step 07 complete: vec_ok=%d, vec_fail=%d, total_polys=%d, ras_ok=%d, ras_fail=%d, bound_ok=%d, bound_fail=%d",
        nOkVec,
        nFailVec,
        totalPolys,
        nOkRas,
        nFailRas,
        nOkBound,
        nFailBound,
    )
    log.info("Step 07 total time: %.2fs", time.perf_counter() - tAll)


if __name__ == "__main__":
    # get praDelineation config file
    cfg = cfgUtils.getModuleConfig(praDelineationVeitinger)
    # get main config file for avalanche dir
    modPath = pathlib.Path(ati.__file__).resolve().parent
    cfgNameFile = modPath / "atiCfg.ini"
    cfgMain = cfgUtils.getGeneralConfig(nameFile=cfgNameFile)
    runPraPrepForFlowPy(cfg, avaDir=cfgMain["MAIN"]["avalancheDirectory"])
