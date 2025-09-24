# praPrepForFlowPy.py

import os
import glob
import time
import logging
import contextlib

import numpy as np
import geopandas as gpd

from in1Utils import dataUtils
from in1Utils.dataUtils import relPath, timeIt
import in1Utils.cfgUtils as cfgUtils

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)

# ------------------ Minimal helpers ------------------ #


def _parseRangeCsv(value):
    v = (value or "").strip()
    parts = [p.strip() for p in v.split(",")]
    if len(parts) != 2:
        raise ValueError(f"Invalid range definition: '{value}' (expected 'low,high')")
    lo = float(parts[0])
    hi = float("inf") if parts[1].lower() == "inf" else float(parts[1])
    return lo, hi

def loadElevationBands(cfg):
    """
    Read elevation bands from INI section [praASSIGNELEV].
    Returns [(label, (lo, hi)), ...] where label is LLLL-HHHH (4 digits; inf->9999).
    """
    sect = cfg["praASSIGNELEV"]
    bands = []
    i = 1
    while True:
        key = f"elevationBand{i}"
        raw = sect.get(key, fallback=None)
        if not raw:
            break
        lo, hi = _parseRangeCsv(raw)
        lo_i = int(round(lo))
        hi_i = int(round(hi if hi != float("inf") else 9999))
        label = f"{lo_i:04d}-{hi_i:04d}"
        bands.append((label, (lo, hi)))
        i += 1
    if not bands:
        raise ValueError("No elevation bands defined in [praASSIGNELEV].")
    return bands

# ------------------ I/O discovery helpers ------------------ #

def _ensureOutputSubfolder(praPrepForFlowPyDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter):
    """Create ./08_praPrepForFlowPy/BnCh2_subC{thr}_{min}_{win}_sizeF{sizeF} and return its path."""
    subfolderName = f"BnCh2_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}"
    outDir = os.path.join(praPrepForFlowPyDir, subfolderName)
    os.makedirs(outDir, exist_ok=True)
    return subfolderName, outDir

def _findStep07Inputs(praAssignElevSizeDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter):
    """Return (inDir, sorted list of '*-ElevBands-Sized.geojson')."""
    subfolderName = f"BnCh2_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}"
    inDir = os.path.join(praAssignElevSizeDir, subfolderName)
    inFiles = sorted(glob.glob(os.path.join(inDir, "*-ElevBands-Sized.geojson")))
    return inDir, inFiles

# ------------------ Rasterization ------------------ #

def _rasterizeAllVectors(outDir, demPath, boundPath, cfg, cairosDir):
    """
    Rasterize GeoJSONs in outDir according to [praPREPFORFLOWPY].
    Creates:
      - <base>-[<attributePRA>].tif   if enableRasterizePRA=True
      - <base>-[<attributeID>].tif    if enableRasterizeID=True
    """
    sect = cfg["praPREPFORFLOWPY"]
    enablePRA = sect.getboolean("enableRasterizePRA", fallback=False)
    enableID  = sect.getboolean("enableRasterizeID", fallback=False)
    compress  = sect.getboolean("compressOutputs", fallback=True)

    if not (enablePRA or enableID):
        log.info("...Rasterize disabled by INI flags")
        return 0, 0

    # PRA rasterization settings
    modePRA      = sect.get("rasterizeModePRA", fallback="attribute").lower()
    attributePRA = sect.get("rasterizeAttributePRA", fallback="praAreaM")

    # ID rasterization settings
    attributeID = sect.get("rasterizeAttributeID", fallback="praID")

    allTouched = False  # fixed default

    # Cache boundary in DEM CRS
    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demCrs = demProfile["crs"]
    boundaryGdfDEM = dataUtils.readBoundaryInDemCrs(boundPath, demCrs)

    vecFiles = sorted(glob.glob(os.path.join(outDir, "*.geojson")))
    if not vecFiles:
        log.warning("No vector files (*.geojson) found to rasterize in ./%s", relPath(outDir, cairosDir))
        return 0, 0

    nOk = nFail = 0
    for vPath in vecFiles:
        try:
            with timeIt(f"rasterize({os.path.basename(vPath)})"):
                gdf = gpd.read_file(vPath)

                # --- PRA raster ---
                if enablePRA and attributePRA in gdf.columns:
                    tifPath = os.path.splitext(vPath)[0] + f"-{attributePRA}.tif"
                    dataUtils.rasterizeGeojsonToTif(
                        gdf=gdf,
                        demPath=demPath,
                        boundaryGdfDEM=boundaryGdfDEM,
                        outPath=tifPath,
                        mode=modePRA,
                        attribute=attributePRA,
                        classField="",
                        allTouched=allTouched,
                        compress=compress
                    )
                    log.debug("    saved raster: ./%s", relPath(tifPath, cairosDir))
                    nOk += 1

                # --- praID raster ---
                if enableID and "praID" in gdf.columns:
                    gdf["praID_val"] = gdf["praID"].astype(int)
                    tifPraID = os.path.splitext(vPath)[0] + f"-{attributeID}.tif"
                    dataUtils.rasterizeGeojsonToTif(
                        gdf=gdf,
                        demPath=demPath,
                        boundaryGdfDEM=boundaryGdfDEM,
                        outPath=tifPraID,
                        mode="attribute",
                        attribute="praID_val",
                        classField="",
                        allTouched=allTouched,
                        compress=compress
                    )
                    log.debug("    saved raster (praID): ./%s", relPath(tifPraID, cairosDir))
                    nOk += 1

        except Exception:
            nFail += 1
            log.exception("Rasterize failed for ./%s", relPath(vPath, cairosDir))

    return nOk, nFail

# ------------------ Boundary derivation ------------------ #

def _derivePraBoundaries(outDir, cfg, cairosDir):
    """
    Create boundary-only rasters from TIFFs in outDir.
    Operates on files produced by rasterization step (praAreaM or praID).
    """
    sect = cfg["praPREPFORFLOWPY"]
    derive = sect.getboolean("deriveBoundaries", fallback=False)
    compress = sect.getboolean("compressOutputs", fallback=True)
    if not derive:
        return 0, 0

    # Optional SciPy kernel; fall back if missing
    try:
        from scipy.ndimage import convolve
    except Exception:
        log.error("deriveBoundaries=True but SciPy not available; skipping.")
        return 0, 0

    inTifs = sorted(glob.glob(os.path.join(outDir, "*.tif")))
    if not inTifs:
        log.warning("No input rasters found for boundary derivation in ./%s", relPath(outDir, cairosDir))
        return 0, 0

    edgeKernel = np.array([[1, 1, 1],
                           [1, 0, 1],
                           [1, 1, 1]], dtype=np.uint8)

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
                dataUtils.saveRaster(tif, outPath, out, dtype=arr.dtype,
                                     nodata=nodata,
                                     compress=("LZW" if compress else None))
                log.debug("    saved boundary raster: ./%s", relPath(outPath, cairosDir))
                nOk += 1
        except Exception:
            nFail += 1
            log.exception("Boundary derivation failed for ./%s", relPath(tif, cairosDir))

    return nOk, nFail



# ------------------ Main driver ------------------ #

def runPraPrepForFlowPy(cfg, workFlowDir):
    """
    Step 07: prepare PRA data for FlowPy.
      1) Read Step-07 inputs (*-ElevBands-Sized.geojson) grouped in parameterized subfolder
      2) Filter by (elev_band, size_class) and write per-combination GeoJSONs
         → adds deterministic 7-digit 'praID' to each feature (first attribute)
      3) Rasterize those vectors according to [praPREPFORFLOWPY] flags
         → <base>- [praAreaM].tif, <base>- [praID].tif
      4) Optionally derive boundary-only rasters from the written TIFFs

    Inputs:
      ./07_praAssignElevSize/BnCh2_subC{thr}_{min}_{win}_sizeF{sizeF}/ *-ElevBands-Sized.geojson
      [MAIN] DEM, BOUNDARY

    Outputs:
      ./08_praPrepForFlowPy/BnCh2_subC{thr}_{min}_{win}_sizeF{sizeF}/
        <per-(band,size) GeoJSONs with praID + rasters per INI>
    """
    tAll = time.perf_counter()

    # --- Config (selection & subcatch params for suffix discovery) ---
    streamThreshold     = cfg["praSUBCATCHMENTS"].getint("streamThreshold", fallback=500)
    minLength           = cfg["praSUBCATCHMENTS"].getint("minLength", fallback=100)
    smoothingWindowSize = cfg["praSUBCATCHMENTS"].getint("smoothingWindowSize", fallback=5)
    sizeFilter          = cfg["praSEGMENTATION"].getfloat("sizeFilter", fallback=500.0)

    # --- Selection threshold code (context only) ---
    thrF  = cfg['praSELECTION'].getfloat('selectedThreshold', fallback=0.30)
    code3 = f"{int(round(thrF * 100)):03d}"

    # --- Directories ---
    cairosDir            = workFlowDir["cairosDir"]
    praAssignElevSizeDir = workFlowDir.get("praAssignElevSizeDir") or os.path.join(cairosDir, "07_praAssignElevSize")
    praPrepForFlowPyDir  = workFlowDir.get("praPrepForFlowPyDir")  or os.path.join(cairosDir, "08_praPrepForFlowPy")
    os.makedirs(praAssignElevSizeDir, exist_ok=True)
    os.makedirs(praPrepForFlowPyDir,  exist_ok=True)

    # --- DEM & BOUNDARY (references) ---
    inputDir  = workFlowDir["inputDir"]
    demName   = cfg["MAIN"].get("DEM", "").strip()
    demPath   = os.path.join(inputDir, demName)
    boundName = cfg["MAIN"].get("BOUNDARY", "").strip()
    boundPath = os.path.join(inputDir, boundName)

    # Sanity read (DEM) to ensure present; also used later
    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demCrs = demProfile["crs"]

    # --- Rasterization settings from INI ---
    sect = cfg["praPREPFORFLOWPY"]
    compress     = sect.getboolean("compressOutputs", fallback=True)
    enablePRA    = sect.getboolean("enableRasterizePRA", fallback=False)
    modePRA      = sect.get("rasterizeModePRA", fallback="attribute").lower()
    attributePRA = sect.get("rasterizeAttributePRA", fallback="praAreaM")
    enableID     = sect.getboolean("enableRasterizeID", fallback=False)
    attributeID  = sect.get("rasterizeAttributeID", fallback="praID")
    deriveBound  = sect.getboolean("deriveBoundaries", fallback=False)

    # --- Parameters line (INFO) ---
    log.info(
        "...PRA → FlowPy using: in=./%s, out=./%s, DEM=./%s, BOUNDARY=./%s, "
        "streamThr=%s, minLen=%s, smoothWin=%s, sizeF=%s, thr=%s",
        relPath(praAssignElevSizeDir, cairosDir),
        relPath(praPrepForFlowPyDir,  cairosDir),
        relPath(demPath, cairosDir),
        relPath(boundPath, cairosDir),
        streamThreshold, minLength, smoothingWindowSize, int(sizeFilter), code3
    )
    log.info("...Rasterize flags: compress=%s, enablePRA=%s(attr=%s, mode=%s), enableID=%s(attr=%s), deriveBoundaries=%s",
             compress, enablePRA, attributePRA, modePRA, enableID, attributeID, deriveBound)

    # --- Ensure output subfolder ---
    _, outDir = _ensureOutputSubfolder(
        praPrepForFlowPyDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter
    )

    # --- Discover Step-07 inputs ---
    inDir, inFiles = _findStep07Inputs(
        praAssignElevSizeDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter
    )
    if not inFiles:
        log.error("No Step-07 inputs found matching *-ElevBands-Sized.geojson in ./%s",
                  relPath(inDir, cairosDir))
        log.info("...PRA → FlowPy - failed (no inputs): %.2fs", time.perf_counter() - tAll)
        return

    # --- Elevation bands (labels) from config ---
    elevBands = loadElevationBands(cfg)
    elevBandLabels = [lab for lab, _rng in elevBands]

    # --- Step 2: filter/write per (band,size) vectors ---
    nOkVec, nFailVec, totalPolys, zeroFeatureFiles = dataUtils.filterAndWriteForFlowPy(
        inFiles, outDir, elevBandLabels, cairosDir,
        sizeClassesToKeep=(2, 3, 4, 5),
        cfg=cfg
    )

    if zeroFeatureFiles:
        for fPath, combos in zeroFeatureFiles.items():
            log.warning("...zero-feature combos in ./%s: %s",
                        dataUtils.relPath(fPath, cairosDir), ", ".join(combos))


    # --- Step 3: rasterize per INI modes ---
    nOkRas, nFailRas = _rasterizeAllVectors(
        outDir=outDir, demPath=demPath, boundPath=boundPath, cfg=cfg, cairosDir=cairosDir
    )

    # --- Step 4: optional boundary-only derivation from TIFFs ---
    nOkBound, nFailBound = _derivePraBoundaries(outDir=outDir, cfg=cfg, cairosDir=cairosDir)

    # --- Done ---
    log.info(
        "...PRA → FlowPy stats: vec_ok=%d, vec_fail=%d, total_polys=%d, "
        "ras_ok=%d, ras_fail=%d, bound_ok=%d, bound_fail=%d",
        nOkVec, nFailVec, totalPolys, nOkRas, nFailRas, nOkBound, nFailBound
    )
    log.info("...PRA → FlowPy - done: %.2fs", time.perf_counter() - tAll)
