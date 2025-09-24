# praProcessing.py


import os
import glob
import logging
import time

import numpy as np
from scipy.ndimage import convolve

import rasterio
from rasterio.features import shapes
import geopandas as gpd
from shapely.geometry import shape

import in1Utils.dataUtils as dataUtils

log = logging.getLogger(__name__)

# ------------------ Cleaning ------------------ #

def runPraCleaning(cfg, workFlowDir, demPath, selectedFiles):
    """
    Two-pass neighborhood cleaning for PRA rasters.

    Pass 1 (direct neighbors N/E/S/W): keep only cells with >= minDirectNeighborsPass1.
    Pass 2 (diagonal neighbors NW/NE/SW/SE): keep only cells with >= minDiagonalNeighborsPass2.

    Writes outputs into workFlowDir['praProcessingDir'] as:
      <base>_BnCh1.tif  and  <base>_BnCh2.tif

    Returns a list of final outputs; on failure for a file, falls back to the original path.
    """
    rel = lambda p: os.path.relpath(p, start=workFlowDir['cairosDir'])
    outDir = workFlowDir['praProcessingDir']
    os.makedirs(outDir, exist_ok=True)

    # INI parameters (with safe fallbacks)
    p1_min = cfg['praPROCESSING'].getint('minDirectNeighborsPass1', fallback=3)
    p2_min = cfg['praPROCESSING'].getint('minDiagonalNeighborsPass2', fallback=2)

    log.debug("PRA cleaning start: pass1(minDirect=%d), pass2(minDiag=%d), nFiles=%d",
              p1_min, p2_min, len(selectedFiles))

    edgeKernel1 = np.array([[0, 1, 0],
                            [1, 0, 1],
                            [0, 1, 0]], dtype=np.uint8)
    edgeKernel2 = np.array([[1, 0, 1],
                            [0, 0, 0],
                            [1, 0, 1]], dtype=np.uint8)

    cleaned_final = []
    n_clean, n_fallback = 0, 0

    for inPath in selectedFiles:
        try:
            log.debug("  cleaning: ./%s", rel(inPath))
            arr, _prof = dataUtils.readRaster(inPath, return_profile=True)

            # binary mask (anything > 0 becomes 1)
            mask = (arr > 0).astype(np.uint8)

            # Pass 1: direct neighbors
            n1 = convolve(mask, edgeKernel1, mode='constant', cval=0)
            mask1 = mask.copy()
            mask1[n1 < p1_min] = 0

            base = os.path.splitext(os.path.basename(inPath))[0]
            out1 = os.path.join(outDir, f"{base}_BnCh1.tif")
            dataUtils.saveRaster(demPath, out1, mask1, dtype='uint8', nodata=0, compress='DEFLATE')
            log.debug("  saved: ./%s", rel(out1))

            # Pass 2: diagonal neighbors
            n2 = convolve(mask1, edgeKernel2, mode='constant', cval=0)
            mask2 = mask1.copy()
            mask2[n2 < p2_min] = 0

            out2 = os.path.join(outDir, f"{base}_BnCh2.tif")
            dataUtils.saveRaster(demPath, out2, mask2, dtype='uint8', nodata=0, compress='DEFLATE')
            log.debug("  saved: ./%s", rel(out2))

            cleaned_final.append(out2)
            n_clean += 1

        except Exception:
            log.exception("Cleaning failed for ./%s", rel(inPath))
            cleaned_final.append(inPath)  # fallback to original
            n_fallback += 1

    log.debug("PRA cleaning done (final _BnCh2=%d, fallback=%d)", n_clean, n_fallback)
    return cleaned_final


# ------------------ Polygonization Helpers ------------------ #

def rasterToPolygons(path, includeZero: bool = False):
    """
    Convert a (single-band) raster to polygons.
    - Masks out nodata from the input raster.
    - If includeZero=False, only values > 0 are polygonized (typical for masks).
    Returns: GeoDataFrame with columns: ['value', 'geometry'] and CRS set.
    """
    with rasterio.open(path) as src:
        arr = src.read(1)
        xform = src.transform
        crs = src.crs
        nodata = src.nodata
        mask = arr != nodata if nodata is not None else None

        if includeZero:
            gen = (
                {"properties": {"value": v}, "geometry": geom}
                for geom, v in shapes(arr, mask=mask, transform=xform)
            )
        else:
            gen = (
                {"properties": {"value": v}, "geometry": geom}
                for geom, v in shapes(arr, mask=mask, transform=xform)
                if v > 0
            )

        geoms = []
        vals = []
        for item in gen:
            geoms.append(shape(item["geometry"]))
            vals.append(item["properties"]["value"])

    gdf = gpd.GeoDataFrame({"value": vals, "geometry": geoms}, crs=crs)
    return gdf


def calcPolygonProperties(gdf):
    """
    Add simple attributes to polygons:
      - area_m (square meters)
      - area_km (square kilometers)
      - thickness (currently None placeholder)
      - ID (1..N)
    Returns the same GeoDataFrame with extra columns.
    """
    # vectorized area (assumes projected CRS in meters)
    gdf["area_m"] = gdf.geometry.area
    gdf["area_km"] = gdf["area_m"] / 1e6
    gdf["thickness"] = None
    gdf["ID"] = range(1, len(gdf) + 1)
    return gdf




# ------------------ Main Driver ------------------ #

def runPraProcessing(cfg, workFlowDir):
    """
    Step 05: PRA processing.
    Loads DEM as reference, selects PRA rasters from praSelectionDir
    according to threshold in [praSELECTION], logs parameters, cleans masks.
    """
    tAll = time.perf_counter()

    # --- Directories ---
    cairosDir       = workFlowDir['cairosDir']
    praSelectionDir = workFlowDir['praSelectionDir']
    inputDir        = workFlowDir['inputDir']

    # --- DEM path ---
    demName = cfg['MAIN']['DEM']
    demPath = os.path.join(inputDir, demName)

    # --- Load DEM as reference (profile + nodata) ---
    dem, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demNoData = demProfile.get('nodata', 0)
    cellSize  = demProfile['transform'][0]
    crsName   = demProfile['crs']

    # --- Threshold code ---
    thrF  = cfg['praSELECTION'].getfloat('selectedThreshold', fallback=0.30)
    code3 = f"{int(thrF * 100):03d}"  # 0.30 -> "030"

    # --- Select PRA rasters ---
    pattern = f"pra{code3}sec*.tif"
    selectedFiles = sorted(glob.glob(os.path.join(praSelectionDir, pattern)))

    # --- Prepare relative paths for logging ---
    rel = lambda p: os.path.relpath(p, start=cairosDir)
    relDemPath = rel(demPath)

    # --- Log parameters ---
    log.info("...PRA processing using: DEM=./%s, cellSize=%.2f, CRS=%s, NoData=%s",
             relDemPath, cellSize, crsName, demNoData)
    log.info("...PRA processing using: threshold=%.2f (%s), nFiles=%d",
             thrF, code3, len(selectedFiles))

    # --- Log relative file paths for processing (INFO) ---
    for f in selectedFiles:
        log.debug("...PRA processing files found: ./%s", rel(f))

    # --- Early exit if no files found ---
    if not selectedFiles:
        log.error("No PRA rasters found for threshold %s in ./%s", code3, rel(praSelectionDir))
        log.info("...PRA processing - failed (no matching rasters): %.2fs", time.perf_counter() - tAll)
        return

    # --- Read PRA rasters & check metadata ---
    for f in selectedFiles:
        arr, prof = dataUtils.readRaster(f, return_profile=True)
        log.info("...PRA cleaning input: ./%s", rel(f))

        # Consistency checks
        if prof['crs'] != demProfile['crs']:
            log.error("CRS mismatch for ./%s (DEM=%s, PRA=%s)", rel(f), demProfile['crs'], prof['crs'])
        if prof['transform'] != demProfile['transform']:
            log.error("Transform mismatch for ./%s", rel(f))
        if (prof['width'], prof['height']) != (demProfile['width'], demProfile['height']):
            log.error("Dimension mismatch for ./%s (DEM=%s, PRA=%s)",
                      rel(f), (demProfile['width'], demProfile['height']),
                      (prof['width'], prof['height']))
        if prof.get('nodata') != demProfile.get('nodata'):
            log.error("NoData mismatch for ./%s (DEM=%s, PRA=%s)",
                      rel(f), demProfile.get('nodata'), prof.get('nodata'))
        if prof['dtype'] != 'int16':
            log.warning("Unexpected dtype for ./%s (expected int16, got %s)", rel(f), prof['dtype'])

    # --- Cleaning (Pass1 + Pass2) ---
    tClean = time.perf_counter()
    cleanedFiles = runPraCleaning(cfg, workFlowDir, demPath, selectedFiles)
    log.info("...PRA cleaning finished - done: %.2fs", time.perf_counter() - tClean)


    # --- Polygonization ---
        # --- Step 4: Polygonize cleaned rasters to Shapefiles ---
    tPoly = time.perf_counter()
    outDir = workFlowDir['praProcessingDir']
    os.makedirs(outDir, exist_ok=True)

    rel = lambda p: os.path.relpath(p, start=cairosDir)
    n_ok, n_fail = 0, 0

    for inPath in cleanedFiles:
        try:
            base = os.path.splitext(os.path.basename(inPath))[0]
            shpPath = os.path.join(outDir, f"{base}.shp")

            gdf = rasterToPolygons(inPath, includeZero=False)  # ignore zeros
            gdf = calcPolygonProperties(gdf)

            gdf.to_file(shpPath)
            log.info("...PRA polygonized → ./%s", rel(shpPath))
            n_ok += 1
        except Exception:
            log.exception("Polygonization failed for ./%s", rel(inPath))
            n_fail += 1

    log.info("...PRA polygonization - done: %.2fs", time.perf_counter() - tPoly)


    


