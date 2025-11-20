# ------------------ Step 04: PRA Processing ---------------------------- #
#
# Purpose :
#     Perform binary cleaning and connectivity-based filtering on PRA masks
#     generated in Step 02, followed by polygonization of the resulting
#     contiguous PRA regions. This step transforms preliminary PRA rasters
#     into clean, topologically valid PRA polygons.
#
# Inputs :
#     - PRA rasters from Step 02 (praSelectionDir)
#     - DEM (from [MAIN]) for optional elevation-based masking
#
# Outputs :
#     - Cleaned PRA rasters:
#         * <name>_BnCh1.tif   (pass 1: direct-neighbor connectivity)
#         * <name>_BnCh2.tif   (pass 2: diagonal-neighbor refinement)
#     - PRA GeoJSON polygons representing the cleaned regions
#
# Config :
#     [praPROCESSING]
#         • Connectivity thresholds (pass 1 / pass 2)
#         • Mask behavior and nodata handling
#         • Output compression settings
#
# Consumes :
#     - PRA masks produced in Step 02
#
# Provides :
#     - Cleaned PRA polygon datasets required for:
#         • Step 05 (PRA Segmentation)
#         • subsequent elevation/size assignment (Step 06)
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
import glob
import time
import logging
import numpy as np
from typing import cast
from scipy.ndimage import convolve
import rasterio
from rasterio.features import shapes
import geopandas as gpd
from shapely.geometry import shape

import in1Utils.dataUtils as dataUtils
from in1Utils.dataUtils import timeIt, relPath

log = logging.getLogger(__name__)

# ------------------ Cleaning ------------------ #

def runPraCleaning(cfg, workFlowDir, demPath, selectedFiles):
    """
    Two-pass neighborhood cleaning for PRA rasters.

    Pass 1: keep cells with >= minDirectNeighborsPass1 (N/E/S/W)
    Pass 2: keep cells with >= minDiagonalNeighborsPass2 (NW/NE/SW/SE)
    """
    outDir = workFlowDir["praProcessingDir"]
    cairosDir = workFlowDir["cairosDir"]
    os.makedirs(outDir, exist_ok=True)

    p1_min = cfg["praPROCESSING"].getint("minDirectNeighborsPass1", fallback=3)
    p2_min = cfg["praPROCESSING"].getint("minDiagonalNeighborsPass2", fallback=2)

    log.info(
        "Step 04: PRA cleaning start (pass1=%d, pass2=%d, nFiles=%d)",
        p1_min, p2_min, len(selectedFiles)
    )

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
            relIn = relPath(inPath, cairosDir)
            log.info("Cleaning PRA: ./%s", relIn)
            arr, _prof = dataUtils.readRaster(inPath, return_profile=True)

            mask = (arr > 0).astype(np.uint8)

            # Pass 1: direct neighbors
            with timeIt("Pass1 - direct neighbors"):
                n1 = convolve(mask, edgeKernel1, mode="constant", cval=0)
                mask1 = mask.copy()
                mask1[n1 < p1_min] = 0

            base = os.path.splitext(os.path.basename(inPath))[0]
            out1 = os.path.join(outDir, f"{base}_BnCh1.tif")
            dataUtils.saveRaster(demPath, out1, mask1, dtype="uint8", nodata=0, compress="DEFLATE")
            log.debug("Saved: ./%s", relPath(out1, cairosDir))

            # Pass 2: diagonal neighbors
            with timeIt("Pass2 - diagonal neighbors"):
                n2 = convolve(mask1, edgeKernel2, mode="constant", cval=0)
                mask2 = mask1.copy()
                mask2[n2 < p2_min] = 0

            out2 = os.path.join(outDir, f"{base}_BnCh2.tif")
            dataUtils.saveRaster(demPath, out2, mask2, dtype="uint8", nodata=0, compress="DEFLATE")
            log.debug("Saved: ./%s", relPath(out2, cairosDir))

            cleaned_final.append(out2)
            n_clean += 1

        except Exception:
            log.exception("Step 04: Cleaning failed for ./%s", relPath(inPath, cairosDir))
            cleaned_final.append(inPath)
            n_fallback += 1

    log.info("Step 04: PRA cleaning done (success=%d, fallback=%d)", n_clean, n_fallback)
    return cleaned_final


# ------------------ Polygonization Helpers ------------------ #

def rasterToPolygons(path, includeZero: bool = False) -> gpd.GeoDataFrame:
    """
    Convert a raster to polygons (masking nodata).
    Returns a GeoDataFrame with ['value', 'geometry'].
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

    # Pylance doesn’t understand GeoPandas constructors -> ignore type
    gdf = gpd.GeoDataFrame({"value": vals, "geometry": geoms}, crs=crs)  # type: ignore[arg-type]
    return gdf


def calcPolygonProperties(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add area and ID fields to polygons."""
    gdf["area_m"] = gdf.geometry.area
    gdf["area_km"] = gdf["area_m"] / 1e6  # type: ignore[operator]
    gdf["thickness"] = None
    gdf["ID"] = range(1, len(gdf) + 1)
    return gdf


# ------------------ Main Driver ------------------ #

def runPraProcessing(cfg, workFlowDir):
    """Step 04: Clean PRA rasters and polygonize results."""
    tAll = time.perf_counter()

    cairosDir       = workFlowDir["cairosDir"]
    praSelectionDir = workFlowDir["praSelectionDir"]
    inputDir        = workFlowDir["inputDir"]

    demName = cfg["MAIN"]["DEM"]
    demPath = os.path.join(inputDir, demName)

    # --- DEM reference ---
    dem, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demNoData = demProfile.get("nodata", 0)
    cellSize  = demProfile["transform"][0]
    crsName   = demProfile["crs"]

    # --- Threshold ---
    thrF  = cfg["praSELECTION"].getfloat("selectedThreshold", fallback=0.30)
    code3 = f"{int(thrF * 100):03d}"

    # --- Select PRA rasters ---
    pattern = f"pra{code3}sec*.tif"
    selectedFiles = sorted(glob.glob(os.path.join(praSelectionDir, pattern)))

    relDemPath = relPath(demPath, cairosDir)
    log.info(
        "Step 04: Using DEM=./%s, cellSize=%.2f, CRS=%s, NoData=%s",
        relDemPath, cellSize, crsName, demNoData
    )
    log.info("Step 04: Threshold=%.2f (%s), nFiles=%d", thrF, code3, len(selectedFiles))

    if not selectedFiles:
        log.error("No PRA rasters found for threshold %s in ./%s",
                  code3, relPath(praSelectionDir, cairosDir))
        log.error("Step 04 failed (no matching rasters)")
        return

    # --- Metadata consistency check ---
    for f in selectedFiles:
        arr, prof = dataUtils.readRaster(f, return_profile=True)
        relF = relPath(f, cairosDir)
        if prof["crs"] != demProfile["crs"]:
            log.warning("CRS mismatch for ./%s", relF)
        if prof["transform"] != demProfile["transform"]:
            log.warning("Transform mismatch for ./%s", relF)
        if (prof["width"], prof["height"]) != (demProfile["width"], demProfile["height"]):
            log.warning("Dimension mismatch for ./%s", relF)
        if prof.get("nodata") != demProfile.get("nodata"):
            log.warning("NoData mismatch for ./%s", relF)
        if prof["dtype"] != "int16":
            log.warning("Unexpected dtype for ./%s (expected int16, got %s)", relF, prof["dtype"])

    # --- Cleaning ---
    with timeIt("Step 04: PRA Cleaning"):
        cleanedFiles = runPraCleaning(cfg, workFlowDir, demPath, selectedFiles)

    # --- Polygonization to GeoJSON ---
    with timeIt("Step 04: Polygonization"):
        outDir = workFlowDir["praProcessingDir"]
        os.makedirs(outDir, exist_ok=True)
        n_ok, n_fail = 0, 0

        for inPath in cleanedFiles:
            try:
                base = os.path.splitext(os.path.basename(inPath))[0]
                geojsonPath = os.path.join(outDir, f"{base}.geojson")

                gdf = rasterToPolygons(inPath, includeZero=False)
                gdf = calcPolygonProperties(cast(gpd.GeoDataFrame, gdf))
                gdf.to_file(geojsonPath, driver="GeoJSON")  # type: ignore[attr-defined]
                log.info("Polygonized → ./%s", relPath(geojsonPath, cairosDir))
                n_ok += 1
            except Exception:
                log.exception("Polygonization failed for ./%s", relPath(inPath, cairosDir))
                n_fail += 1

        log.info("Polygonization complete (ok=%d, fail=%d)", n_ok, n_fail)

    log.info("Step 04: PRA Processing complete in %.2fs", time.perf_counter() - tAll)
