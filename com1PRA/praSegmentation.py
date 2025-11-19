# ------------------ Step 05: PRA Segmentation -------------------------- #
#
# Purpose :
#     Segment cleaned PRA polygons (from Step 04) by intersecting them with
#     smoothed subcatchment units (from Step 03). This ensures hydrologically
#     meaningful PRA partitions and prepares size-filtered PRA subsets for
#     elevation and size assignment.
#
# Inputs :
#     - Cleaned PRA polygons (GeoJSONs) from Step 04
#     - Subcatchment polygons (SHP/GeoJSON) from Step 03
#
# Outputs :
#     - Segmented PRA GeoJSONs (PRA × subcatchment intersections)
#     - Size-filtered PRA GeoJSONs according to segmentation thresholds
#
# Config :
#     [praSEGMENTATION]
#         • sizeClass definitions
#         • minimum area thresholds
#         • optional filters for small objects
#
# Consumes :
#     - Cleaned PRA polygons produced in Step 04
#     - Subcatchments generated in Step 03
#
# Provides :
#     - Segmented, size-filtered PRA polygons required for:
#         • Step 06 (Assign Elevation & Size)
#         • Step 07 (PRA → FlowPy preparation)
#
# Author :
#     Christoph Hesselbach
#
# Institution :
#     Austrian Research Centre for Forests (BFW)
#     Department of Natural Hazards | Snow and Avalanche Unit
#
# Version :
#     2025-11
#
# ----------------------------------------------------------------------- #


import os
import glob
import time
import logging
import contextlib
import geopandas as gpd
import numpy as np
import pandas as pd

import in1Utils.dataUtils as dataUtils
from in1Utils.dataUtils import timeIt, relPath

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)

# ------------------ Helper functions ------------------ #

def findPraFiles(praProcessingDir: str, code3: str):
    """Find polygonized PRA GeoJSONs from Step 04 (code like '030')."""
    pattern = f"pra{code3}sec*_BnCh2.geojson"
    return sorted(glob.glob(os.path.join(praProcessingDir, pattern)))


def buildSubcatchSmoothedPath(praSubcatchmentsDir: str,
                              streamThreshold: int,
                              minLength: int,
                              smoothingWindowSize: int,
                              weightedSlopeFlow: bool):
    """Return expected smoothed subcatchments path (SHP from Step 03)."""
    weight_tag = "weighted" if weightedSlopeFlow else "unweighted"
    fname = f"subcatchments_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}_{weight_tag}.shp"
    return os.path.join(praSubcatchmentsDir, fname)


def ensureGeojsonVersion(src_path: str) -> str:
    """
    Convert SHP → GeoJSON if not already GeoJSON.
    Returns path to the GeoJSON file.
    """
    if src_path.lower().endswith(".geojson"):
        return src_path
    geojson_path = os.path.splitext(src_path)[0] + ".geojson"
    try:
        gdf = gpd.read_file(src_path)
        gdf.to_file(geojson_path, driver="GeoJSON")
        log.info("Converted subcatchments shapefile to GeoJSON: ./%s", relPath(geojson_path, os.getcwd()))
        return geojson_path
    except Exception:
        log.exception("Failed to convert shapefile → GeoJSON: %s", src_path)
        raise


def _parseRangeCsv(value: str):
    v = (value or "").strip()
    parts = [p.strip() for p in v.split(",")]
    if len(parts) != 2:
        raise ValueError(f"Invalid size class: '{value}' (expected 'low,high')")
    lo = float(parts[0])
    hi = float("inf") if parts[1].lower() == "inf" else float(parts[1])
    return lo, hi


def loadSizeClasses(cfg):
    sect = cfg["praSEGMENTATION"]
    sizeClasses = {}
    for i in range(1, 6):
        key = f"sizeClass{i}"
        lo, hi = _parseRangeCsv(sect.get(key, fallback="0,inf"))
        sizeClasses[i] = (lo, hi)
    return sizeClasses


def classifyAreasSqm(areasSqm, sizeClasses):
    counts = {k: 0 for k in sizeClasses}
    for a in areasSqm:
        for cid, (lo, hi) in sizeClasses.items():
            if lo <= a < hi:
                counts[cid] += 1
                break
    return counts


def attachAreasMetersNoGeomChange(gdf: gpd.GeoDataFrame, demCrs) -> gpd.GeoDataFrame:
    """Compute areas (m², km²) without changing CRS."""
    try:
        if len(gdf) == 0:
            return gdf.assign(area_m=[], area_km=[])
        isProjected = getattr(demCrs, "is_projected", None)
        if isProjected:
            area_series = gdf.to_crs(demCrs).geometry.area
        else:
            try:
                utm = gdf.estimate_utm_crs()
                area_series = gdf.to_crs(utm).geometry.area
            except Exception:
                area_series = gdf.geometry.area
        return gdf.assign(area_m=area_series.values, area_km=area_series.values / 1e6)  # type: ignore
    except Exception:
        log.exception("Area computation failed; writing zeros.")
        z = np.zeros(len(gdf))
        return gdf.assign(area_m=z, area_km=z / 1e6)


def applySizeFilter(inputGeoPath, sizeFilter, outBasePath, cairosDir, sizeClasses):
    """Keep only features ≥ sizeFilter (m²). Output GeoJSON."""
    gdf = gpd.read_file(inputGeoPath)
    if "area_m" not in gdf.columns:
        gdf["area_m"] = gdf.geometry.area
    gdfFiltered = gdf[gdf["area_m"] >= float(sizeFilter)]

    outGeo = f"{outBasePath}.geojson"
    gdfFiltered.to_file(outGeo, driver="GeoJSON")
    filteredClasses = classifyAreasSqm(gdfFiltered["area_m"].astype(float).tolist(), sizeClasses)

    log.info("...size filter %.0f m² → kept=%d, out=./%s",
             sizeFilter, len(gdfFiltered), relPath(outGeo, cairosDir))
    return len(gdfFiltered), outGeo, filteredClasses


# ------------------ Core per-file operation ------------------ #

def processSinglePraLayer(inPath: str, subcatchGdf: gpd.GeoDataFrame, outDir: str,
                          streamThreshold: int, minLength: int, smoothingWindowSize: int,
                          cairosDir: str, sizeClasses, demCrs):
    """Overlay PRA × subcatchments; compute areas and save GeoJSON."""
    try:
        with timeIt(f"processSinglePraLayer({os.path.basename(inPath)})"):
            praGdf = gpd.read_file(inPath)

            subcUse = subcatchGdf.to_crs(praGdf.crs) if subcatchGdf.crs != praGdf.crs else subcatchGdf
            clipped = gpd.overlay(praGdf, subcUse, how="intersection", keep_geom_type=True)

            if clipped.empty:
                log.debug("No intersection for ./%s", relPath(inPath, cairosDir))
                return None, 0, 0.0, {k: 0 for k in sizeClasses}

            clipped = clipped.explode(index_parts=True).reset_index(drop=True)
            clipped = clipped[["geometry"]]
            clipped = attachAreasMetersNoGeomChange(clipped, demCrs)
            classCounts = classifyAreasSqm(clipped["area_m"].astype(float).tolist(), sizeClasses)

            base = os.path.splitext(os.path.basename(inPath))[0]
            outPath = os.path.join(outDir, f"{base}_subC{streamThreshold}_{minLength}_{smoothingWindowSize}.geojson")
            clipped.to_file(outPath, driver="GeoJSON")

            log.info("Segmented PRA → ./%s (%d polys)", relPath(outPath, cairosDir), len(clipped))
            return outPath, len(clipped), float(clipped["area_m"].sum()), classCounts
    except Exception:
        log.exception("Segmentation failed for ./%s", relPath(inPath, cairosDir))
        return None, 0, 0.0, {k: 0 for k in sizeClasses}


# ------------------ Main driver ------------------ #

def runPraSegmentation(cfg, workFlowDir):
    """Step 05: PRA segmentation (PRA GeoJSON × subcatchment SHP)."""
    tAll = time.perf_counter()

    cairosDir = workFlowDir["cairosDir"]
    praProcessingDir = workFlowDir["praProcessingDir"]
    praSubcatchmentsDir = workFlowDir["praSubcatchmentsDir"]
    praSegmentationDir = workFlowDir.get("praSegmentationDir") or os.path.join(cairosDir, "06_praSegmentation")
    os.makedirs(praSegmentationDir, exist_ok=True)

    thrF = cfg["praSELECTION"].getfloat("selectedThreshold", fallback=0.30)
    code3 = f"{int(round(thrF * 100)):03d}"

    subCfg = cfg["praSUBCATCHMENTS"]
    streamThreshold = subCfg.getint("streamThreshold", fallback=500)
    minLength = subCfg.getint("minLength", fallback=100)
    smoothingWindowSize = subCfg.getint("smoothingWindowSize", fallback=5)
    weightedSlopeFlow = subCfg.getboolean("weightedSlopeFlow", fallback=False)

    inputDir = workFlowDir["inputDir"]
    demName = cfg["MAIN"].get("DEM", "").strip()
    demPath = os.path.join(inputDir, demName)
    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demCrs = demProfile["crs"]

    sizeClasses = loadSizeClasses(cfg)
    sizeFilter = cfg["praSEGMENTATION"].getfloat("sizeFilter", fallback=500.0)

    praFiles = findPraFiles(praProcessingDir, code3)
    subcatchPath = buildSubcatchSmoothedPath(praSubcatchmentsDir, streamThreshold, minLength, smoothingWindowSize, weightedSlopeFlow)

    log.info(
        "Step 05: PRA segmentation → out=./%s, SubC=./%s",
        relPath(praSegmentationDir, cairosDir),
        relPath(subcatchPath, cairosDir),
    )

    if not praFiles:
        log.error("No PRA GeoJSONs found in ./%s", relPath(praProcessingDir, cairosDir))
        return
    if not os.path.exists(subcatchPath):
        log.error("Subcatchments file missing: ./%s", relPath(subcatchPath, cairosDir))
        return

    # --- Convert SHP → GeoJSON if necessary ---
    subcatchGeo = ensureGeojsonVersion(subcatchPath)

    # --- Load subcatchments ---
    try:
        subcatchGdf = gpd.read_file(subcatchGeo)
    except Exception:
        log.exception("Failed to read subcatchments: ./%s", relPath(subcatchGeo, cairosDir))
        return

    # --- Process all PRA files ---
    nOk, totalPolys, totalAreaSqm = 0, 0, 0
    totalClassCounts = {k: 0 for k in sizeClasses}

    totalPolysFiltered, totalAreaSqmFiltered = 0, 0
    totalClassCountsFiltered = {k: 0 for k in sizeClasses}

    for inPath in praFiles:
        outPath, nPolys, sumAreaSqm, classCounts = processSinglePraLayer(
            inPath, subcatchGdf, praSegmentationDir,
            streamThreshold, minLength, smoothingWindowSize,
            cairosDir, sizeClasses, demCrs
        )
        if not outPath:
            continue

        nOk += 1
        totalPolys += nPolys
        totalAreaSqm += sumAreaSqm
        for k in totalClassCounts:
            totalClassCounts[k] += classCounts[k]

        baseNoExt = os.path.splitext(os.path.basename(inPath))[0]
        filteredBase = os.path.join(
            praSegmentationDir,
            f"{baseNoExt}_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}"
        )

        kept, outGeo, filteredClasses = applySizeFilter(
            outPath, sizeFilter, filteredBase, cairosDir, sizeClasses
        )
        totalPolysFiltered += kept
        if kept > 0:
            gdfF = gpd.read_file(outGeo)
            totalAreaSqmFiltered += float(gdfF["area_m"].sum())
        for k in totalClassCountsFiltered:
            totalClassCountsFiltered[k] += filteredClasses[k]

    tDt = time.perf_counter() - tAll

    cc, ccF = totalClassCounts, totalClassCountsFiltered
    log.info("Step 05: total n=%d, area=%.3f km², classes={%s}",
             totalPolys, totalAreaSqm / 1e6,
             ", ".join(f"{k}:{cc[k]}" for k in cc))
    log.info("Step 05: filtered n=%d, area=%.3f km², classes={%s}",
             totalPolysFiltered, totalAreaSqmFiltered / 1e6,
             ", ".join(f"{k}:{ccF[k]}" for k in ccF))
    log.info("Step 05 complete in %.2fs", tDt)
