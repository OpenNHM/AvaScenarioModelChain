# ------------------ Step 06: PRA Assign Elevation & Size ------------------ #
#
# Purpose :
#     Assign elevation statistics, elevation bands, size classes, and
#     administrative region metadata to PRA polygons produced in Step 05.
#     This enriches each segmented PRA with all required attributes for
#     FlowPy preparation and scenario parameterization.
#
# Inputs :
#     - Segmented and size-filtered PRA GeoJSONs (from Step 05)
#     - DEM raster (for elevation statistics)
#     - Commission / region polygons (administrative overlays)
#
# Outputs :
#     - Enriched PRA GeoJSONs containing:
#         • min / max / mean elevation
#         • elevation band classification
#         • size class (via user-defined thresholds)
#         • administrative region assignment
#
# Config :
#     [praASSIGNELEV]      Elevation band definitions
#     [praSEGMENTATION]    Size class thresholds and filters
#     [praSUBCATCHMENTS]   Optional subcatchment metadata
#     [MAIN]               DEM / region datasets
#
# Consumes :
#     - Size-filtered PRA GeoJSONs from Step 05
#
# Provides :
#     - Elevation- and size-classed PRA datasets required for:
#         • Step 07 (PRA → FlowPy preparation)
#         • FlowPy parameterization and scenario creation
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

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.mask import mask

import ati.mod0Helper.dataUtils as dataUtils
from ati.mod0Helper.cfgUtils import loadElevationBands, parseRangeCsv

# ------------------ Logging setup ------------------ #

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)

# ------------------ Minimal helpers ------------------ #

def loadSizeClasses(cfg):
    """Read size class ranges from [praSEGMENTATION]."""
    sect = cfg["praSEGMENTATION"]
    sizeClasses = {}
    for i in range(1, 6):
        key = f"sizeClass{i}"
        if not sect.get(key, fallback=None):
            continue
        lo, hi = parseRangeCsv(sect.get(key))
        sizeClasses[i] = (lo, hi)
    if not sizeClasses:
        raise ValueError("No size classes defined in [praSEGMENTATION].")
    return sizeClasses


def assignElevationBand(row, bands):
    emin, emax, emean = row.get("elev_min"), row.get("elev_max"), row.get("elev_mean")
    if emin is not None and emax is not None:
        for label, (lo, hi) in bands:
            if (emin >= lo) and (emax < hi):
                return label, "minMax"
    if emean is not None:
        for label, (lo, hi) in bands:
            if (emean >= lo) and (emean < hi):
                return label, "mean"
    return "Unknown", "None"


# ------------------ File discovery & naming ------------------ #

def findFilteredGeojsons(praSegmentationDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter):
    """Locate filtered PRA GeoJSONs based on naming suffix."""
    suffix = f"_BnCh2_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}.geojson"
    pattern = f"*{suffix}"
    return sorted(glob.glob(os.path.join(praSegmentationDir, pattern))), suffix


def simplifiedBasename(inputPath, suffix):
    """Strip the long suffix to produce a short, clean base name."""
    base = os.path.splitext(os.path.basename(inputPath))[0]
    if base.endswith(suffix.replace(".geojson", "")):
        short = base[: -len(suffix.replace(".geojson", ""))].rstrip("_")
        return short
    return base


def renameAndReorderColumns(gdf):
    """Rename standardized columns and reorder according to CAIROS schema."""
    gdf = gdf.copy()
    colMap = {
        "area_m": "praAreaM",
        "size_class": "praAreaSized",
        "elev_min": "praElevMin",
        "elev_max": "praElevMax",
        "elev_mean": "praElevMean",
        "elev_band": "praElevBand",
        "elev_rule": "praElevBandRule",
    }
    gdf = gdf.rename(columns=colMap)
    gdf["praAreaVol"] = np.zeros(len(gdf), dtype=float).round(2)

    desiredOrder = [
        "praAreaM",
        "praAreaSized",
        "praAreaVol",
        "praElevMin",
        "praElevMax",
        "praElevMean",
        "praElevBand",
        "praElevBandRule",
    ]
    otherCols = [c for c in gdf.columns if c not in desiredOrder and c != "geometry"]
    return gdf[desiredOrder + otherCols + ["geometry"]]


# ------------------ Elevation stats ------------------ #

def addElevationStatsFromDem(gdf, demPath, demNoData):
    """Add min, max, mean elevation stats per geometry."""
    if len(gdf) == 0:
        return gdf.assign(elev_min=[], elev_max=[], elev_mean=[])

    mins, maxs, means = [], [], []
    try:
        with rasterio.open(demPath) as src:
            for geom in gdf.geometry:
                if geom is None or geom.is_empty:
                    mins.append(None); maxs.append(None); means.append(None); continue
                try:
                    data, _ = mask(src, [geom.__geo_interface__], crop=True, filled=True, nodata=demNoData)
                    arr = data[0]
                    maskArr = (arr == demNoData) | (arr <= 0)
                    ma = np.ma.array(arr, mask=maskArr)
                    if ma.count() == 0:
                        mins.append(None); maxs.append(None); means.append(None)
                    else:
                        mins.append(round(float(ma.min()), 2))
                        maxs.append(round(float(ma.max()), 2))
                        means.append(round(float(ma.mean()), 2))
                except Exception:
                    log.exception("Zonal stats failed for a geometry; writing None.")
                    mins.append(None); maxs.append(None); means.append(None)
    except Exception:
        log.exception("Failed to open DEM for zonal stats: ./%s", demPath)
        return gdf.assign(elev_min=[None]*len(gdf),
                          elev_max=[None]*len(gdf),
                          elev_mean=[None]*len(gdf))
    return gdf.assign(elev_min=mins, elev_max=maxs, elev_mean=means)


def assignSizeClass(row, sizeClasses):
    """Assign PRA size class from area (m²)."""
    a = row.get("area_m")
    if a is None:
        try:
            a = float(row.geometry.area)
        except Exception:
            return None
    for cls, (lo, hi) in sizeClasses.items():
        if (a >= lo) and (a < hi):
            return cls
    return None


# ------------------ Overlay helpers ------------------ #

def assignByLargestOverlapFast(praGdf, regionGdf, regionCols, demCrs):
    """Assign region attributes by maximum overlap (spatial join + area test)."""
    pra = praGdf.copy()
    pra = pra.set_index(pra.index)
    joined = gpd.sjoin(pra, regionGdf, how="left", predicate="intersects")
    results = {col: [None] * len(pra) for col in regionCols}

    for idx, group in joined.groupby(joined.index):
        geom = pra.loc[idx].geometry
        if geom is None or geom.is_empty:
            continue
        candidates = regionGdf.loc[group["index_right"].dropna().astype(int).unique()]
        if len(candidates) == 0:
            continue
        candidates = candidates.assign(overlapArea=candidates.geometry.intersection(geom).area)
        best = candidates.sort_values("overlapArea", ascending=False).iloc[0]
        for col in regionCols:
            results[col][idx] = best[col]

    for col in regionCols:
        pra[col] = results[col]
    return pra


# ------------------ Main driver ------------------ #

def runPraAssignElevSize(cfg, workFlowDir):
    """Step 06: assign elevation bands, size classes, and region tags to PRA polygons."""
    tAll = time.perf_counter()

    cairosDir = workFlowDir["cairosDir"]
    praSegmentationDir = workFlowDir.get("praSegmentationDir", os.path.join(cairosDir, "06_praSegmentation"))
    praAssignElevSizeDir = workFlowDir.get("praAssignElevSizeDir", os.path.join(cairosDir, "07_praAssignElevSize"))
    os.makedirs(praAssignElevSizeDir, exist_ok=True)

    # Config parameters
    streamThreshold = cfg["praSUBCATCHMENTS"].getint("streamThreshold", fallback=500)
    minLength = cfg["praSUBCATCHMENTS"].getint("minLength", fallback=100)
    smoothingWindowSize = cfg["praSUBCATCHMENTS"].getint("smoothingWindowSize", fallback=5)
    sizeFilter = cfg["praSEGMENTATION"].getfloat("sizeFilter", fallback=500.0)

    inputDir = workFlowDir["inputDir"]
    demPath = os.path.join(inputDir, cfg["MAIN"].get("DEM", "").strip())

    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demNoData = demProfile.get("nodata", -9999.0)
    demCrs = demProfile["crs"]

    elevationBands = loadElevationBands(cfg)
    sizeClasses = loadSizeClasses(cfg)

    # Commission and region polygons
    commissionsPath = os.path.join(inputDir, cfg["MAIN"].get("COMMISSIONS", "").strip())
    avaReportPath = os.path.join(inputDir, cfg["MAIN"].get("AVAREPORT", "").strip())

    commissions = gpd.read_file(commissionsPath).to_crs(demCrs)
    microRegions = gpd.read_file(avaReportPath).to_crs(demCrs)

    filteredFiles, longSuffix = findFilteredGeojsons(
        praSegmentationDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter
    )
    targetDir = praAssignElevSizeDir

    log.info("Step 06: Start PRA elevation/size assignment...")
    log.info("Input: ./%s", dataUtils.relPath(praSegmentationDir, cairosDir))
    log.info("Output (flat layout): ./%s", dataUtils.relPath(targetDir, cairosDir))
    log.info("DEM: ./%s", dataUtils.relPath(demPath, cairosDir))

    if not filteredFiles:
        log.error("No filtered GeoJSONs found matching *%s in ./%s",
                  longSuffix, dataUtils.relPath(praSegmentationDir, cairosDir))
        return

    nOk = nFail = totalPolys = 0

    for inPath in filteredFiles:
        try:
            short = simplifiedBasename(inPath, longSuffix)
            with dataUtils.timeIt(f"assignElevSize({short})"):
                gdf = gpd.read_file(inPath)

                if "area_m" not in gdf.columns:
                    gdf = dataUtils.attachAreasMetersNoGeomChange(gdf[["geometry"]].copy(), demCrs)
                else:
                    gdf = gdf[["geometry", "area_m"]]

                # Elevation stats
                gdfElev = addElevationStatsFromDem(gdf, demPath, demNoData)
                outElev = os.path.join(targetDir, f"{short}-Elev.geojson")
                gdfElev.to_file(outElev, driver="GeoJSON")

                # Elevation band assignment
                bandsCols = gdfElev.apply(lambda r: assignElevationBand(r, elevationBands),
                                          axis=1, result_type="expand")
                bandsCols.columns = ["elev_band", "elev_rule"]
                gdfBand = gdfElev.join(bandsCols)
                outBands = os.path.join(targetDir, f"{short}-ElevBands.geojson")
                gdfBand.to_file(outBands, driver="GeoJSON")

                # Size class
                gdfBand["size_class"] = gdfBand.apply(lambda r: assignSizeClass(r, sizeClasses), axis=1)

                # Region overlays
                gdfBand = assignByLargestOverlapFast(
                    gdfBand, commissions, ["LKGebietID", "LKGebiet", "LKRegion"], demCrs
                )
                gdfBand = assignByLargestOverlapFast(
                    gdfBand, microRegions, ["LWDGebietID"], demCrs
                )

                # Final rename/reorder
                gdfFinal = renameAndReorderColumns(gdfBand)
                outFinal = os.path.join(targetDir, f"{short}-ElevBands-Sized.geojson")
                gdfFinal.to_file(outFinal, driver="GeoJSON")

                nOk += 1
                totalPolys += len(gdfFinal)
                log.info(
                    "Processed ./%s → ./%s",
                    dataUtils.relPath(inPath, cairosDir),
                    dataUtils.relPath(outFinal, cairosDir),
                )

        except Exception:
            nFail += 1
            log.exception("Step 06 failed for ./%s", dataUtils.relPath(inPath, cairosDir))

    log.info("Step 06 complete: files_ok=%d, files_failed=%d, total_polys=%d", nOk, nFail, totalPolys)
    log.info("Step 06 total time: %.2fs", time.perf_counter() - tAll)
