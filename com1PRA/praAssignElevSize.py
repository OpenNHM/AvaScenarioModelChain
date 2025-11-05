# ------------------ Step 06: PRA Assign Elevation & Size ------------------ #
# Purpose: Add elevation, size, and administrative region metadata
#          to segmented PRA polygons from Step 05.
# Inputs:  Filtered PRA GeoJSONs (Step 05) + DEM + Commission / Region polygons
# Outputs: GeoJSONs with elevation statistics, elevation bands,
#          and final size-classed + region-tagged PRA features.
# Config:  [praASSIGNELEV], [praSEGMENTATION], [praSUBCATCHMENTS], [MAIN]
# Consumes: Filtered PRA GeoJSONs (size-filtered)
# Provides: Elevation- and size-classed PRA GeoJSONs for FlowPy preparation (Step 07)

import os
import glob
import time
import logging
import contextlib

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.mask import mask

import in1Utils.dataUtils as dataUtils
from in1Utils.dataUtils import relPath, timeIt

# ------------------ Logging setup ------------------ #

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)

# ------------------ Minimal helpers ------------------ #

def _parseRangeCsv(value: str):
    """Parse CSV-style numeric range string 'low,high' (supports 'inf')."""
    v = (value or "").strip()
    parts = [p.strip() for p in v.split(",")]
    if len(parts) != 2:
        raise ValueError(f"Invalid range definition: '{value}' (expected 'low,high')")
    lo = float(parts[0])
    hi = float("inf") if parts[1].lower() == "inf" else float(parts[1])
    return lo, hi


def loadSizeClasses(cfg):
    """Read size class ranges from [praSEGMENTATION]."""
    sect = cfg["praSEGMENTATION"]
    sizeClasses = {}
    for i in range(1, 6):
        key = f"sizeClass{i}"
        if not sect.get(key, fallback=None):
            continue
        lo, hi = _parseRangeCsv(sect.get(key))
        sizeClasses[i] = (lo, hi)
    if not sizeClasses:
        raise ValueError("No size classes defined in [praSEGMENTATION].")
    return sizeClasses


def loadElevationBands(cfg):
    """Read elevation bands from [praASSIGNELEV]."""
    sect = cfg["praASSIGNELEV"]
    bands = []
    i = 1
    while True:
        key = f"elevationBand{i}"
        val = sect.get(key, fallback=None)
        if not val:
            break
        lo, hi = _parseRangeCsv(val)
        lo_i = int(round(lo))
        hi_i = int(round(hi if hi != float("inf") else 9999))
        label = f"{lo_i:04d}-{hi_i:04d}"
        bands.append((label, (lo, hi)))
        i += 1
    if not bands:
        raise ValueError("No elevation bands defined in [praASSIGNELEV].")
    return bands


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


def attachAreasMetersNoGeomChange(gdf, demCrs):
    """Compute planar area (m² / km²) without modifying geometry."""
    try:
        if len(gdf) == 0:
            return gdf.assign(area_m=[], area_km=[])
        if getattr(demCrs, "is_projected", None) is True:
            areaSeries = gdf.to_crs(demCrs).geometry.area
        else:
            try:
                utm = gdf.estimate_utm_crs()
                areaSeries = gdf.to_crs(utm).geometry.area
            except Exception:
                areaSeries = gdf.geometry.area
        return gdf.assign(area_m=areaSeries.values, area_km=(areaSeries.values / 1e6))
    except Exception:
        log.exception("Area computation failed; writing zeros (geometry unchanged).")
        z = np.zeros(len(gdf))
        return gdf.assign(area_m=z, area_km=z / 1e6)


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
    subfolderName = longSuffix.replace(".geojson", "").lstrip("_")
    targetDir = os.path.join(praAssignElevSizeDir, subfolderName)
    os.makedirs(targetDir, exist_ok=True)

    log.info("Step 06: Start PRA elevation/size assignment...")
    log.info("Input: ./%s", relPath(praSegmentationDir, cairosDir))
    log.info("Output: ./%s", relPath(targetDir, cairosDir))
    log.info("DEM: ./%s", relPath(demPath, cairosDir))

    if not filteredFiles:
        log.error("No filtered GeoJSONs found matching *%s in ./%s",
                  longSuffix, relPath(praSegmentationDir, cairosDir))
        return

    nOk = nFail = totalPolys = 0

    for inPath in filteredFiles:
        try:
            short = simplifiedBasename(inPath, longSuffix)
            with timeIt(f"assignElevSize({short})"):
                gdf = gpd.read_file(inPath)

                if "area_m" not in gdf.columns:
                    gdf = attachAreasMetersNoGeomChange(gdf[["geometry"]].copy(), demCrs)
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
                log.info("Processed ./%s → ./%s", relPath(inPath, cairosDir), relPath(outFinal, cairosDir))

        except Exception:
            nFail += 1
            log.exception("Step 06 failed for ./%s", relPath(inPath, cairosDir))

    log.info("Step 06 complete: files_ok=%d, files_failed=%d, total_polys=%d", nOk, nFail, totalPolys)
    log.info("Step 06 total time: %.2fs", time.perf_counter() - tAll)
