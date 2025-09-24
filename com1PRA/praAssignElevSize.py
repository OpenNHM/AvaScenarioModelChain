# assignElevSize.py

import os
import glob
import time
import logging
import contextlib

import geopandas as geopandas
import numpy as numpy
import rasterio
from rasterio.mask import mask

import in1Utils.dataUtils as dataUtils

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)

# ------------------ Minimal helpers ------------------ #

def relPath(path, cairosDir):
    try:
        return os.path.relpath(path, start=cairosDir)
    except Exception:
        return path

@contextlib.contextmanager
def timeIt(label, level=logging.DEBUG):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        log.log(level, "%s finished in %.2fs", label, time.perf_counter() - t0)

def _parseRangeCsv(value):
    v = (value or "").strip()
    parts = [p.strip() for p in v.split(",")]
    if len(parts) != 2:
        raise ValueError(f"Invalid range definition: '{value}' (expected 'low,high')")
    lo = float(parts[0])
    hi = float('inf') if parts[1].lower() == "inf" else float(parts[1])
    return lo, hi

def loadSizeClasses(cfg):
    sect = cfg["praSEGMENTATION"]
    sizeClasses = {}
    for i in range(1, 6):
        key = f"sizeClass{i}"
        lo, hi = _parseRangeCsv(sect.get(key, fallback=None))
        sizeClasses[i] = (lo, hi)
    return sizeClasses

def loadElevationBands(cfg):
    sect = cfg["praASSIGNELEV"]
    bands = []
    i = 1
    while True:
        key = f"elevationBand{i}"
        if not sect.get(key, fallback=None):
            break
        lo, hi = _parseRangeCsv(sect.get(key))
        lo_i = int(round(lo))
        hi_i = int(round(hi if hi != float("inf") else 9999))
        label = f"{lo_i:04d}-{hi_i:04d}"
        bands.append((label, (lo, hi)))
        i += 1
    if not bands:
        raise ValueError("No elevation bands defined in [praASSIGNELEV].")
    return bands

def assignElevationBand(row, bands):
    emin = row.get("elev_min")
    emax = row.get("elev_max")
    emean = row.get("elev_mean")

    if (emin is not None) and (emax is not None):
        for label, (lo, hi) in bands:
            if (emin >= lo) and (emax < hi):
                return label, "minMax"

    if emean is not None:
        for label, (lo, hi) in bands:
            if (emean >= lo) and (emean < hi):
                return label, "mean"

    return "Unknown", "None"

def attachAreasMetersNoGeomChange(gdf, demCrs):
    try:
        if len(gdf) == 0:
            return gdf.assign(area_m=[], area_km=[])
        isProjected = getattr(demCrs, "is_projected", None)
        if isProjected is True:
            area_series = gdf.to_crs(demCrs).geometry.area
        else:
            try:
                utm = gdf.estimate_utm_crs()
                area_series = gdf.to_crs(utm).geometry.area
            except Exception:
                area_series = gdf.geometry.area
        return gdf.assign(area_m=area_series.values, area_km=(area_series.values / 1e6))
    except Exception:
        log.exception("Area computation failed; writing zeros (geometry unchanged).")
        z = numpy.zeros(len(gdf))
        return gdf.assign(area_m=z, area_km=z / 1e6)

# ------------------ File discovery & naming ------------------ #

def findFilteredGeojsons(praSegmentationDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter):
    suffix = f"_BnCh2_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}.geojson"
    pattern = f"*{suffix}"
    return sorted(glob.glob(os.path.join(praSegmentationDir, pattern))), suffix

def simplifiedBasename(inputPath, suffix):
    base = os.path.splitext(os.path.basename(inputPath))[0]
    if base.endswith(suffix.replace(".geojson", "")):
        short = base[: -len(suffix.replace(".geojson", ""))]
        if short.endswith("_"):
            short = short[:-1]
        return short
    return base


def renameAndReorderColumns(gdf):
    gdf = gdf.copy()
    # rename according to mapping
    colMap = {
        "area_m": "praAreaM",
        "size_class": "praAreaSized",
        "elev_min": "praElevMin",
        "elev_max": "praElevMax",
        "elev_mean": "praElevMean",
        "elev_band": "praElevBand",
        "elev_rule": "praElevBandRule"
    }
    gdf = gdf.rename(columns=colMap)

    # add empty praAreaVol initialized to 0.00
    gdf["praAreaVol"] = numpy.zeros(len(gdf), dtype=float).round(2)

    # reorder columns (geometry last)
    desiredOrder = [
        "praAreaM",
        "praAreaSized",
        "praAreaVol",
        "praElevMin",
        "praElevMax",
        "praElevMean",
        "praElevBand",
        "praElevBandRule"
    ]
    otherCols = [c for c in gdf.columns if c not in desiredOrder and c != "geometry"]
    gdf = gdf[desiredOrder + otherCols + ["geometry"]]

    return gdf


# ------------------ Elevation stats ------------------ #

def addElevationStatsFromDem(gdf, demPath, demNoData):
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
                    m = (arr == demNoData) | (arr <= 0)
                    ma = numpy.ma.array(arr, mask=m)
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
    """
    Faster alternative to assignByLargestOverlap.
    Uses spatial join to prefilter candidates, then selects region with max overlap.
    """
    pra = praGdf.copy()
    pra = pra.set_index(pra.index)

    # spatial join (candidates only)
    joined = geopandas.sjoin(pra, regionGdf, how="left", predicate="intersects")

    # storage
    results = {col: [None] * len(pra) for col in regionCols}

    for idx, group in joined.groupby(joined.index):
        geom = pra.loc[idx].geometry
        if geom is None or geom.is_empty:
            continue
        candidates = regionGdf.loc[group["index_right"].dropna().astype(int).unique()]
        if len(candidates) == 0:
            continue
        # approximate overlap by area of intersection
        candidates["overlapArea"] = candidates.geometry.intersection(geom).area
        best = candidates.sort_values("overlapArea", ascending=False).iloc[0]
        for col in regionCols:
            results[col][idx] = best[col]

    # attach results
    for col in regionCols:
        pra[col] = results[col]
    return pra

# ------------------ Main driver ------------------ #

def runPraAssignElevSize(cfg, workFlowDir):
    tAll = time.perf_counter()

    cairosDir = workFlowDir["cairosDir"]
    praSegmentationDir = workFlowDir.get("praSegmentationDir") or os.path.join(cairosDir, "06_praSegmentation")
    praAssignElevSizeDir = workFlowDir.get("praAssignElevSizeDir") or os.path.join(cairosDir, "07_praAssignElevSize")
    os.makedirs(praAssignElevSizeDir, exist_ok=True)

    streamThreshold = cfg["praSUBCATCHMENTS"].getint("streamThreshold", fallback=500)
    minLength = cfg["praSUBCATCHMENTS"].getint("minLength", fallback=100)
    smoothingWindowSize = cfg["praSUBCATCHMENTS"].getint("smoothingWindowSize", fallback=5)
    sizeFilter = cfg["praSEGMENTATION"].getfloat("sizeFilter", fallback=500.0)

    inputDir = workFlowDir["inputDir"]
    demName = cfg["MAIN"].get("DEM", "").strip()
    demPath = os.path.join(inputDir, demName)
    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demNoData = demProfile.get("nodata", -9999.0)
    demCrs = demProfile["crs"]

    elevationBands = loadElevationBands(cfg)
    sizeClasses = loadSizeClasses(cfg)

    # overlay layers
    commissionsPath = os.path.join(inputDir, cfg["MAIN"].get("COMMISSIONS", "").strip())
    avaReportPath = os.path.join(inputDir, cfg["MAIN"].get("AVAREPORT", "").strip())
    commissions = geopandas.read_file(commissionsPath).to_crs(demCrs)
    microRegions = geopandas.read_file(avaReportPath).to_crs(demCrs)

    filteredFiles, longSuffix = findFilteredGeojsons(
        praSegmentationDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter
    )

    subfolderName = longSuffix.replace(".geojson", "").lstrip("_")
    targetDir = os.path.join(praAssignElevSizeDir, subfolderName)
    os.makedirs(targetDir, exist_ok=True)

    log.info("...PRA assign elev/size using: in=./%s, out=./%s, DEM=./%s",
             relPath(praSegmentationDir, cairosDir),
             relPath(targetDir, cairosDir),
             relPath(demPath, cairosDir))

    if not filteredFiles:
        log.error("No filtered GeoJSONs found matching *%s in ./%s",
                  longSuffix, relPath(praSegmentationDir, cairosDir))
        return

    nOk, nFail, totalPolys = 0, 0, 0

    for inPath in filteredFiles:
        try:
            with timeIt(f"assignElevSize({os.path.basename(inPath)})"):
                short = simplifiedBasename(inPath, longSuffix)
                gdf = geopandas.read_file(inPath)

                if "area_m" not in gdf.columns:
                    gdf = attachAreasMetersNoGeomChange(gdf[["geometry"]].copy(), demCrs)
                else:
                    gdf = gdf[["geometry", "area_m"]]

                # add elevation stats
                gdfElev = addElevationStatsFromDem(gdf, demPath, demNoData)
                outElev = os.path.join(targetDir, f"{short}-Elev.geojson")
                gdfElev.to_file(outElev, driver="GeoJSON")

                # assign elevation band
                bandsCols = gdfElev.apply(lambda r: assignElevationBand(r, elevationBands),
                                          axis=1, result_type="expand")
                bandsCols.columns = ["elev_band", "elev_rule"]
                gdfBand = gdfElev.copy()
                gdfBand["elev_band"] = bandsCols["elev_band"]
                gdfBand["elev_rule"] = bandsCols["elev_rule"]
                outBands = os.path.join(targetDir, f"{short}-ElevBands.geojson")
                gdfBand.to_file(outBands, driver="GeoJSON")

                # assign size class
                gdfBand["size_class"] = gdfBand.apply(lambda r: assignSizeClass(r, sizeClasses), axis=1)

                # assign commissions by largest overlap (fast)
                gdfBand = assignByLargestOverlapFast(
                    gdfBand, commissions, ["LKGebietID", "LKGebiet", "LKRegion"], demCrs
                )

                # assign avaReport region by largest overlap (fast)
                gdfBand = assignByLargestOverlapFast(
                    gdfBand, microRegions, ["LWDGebietID"], demCrs
                )

                # rename, reorder, add praAreaVol
                gdfFinal = renameAndReorderColumns(gdfBand)

                outFinal = os.path.join(targetDir, f"{short}-ElevBands-Sized.geojson")
                gdfFinal.to_file(outFinal, driver="GeoJSON")

                n = int(len(gdfFinal))
                totalPolys += n
                log.info("...processed: ./%s", relPath(outElev, cairosDir))
                log.info("...saved to: ./%s", relPath(outFinal, cairosDir))
                nOk += 1

        except Exception:
            nFail += 1
            log.exception("Assign elev/size failed for ./%s", relPath(inPath, cairosDir))
            continue

    log.info("...PRA assign elev/size stats: files_ok=%d, files_failed=%d, total_polys=%d",
             nOk, nFail, totalPolys)
    log.info("...PRA assign elev/size - done: %.2fs", time.perf_counter() - tAll)
