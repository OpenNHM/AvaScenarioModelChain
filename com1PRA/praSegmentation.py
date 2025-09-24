# praSegmentation.py

import os
import glob
import time
import logging
import contextlib

import geopandas as geopandas
import pandas as pandas
import numpy as numpy

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

def findPraFiles(praProcessingDir, code3):
    """
    Find polygonized PRA layers produced in step 05 using the threshold code.
    Example filenames: pra030secE_BnCh2.shp, pra030secN_BnCh2.shp, ...
    """
    pattern = f"pra{code3}sec*_BnCh2.shp"
    return sorted(glob.glob(os.path.join(praProcessingDir, pattern)))

def buildSubcatchSmoothedPath(praSubcatchmentsDir, streamThreshold, minLength, smoothingWindowSize, weightedSlopeFlow):
    """
    Construct the exact smoothed subcatchments file name:
    subcatchments_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}_{weighted|unweighted}.shp
    """
    weight_tag = "weighted" if weightedSlopeFlow else "unweighted"
    fname = f"subcatchments_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}_{weight_tag}.shp"
    return os.path.join(praSubcatchmentsDir, fname)

def _parseRangeCsv(value):
    v = (value or "").strip()
    parts = [p.strip() for p in v.split(",")]
    if len(parts) != 2:
        raise ValueError(f"Invalid size class definition: '{value}' (expected 'low,high')")
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

def classifyAreasSqm(areasSqm, sizeClasses):
    counts = {k: 0 for k in sizeClasses}
    for a in areasSqm:
        for cid, (lo, hi) in sizeClasses.items():
            if lo <= a < hi:
                counts[cid] += 1
                break
    return counts

def attachAreasMetersNoGeomChange(gdf, demCrs):
    """
    Compute areas in meters using a temporary projected CRS (prefer DEM CRS if projected),
    but KEEP the geometry CRS unchanged.
    """
    try:
        if len(gdf) == 0:
            return gdf.assign(area_m=[], area_km=[])
        # DEM projected? use it. Else estimate UTM; else fall back to native units.
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
    

def applySizeFilter(inputShpPath, sizeFilter, outBasePath, cairosDir, sizeClasses):
    """
    Keep only features with area_m >= sizeFilter. Writes:
      - {outBasePath}.shp   (only if kept > 0 to avoid empty SHP issues)
      - {outBasePath}.geojson
    Returns: kept, removed, outShp_or_None, outGeo, filteredClasses
    """
    gdf = geopandas.read_file(inputShpPath)

    # Ensure area_m exists; fallback to native units if missing (shouldn't happen)
    if "area_m" not in gdf.columns:
        gdf = gdf.assign(area_m=gdf.geometry.area)

    # Keep only what's needed to avoid attribute collisions
    gdf = gdf[["geometry", "area_m"]]

    before = len(gdf)
    sizeFilter = float(sizeFilter)
    gdfFiltered = gdf[gdf["area_m"] >= sizeFilter]
    kept, removed = len(gdfFiltered), before - len(gdfFiltered)

    outShp = f"{outBasePath}.shp"
    outGeo = f"{outBasePath}.geojson"

    # Save (SHP only if non-empty to avoid driver errors; GeoJSON is fine empty)
    outShpWritten = None
    if kept > 0:
        gdfFiltered.to_file(outShp)
        outShpWritten = outShp
    else:
        log.debug("Size filter produced 0 features for ./%s; skipping shapefile write.", relPath(inputShpPath, cairosDir))

    gdfFiltered.to_file(outGeo, driver="GeoJSON")

    # Class stats for filtered set
    filteredClasses = classifyAreasSqm(gdfFiltered["area_m"].astype(float).tolist(), sizeClasses)

    log.info("...apply size filter = %.0f m² → kept=%d, removed=%d → out=./%s",
             sizeFilter, kept, removed,
             relPath(outShp if outShpWritten else outGeo, cairosDir))

    return kept, removed, outShpWritten, outGeo, filteredClasses


 


# ------------------ Core per-file operation ------------------ #

def processSinglePraLayer(inPath, subcatchGdf, outDir,
                          streamThreshold, minLength, smoothingWindowSize,
                          cairosDir, sizeClasses, demCrs):
    """
    Simple: overlay in PRA CRS, explode, keep geometry, attach area attrs without changing geometry CRS.
    """
    try:
        with timeIt(f"processSinglePraLayer({os.path.basename(inPath)})"):
            praGdf = geopandas.read_file(inPath)

            # Overlay in PRA CRS (no prefilter, no snapping, no precision grid)
            praUse  = praGdf
            subcUse = subcatchGdf.to_crs(praUse.crs) if (subcatchGdf.crs != praUse.crs) else subcatchGdf
            clipped = geopandas.overlay(praUse, subcUse, how='intersection', keep_geom_type=True)

            if len(clipped) == 0:
                log.debug("No intersection for ./%s", relPath(inPath, cairosDir))
                return None, 0, 0.0, {k: 0 for k in sizeClasses}

            clipped = clipped.explode(index_parts=True).reset_index(drop=True)
            clipped = clipped[['geometry']]

            # Compute areas (m²/km²) without changing geometry CRS
            clipped = attachAreasMetersNoGeomChange(clipped, demCrs)

            # Class stats
            classCounts = classifyAreasSqm(clipped["area_m"].astype(float).tolist(), sizeClasses)

            # Output
            base = os.path.splitext(os.path.basename(inPath))[0]
            outPath = os.path.join(outDir, f"{base}_subC{streamThreshold}_{minLength}_{smoothingWindowSize}.shp")
            clipped.to_file(outPath)

            # Per-file INFO
            nPolys = int(len(clipped))
            sumAreaSqm = float(clipped["area_m"].sum())
            cc = classCounts
            log.info("...segmentation for: ./%s → n=%d, area=%.3f km², classes={1:%d,2:%d,3:%d,4:%d,5:%d}",
                     relPath(outPath, cairosDir), nPolys, (sumAreaSqm / 1e6),
                     cc[1], cc[2], cc[3], cc[4], cc[5])

            return outPath, nPolys, sumAreaSqm, classCounts

    except Exception:
        log.exception("Segmentation failed for ./%s", relPath(inPath, cairosDir))
        raise


# ------------------ Main driver ------------------ #

def runPraSegmentation(cfg, workFlowDir):
    """
    Step 06: PRA segmentation.
    Intersects polygonized PRA from 05_praProcessing with **smoothed** subcatchments
    from 04_praSubcatchments, writes results to 06_praSegmentation.
    """
    tAll = time.perf_counter()

    # --- Directories ---
    cairosDir = workFlowDir['cairosDir']
    praProcessingDir = workFlowDir['praProcessingDir']
    praSubcatchmentsDir = workFlowDir['praSubcatchmentsDir']
    praSegmentationDir = workFlowDir.get('praSegmentationDir') or os.path.join(cairosDir, "06_praSegmentation")
    os.makedirs(praSegmentationDir, exist_ok=True)

    # --- Config ---
    thrF = cfg['praSELECTION'].getfloat('selectedThreshold', fallback=0.30)
    code3 = f"{int(round(thrF * 100)):03d}"  # robust (0.30 -> 030)

    streamThreshold     = cfg['praSUBCATCHMENTS'].getint('streamThreshold', fallback=500)
    minLength           = cfg['praSUBCATCHMENTS'].getint('minLength', fallback=100)
    smoothingWindowSize = cfg['praSUBCATCHMENTS'].getint('smoothingWindowSize', fallback=5)
    weightedSlopeFlow   = cfg['praSUBCATCHMENTS'].getboolean('weightedSlopeFlow', fallback=False)

    # --- DEM as reference (CRS, cellSize, nodata) ---
    inputDir = workFlowDir['inputDir']
    demName  = cfg['MAIN'].get('DEM', '').strip()
    demPath  = os.path.join(inputDir, demName)
    _, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demNoData = demProfile.get('nodata', 0)
    cellSize  = demProfile['transform'][0]
    demCrs    = demProfile['crs']

    # --- Size classes & filter from INI ---
    sizeClasses = loadSizeClasses(cfg)
    sizeFilter  = cfg['praSEGMENTATION'].getfloat('sizeFilter', fallback=500.0)

    # --- Inputs ---
    praFiles = findPraFiles(praProcessingDir, code3)
    subcatchPath = buildSubcatchSmoothedPath(praSubcatchmentsDir, streamThreshold, minLength, smoothingWindowSize, weightedSlopeFlow)

    # --- Parameters logging (minimal, as requested) ---
    log.info("...PRA segmentation using: out=./%s, SubC=./%s, streamThr=%s, minLen=%s, smoothWin=%s, weightedSF=%s",
             relPath(praSegmentationDir, cairosDir),
             relPath(subcatchPath, cairosDir),
             streamThreshold, minLength, smoothingWindowSize, weightedSlopeFlow)

    # --- Early exits ---
    if not praFiles:
        log.error("No polygonized PRA layers found matching pra%sec*_BnCh2.shp in ./%s", code3, relPath(praProcessingDir, cairosDir))
        log.info("...PRA segmentation - failed (no inputs): %.2fs", time.perf_counter() - tAll)
        return
    if not os.path.exists(subcatchPath):
        log.error("Smoothed subcatchments file not found: ./%s", relPath(subcatchPath, cairosDir))
        log.info("...PRA segmentation - failed (no subcatchments): %.2fs", time.perf_counter() - tAll)
        return

    # --- Read subcatchments once ---
    try:
        subcatchGdf = geopandas.read_file(subcatchPath)
    except Exception:
        log.exception("Failed to read subcatchments: ./%s", relPath(subcatchPath, cairosDir))
        log.info("...PRA segmentation - failed (read error): %.2fs", time.perf_counter() - tAll)
        return

    # --- Optional CRS consistency check against DEM (log-only, do nothing) ---
    try:
        if subcatchGdf.crs == demCrs:
            log.debug("Subcatchments CRS matches DEM CRS; no reprojection applied.")
        else:
            log.debug("Subcatchments CRS != DEM CRS; proceeding without changing geometry (areas still computed in meters).")
    except Exception:
        pass

    # --- Process each PRA file ---
    nOk, nFail = 0, 0
    totalPolys, totalAreaSqm = 0, 0
    totalClassCounts = {k: 0 for k in sizeClasses}

    totalPolysFiltered, totalAreaSqmFiltered = 0, 0
    totalClassCountsFiltered = {k: 0 for k in sizeClasses}

    for inPath in praFiles:
        try:
            outPath, nPolys, sumAreaSqm, classCounts = processSinglePraLayer(
                inPath, subcatchGdf, praSegmentationDir,
                streamThreshold, minLength, smoothingWindowSize,
                cairosDir, sizeClasses, demCrs
            )
            if outPath is None:
                # no intersection → skip both aggregation and filtering
                continue

            # --- aggregate originals ---
            nOk += 1
            totalPolys += nPolys
            totalAreaSqm += float(sumAreaSqm)
            for k in totalClassCounts:
                totalClassCounts[k] += classCounts[k]

            # --- filter and aggregate filtered stats ---
            baseNoExt = os.path.splitext(os.path.basename(inPath))[0]
            filteredBase = os.path.join(
                praSegmentationDir,
                f"{baseNoExt}_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}"
            )
            kept, removed, outShp, outGeo, filteredClasses = applySizeFilter(
                outPath, sizeFilter, filteredBase, cairosDir, sizeClasses
            )

            totalPolysFiltered += int(kept)
            if kept > 0 and os.path.exists(outShp):
                gdfFiltered = geopandas.read_file(outShp)
                if "area_m" in gdfFiltered:
                    totalAreaSqmFiltered += float(gdfFiltered["area_m"].sum())
            for k in totalClassCountsFiltered:
                totalClassCountsFiltered[k] += filteredClasses[k]

        except Exception:
            nFail += 1
            log.exception("Segmentation/filtering failed for ./%s", relPath(inPath, cairosDir))
            continue


    # --- Done ---
    tDt = time.perf_counter() - tAll

    # original stats
    cc = totalClassCounts
    log.info("...PRA segmentation stats: total pra=%d, total pra area=%.3f km², size classes={1:%d,2:%d,3:%d,4:%d,5:%d}",
            totalPolys, (totalAreaSqm / 1e6),
            cc[1], cc[2], cc[3], cc[4], cc[5])

    # filtered stats
    ccF = totalClassCountsFiltered
    log.info("...PRA segmentation stats: filtered pra=%d, total pra area=%.3f km², size classes={1:%d,2:%d,3:%d,4:%d,5:%d}",
            totalPolysFiltered, (totalAreaSqmFiltered / 1e6),
            ccF[1], ccF[2], ccF[3], ccF[4], ccF[5])

    log.info("...PRA segmentation - done: %.2fs", tDt)

