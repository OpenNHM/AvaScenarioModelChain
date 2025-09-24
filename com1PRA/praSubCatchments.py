# subcatchments.py

import os
import sys
import time
import pathlib
import logging
import warnings
import contextlib
import geopandas as gpd
from shapely.validation import make_valid
from whitebox.whitebox_tools import WhiteboxTools


# ------------------ Utility Functions ------------------ #

log = logging.getLogger(__name__)
wbt = WhiteboxTools()

# quiet down third-party noise
logging.getLogger("pyogrio").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", category=RuntimeWarning, module=r"pyogrio\..*")

def relPathForLog(absPath: str, projectRoot: pathlib.Path) -> str:
    """Return './...' style path relative to project root; fall back to abs if outside."""
    try:
        rel = pathlib.Path(absPath).resolve().relative_to(projectRoot.resolve())
        return f"./{rel.as_posix()}"
    except Exception:
        return absPath

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

@contextlib.contextmanager
def timeIt(label, level=logging.DEBUG):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        log.log(level, "%s finished in %.2fs", label, dt)

def fixInvalidGeometries(shpPath):
    log.debug("Fixing invalid geometries: %s", os.path.basename(shpPath))
    gdf = gpd.read_file(shpPath)
    gdf["geometry"] = gdf["geometry"].apply(make_valid)
    gdf.to_file(shpPath)

@contextlib.contextmanager
def suppressWbtOutput():
    """Silence WhiteboxTools stdout (progress spam)."""
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
    if ext == "":
        p2 = buildPath(inputDir, cand + ".tif")
        if os.path.exists(p2):
            return p2
    raise FileNotFoundError(f"DEM not found (from INI): {cand} under inputDir: {inputDir}")


# ------------------ Hydro Prep ------------------ #

def prepHydroGrids(demPath, outDir, weightedSlopeFlow=False):
    """Fill DEM, derive flow dir/accum, slope; optional weighting."""
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
        log.debug("Using slope‑weighted flow accumulation")
        weightedFlow = buildPath(outDir, "weighted_flow.tif")
        with suppressWbtOutput():
            wbt.multiply(flowAcc, slopeTif, weightedFlow)
        return filledDem, flowDir, weightedFlow, "_weighted"

    log.debug("Using unweighted flow accumulation")
    return filledDem, flowDir, flowAcc, "_unweighted"


# ------------------ Parameter Run ------------------ #

def runParamSet(flowDir, flowAccToUse, outDir, streamThreshold, minLength, smoothingWindowSize, flowSuffix):
    """Run one full subcatchment delineation for given parameters."""
    log.info("...streamThr=%s, minLen=%s, smooth=%s, weightedFlow=%s",
             streamThreshold, minLength, smoothingWindowSize, flowSuffix)

    streamsTif           = buildPath(outDir, f"streams_{streamThreshold}_minlen{minLength}_smooth{smoothingWindowSize}{flowSuffix}.tif")
    filtStreamsTif       = buildPath(outDir, f"filtered_streams_{streamThreshold}_minlen{minLength}_smooth{smoothingWindowSize}{flowSuffix}.tif")
    junctionsTif         = buildPath(outDir, f"stream_junctions_{streamThreshold}_minlen{minLength}_smooth{smoothingWindowSize}{flowSuffix}.tif")
    subcatchTif          = buildPath(outDir, f"subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.tif")
    subcatchShp          = buildPath(outDir, f"subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp")
    smoothSubcatchTif    = buildPath(outDir, f"smooth_subcatchments_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.tif")
    smoothSubcatchShp    = buildPath(outDir, f"subcatchments_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp")
    nonSmoothSubcatchShp = buildPath(outDir, f"subcatchments_non_smoothed_{streamThreshold}_{minLength}_{smoothingWindowSize}{flowSuffix}.shp")

    with timeIt("Extract streams"):
        with suppressWbtOutput():
            wbt.extract_streams(flowAccToUse, streamsTif, streamThreshold)

    with timeIt("Remove short streams"):
        with suppressWbtOutput():
            wbt.remove_short_streams(flowDir, streamsTif, filtStreamsTif, min_length=minLength, esri_pntr=False)

    with timeIt("Stream junctions"):
        with suppressWbtOutput():
            wbt.stream_link_identifier(flowDir, filtStreamsTif, junctionsTif)

    with timeIt("Watershed delineation"):
        with suppressWbtOutput():
            wbt.watershed(flowDir, junctionsTif, subcatchTif)

    with timeIt("Raster→Vector (raw)"):
        with suppressWbtOutput():
            wbt.raster_to_vector_polygons(subcatchTif, subcatchShp)
        fixInvalidGeometries(subcatchShp)

    with timeIt("Smooth + Vectorize"):
        with suppressWbtOutput():
            wbt.majority_filter(subcatchTif, smoothSubcatchTif, smoothingWindowSize)
            wbt.raster_to_vector_polygons(smoothSubcatchTif, smoothSubcatchShp)
        fixInvalidGeometries(smoothSubcatchShp)

    with timeIt("Non‑smoothed vector"):
        with suppressWbtOutput():
            wbt.raster_to_vector_polygons(subcatchTif, nonSmoothSubcatchShp)
        fixInvalidGeometries(nonSmoothSubcatchShp)

    log.info("...completed streamThr=%s, minLen=%s, smooth=%s, weightedFlow=%s",
             streamThreshold, minLength, smoothingWindowSize, flowSuffix)




# ------------------ Main ------------------ #

# ------------------ Main ------------------ #
def runSubcatchments(cfg, workFlowDir):
    tAll = time.perf_counter()

    inputDir  = workFlowDir["inputDir"]
    outputDir = ensureDir(workFlowDir["praSubcatchmentsDir"])

    # ✨ DEM now comes from [MAIN]
    demValue   = cfg["MAIN"]["DEM"]
    demPathAbs = resolveDemPath(demValue, inputDir)

    # project root = parent of inputDir (for ./ relative logging)
    projectRoot = pathlib.Path(inputDir).resolve().parent
    try:
        demPathLog = f"./{pathlib.Path(demPathAbs).resolve().relative_to(projectRoot).as_posix()}"
    except Exception:
        demPathLog = demPathAbs  # fallback

    subcCfg = cfg["praSUBCATCHMENTS"]
    streamThresholdList     = parseIntList(subcCfg.get("streamThreshold", "500"))
    minLengthList           = parseIntList(subcCfg.get("minLength", "200"))
    smoothingWindowSizeList = parseIntList(subcCfg.get("smoothingWindowSize", "5"))
    weightedSlopeFlow       = subcCfg.getboolean("weightedSlopeFlow", fallback=False)

    # compact, single line (relative DEM path)
    log.info(
        "...subcatchments using: DEM=%s, streamThr=%s, minLen=%s, smooth=%s, weightedFlow=%s",
        demPathLog,
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
                log.debug("running: thr=%s, minLen=%s, smooth=%s%s", thr, ml, sw, flowSuffix)
                runParamSet(flowDir, flowAccToUse, outputDir, thr, ml, sw, flowSuffix)

    log.info("...all parameter sets done in %.2fs", time.perf_counter() - tAll)