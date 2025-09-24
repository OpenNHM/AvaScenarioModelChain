import os
import glob
import time
import logging
import numpy as np

import in1Utils.dataUtils as dataUtils

log = logging.getLogger(__name__)

# ----------------------------- IO Helpers (via dataUtils) -----------------------------

def readRaster(path):
    """Read first band + return transform/CRS via dataUtils."""
    data, profile = dataUtils.readRaster(path, return_profile=True)
    return data, profile["transform"], profile["crs"]

def writeRaster(path, data, demPath, *, dtype='int16', nodata=-9999):
    """Write raster aligned to DEM grid (CRS/transform match exactly)."""
    dataUtils.saveRaster(demPath, path, data, dtype=dtype, nodata=nodata)

# ----------------------------- Filters -----------------------------

def applyPraFilter(praData, praThreshold=0.4):
    """Binary mask from PRA threshold."""
    return np.where(praData >= praThreshold, 1, 0)

def applyDemFilter(demData, minElev=1000, maxElev=4000):
    """Binary mask for elevation range."""
    return np.where((demData >= minElev) & (demData <= maxElev), 1, 0)

def getAspectSector(aspect):
    """Map aspect degrees → compass sector code."""
    if 337.5 <= aspect or aspect < 22.5:
        return 'N'
    elif 22.5 <= aspect < 67.5:
        return 'NE'
    elif 67.5 <= aspect < 112.5:
        return 'E'
    elif 112.5 <= aspect < 157.5:
        return 'SE'
    elif 157.5 <= aspect < 202.5:
        return 'S'
    elif 202.5 <= aspect < 247.5:
        return 'SW'
    elif 247.5 <= aspect < 292.5:
        return 'W'
    elif 292.5 <= aspect < 337.5:
        return 'NW'

def applyAspectFilter(aspectData, sectors=('N','NE','E','SE','S','SW','W','NW')):
    """Binary mask for allowed aspect sectors."""
    aspectSectorMap = np.vectorize(getAspectSector)(aspectData)
    return np.isin(aspectSectorMap, sectors).astype(int)

# ----------------------------- Sector Groups -----------------------------

sectorGroups = [
    ['N', 'NE', 'NW'],
    ['E', 'NE', 'SE'],
    ['S', 'SE', 'SW'],
    ['W', 'SW', 'NW'],
]

sectorNameToGroups = {
    "secn": ['N','NE','NW'],
    "sece": ['E','NE','SE'],
    "secs": ['S','SE','SW'],
    "secw": ['W','SW','NW'],
}

sectorNames = {
    frozenset(['N','NE','NW']): "secN",
    frozenset(['E','NE','SE']): "secE",
    frozenset(['S','SE','SW']): "secS",
    frozenset(['W','SW','NW']): "secW",
}

# ----------------------------- Internal Helpers -----------------------------

def _findFileExactOrGlob(folder, exactName, fallbackPattern):
    """Find exact filename or first match for glob pattern."""
    p = os.path.join(folder, exactName)
    if os.path.exists(p):
        return p
    hits = sorted(glob.glob(os.path.join(folder, fallbackPattern)))
    if not hits:
        raise FileNotFoundError(
            f"Could not find '{exactName}' nor pattern '{fallbackPattern}' in {folder}"
        )
    return hits[0]

# ----------------------------- Main -----------------------------
def runPraSelection(cfg, workFlowDir):
    tAll = time.perf_counter()

    # --- Directories ---
    delineationDir = workFlowDir['praDelineationDir']   # e.g. "01_praDelineation"
    inputDir       = workFlowDir['inputDir']
    outputDir      = workFlowDir['praSelectionDir']
    cairosDir      = workFlowDir['cairosDir']
    os.makedirs(outputDir, exist_ok=True)

    # --- Inputs produced by praDelineation step ---
    praPath    = _findFileExactOrGlob(delineationDir, "pra.tif",    "pra*.tif")
    aspectPath = _findFileExactOrGlob(delineationDir, "aspect.tif", "aspect*.tif")

    # --- DEM from [MAIN] ---
    demName = cfg['MAIN']['DEM']
    demPath = os.path.join(inputDir, demName)

    # --- Parameters from [praSELECTION] ---
    selCfg       = cfg['praSELECTION']
    praThreshold = selCfg.getfloat('selectedThreshold', fallback=0.30)
    minElev      = selCfg.getint('minElev', fallback=0)
    maxElev      = selCfg.getint('maxElev', fallback=4000)
    aspectSector = selCfg.get('aspectSector', fallback='all').strip().lower()

    # --- Read rasters ---
    praData, _, _    = readRaster(praPath)
    demData, _, _    = readRaster(demPath)
    aspectData, _, _ = readRaster(aspectPath)

    # --- Basic filters ---
    praFiltered = applyPraFilter(praData, praThreshold=praThreshold)
    demFiltered = applyDemFilter(demData, minElev=minElev, maxElev=maxElev)

    # --- Determine sector groups ---
    if aspectSector == 'all':
        groupsToRun = sectorGroups
    else:
        keys = [k.strip().lower() for k in aspectSector.split(',') if k.strip()]
        invalid = [k for k in keys if k not in sectorNameToGroups]
        if invalid:
            raise ValueError(
                f"Invalid aspectSector value(s): {invalid}. "
                f"Valid: 'all' or any of: secN, secE, secS, secW (comma-separated)"
            )
        groupsToRun = [sectorNameToGroups[k] for k in keys]

    # --- Prepare relative paths for logging ---
    relDemPath = os.path.relpath(demPath, start=cairosDir)
    relPraDir  = os.path.relpath(delineationDir, start=cairosDir)

    # --- Compact, unified style log line ---
    log.info(
        "...PRA selection using: DEM=./%s, threshold=%.2f, elev=%dhm-%dhm, aspect=%s",
        relDemPath, praThreshold, minElev, maxElev, aspectSector
    )
    log.info("...using pra/aspect from: ./%s", relPraDir)

    # --- Run per-sector and write outputs aligned to DEM ---
    praThreshold100 = f"{int(praThreshold * 100):03d}"
    for sectors in groupsToRun:
        aspectFiltered = applyAspectFilter(aspectData, sectors=sectors)
        finalMask = np.logical_and(np.logical_and(praFiltered, demFiltered), aspectFiltered)

        sectorKey = frozenset(sectors)
        sectorsStrOut = sectorNames.get(sectorKey, '-'.join(sectors))

        outPath = os.path.join(outputDir, f"pra{praThreshold100}{sectorsStrOut}.tif")
        writeRaster(outPath, finalMask.astype(np.int16), demPath)

        relPath = os.path.relpath(outPath, start=cairosDir)
        log.info("...selected pra written to: ./%s", relPath)

    log.info("...PRA selection - done: %.2fs", time.perf_counter() - tAll)
