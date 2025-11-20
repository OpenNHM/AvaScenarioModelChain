# ------------------ Step 02: PRA Selection ----------------------------- #
#
# Purpose :
#     Filter the continuous PRA raster field generated in Step 01 using
#     configurable thresholds, elevation limits, and directional (aspect)
#     sectors. This step produces binary PRA masks representing potential
#     release areas that satisfy the selected criteria.
#
# Inputs :
#     - pra.tif        (PRA probability field from Step 01)
#     - aspect.tif     (Aspect raster from Step 01)
#     - DEM            (from [MAIN], used for elevation filtering)
#
# Outputs :
#     - Aspect-sector-filtered PRA rasters:
#         pra<aspectSector>sec<minElev>-<maxElev>.tif
#       (naming pattern may vary depending on configuration)
#
# Config :
#     [praSELECTION]
#         • selectedThreshold   PRA probability cutoff
#         • minElev, maxElev    elevation limits
#         • aspectSector        directional constraint (e.g. N, E, SW, all)
#         • maskCommRegion      optional masking by commission region
#
# Consumes :
#     - Outputs from Step 01 (PRA delineation)
#
# Provides :
#     - Filtered PRA masks required for:
#         • Step 03 (Subcatchments)
#         • Step 04 (PRA Processing)
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
import geopandas as gpd
import rasterio.features
from typing import cast

import in1Utils.dataUtils as dataUtils
from in1Utils.dataUtils import timeIt, relPath

log = logging.getLogger(__name__)

# ----------------------------- IO Helpers -----------------------------

def readRaster(path):
    """Read first band + return transform/CRS via dataUtils."""
    data, profile = dataUtils.readRaster(path, return_profile=True)
    return data, profile["transform"], profile["crs"]

def writeRaster(path, data, demPath, *, dtype="int16", nodata=-9999):
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
        return "N"
    elif 22.5 <= aspect < 67.5:
        return "NE"
    elif 67.5 <= aspect < 112.5:
        return "E"
    elif 112.5 <= aspect < 157.5:
        return "SE"
    elif 157.5 <= aspect < 202.5:
        return "S"
    elif 202.5 <= aspect < 247.5:
        return "SW"
    elif 247.5 <= aspect < 292.5:
        return "W"
    elif 292.5 <= aspect < 337.5:
        return "NW"
    return "Unknown"

def applyAspectFilter(aspectData, sectors=("N", "NE", "E", "SE", "S", "SW", "W", "NW")):
    """Binary mask for allowed aspect sectors."""
    aspectSectorMap = np.vectorize(getAspectSector)(aspectData)
    return np.isin(aspectSectorMap, sectors).astype(int)

# ----------------------------- Sector Groups -----------------------------

sectorGroups = [
    ["N", "NE", "NW"],
    ["E", "NE", "SE"],
    ["S", "SE", "SW"],
    ["W", "SW", "NW"],
]

sectorNameToGroups = {
    "secn": ["N", "NE", "NW"],
    "sece": ["E", "NE", "SE"],
    "secs": ["S", "SE", "SW"],
    "secw": ["W", "SW", "NW"],
}

sectorNames = {
    frozenset(["N", "NE", "NW"]): "secN",
    frozenset(["E", "NE", "SE"]): "secE",
    frozenset(["S", "SE", "SW"]): "secS",
    frozenset(["W", "SW", "NW"]): "secW",
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

    delineationDir = workFlowDir["praDelineationDir"]
    inputDir = workFlowDir["inputDir"]
    outputDir = workFlowDir["praSelectionDir"]
    cairosDir = workFlowDir["cairosDir"]
    os.makedirs(outputDir, exist_ok=True)

    # --- Input rasters from Step 01 ---
    praPath = _findFileExactOrGlob(delineationDir, "pra.tif", "pra*.tif")
    aspectPath = _findFileExactOrGlob(delineationDir, "aspect.tif", "aspect*.tif")

    # --- DEM from [MAIN] ---
    demName = cfg["MAIN"]["DEM"]
    demPath = os.path.join(inputDir, demName)

    # --- Parameters from [praSELECTION] ---
    selCfg = cfg["praSELECTION"]
    praThreshold = selCfg.getfloat("selectedThreshold", fallback=0.30)
    minElev = selCfg.getint("minElev", fallback=0)
    maxElev = selCfg.getint("maxElev", fallback=4000)
    aspectSector = selCfg.get("aspectSector", fallback="all").strip().lower()
    maskCommRegion = selCfg.getboolean("maskCommRegion", fallback=False)

    # --- Read rasters ---
    praData, transform, demCrs = readRaster(praPath)
    demData, _, _ = readRaster(demPath)
    aspectData, _, _ = readRaster(aspectPath)

    # --- Optional commission region mask ---
    if maskCommRegion:
        commRegionName = cfg["MAIN"].get("COMMISSIONREGION", "").strip()
        if not commRegionName:
            raise ValueError("...maskCommRegion=True but COMMISSIONREGION missing in [MAIN].")
        commRegionPath = os.path.join(inputDir, commRegionName)
        if not os.path.exists(commRegionPath):
            raise FileNotFoundError(f"Commission region file not found: {commRegionPath}")

        commGdf = cast(gpd.GeoDataFrame, gpd.read_file(commRegionPath))
        if getattr(commGdf, "crs", None) != demCrs:
            commGdf = commGdf.to_crs(demCrs)

        with timeIt("commission region mask"):
            maskArr = rasterio.features.rasterize(
                [(geom, 1) for geom in commGdf.geometry],
                out_shape=praData.shape,
                transform=transform,
                fill=0,
                dtype="uint8",
            )
            praData = np.where(maskArr == 1, praData, 0)
            log.info("...applied commission region mask (%s)", os.path.basename(commRegionPath))

    # --- Basic filters ---
    praFiltered = applyPraFilter(praData, praThreshold=praThreshold)
    demFiltered = applyDemFilter(demData, minElev=minElev, maxElev=maxElev)

    # --- Aspect sectors to process ---
    if aspectSector == "all":
        groupsToRun = sectorGroups
    else:
        keys = [k.strip().lower() for k in aspectSector.split(",") if k.strip()]
        invalid = [k for k in keys if k not in sectorNameToGroups]
        if invalid:
            raise ValueError(
                f"Invalid aspectSector value(s): {invalid}. "
                f"Valid: 'all' or any of: secN, secE, secS, secW"
            )
        groupsToRun = [sectorNameToGroups[k] for k in keys]

    # --- Relative paths for logs ---
    relDemPath = relPath(demPath, cairosDir)
    relPraDir = relPath(delineationDir, cairosDir)

    log.info(
        "...PRA selection using: DEM=./%s, threshold=%.2f, elev=%dhm-%dhm, aspect=%s, maskCommRegion=%s",
        relDemPath, praThreshold, minElev, maxElev, aspectSector, maskCommRegion,
    )
    log.info("...using pra/aspect from: ./%s", relPraDir)

    # --- Run per sector ---
    praThreshold100 = f"{int(praThreshold * 100):03d}"
    for sectors in groupsToRun:
        with timeIt(f"aspect sector {sectors}"):
            aspectFiltered = applyAspectFilter(aspectData, sectors=sectors)
            finalMask = np.logical_and(np.logical_and(praFiltered, demFiltered), aspectFiltered)

            sectorKey = frozenset(sectors)
            sectorsStrOut = sectorNames.get(sectorKey, "-".join(sectors))
            outPath = os.path.join(outputDir, f"pra{praThreshold100}{sectorsStrOut}.tif")

            writeRaster(outPath, finalMask.astype(np.int16), demPath)
            relOutPath = relPath(outPath, cairosDir)
            log.info("...selected PRA written to: ./%s", relOutPath)

    log.info("Step 02: PRA selection done in %.2fs", time.perf_counter() - tAll)
