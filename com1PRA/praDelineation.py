# ------------------ Step 01: PRA Delineation --------------------------- #
#
# Purpose :
#     Generate a continuous PRA (Potential Release Area) raster field from DEM and
#     forest layers using fuzzy-logic terrain indicators. These indicators quantify
#     slope, ruggedness, wind shelter, and forest attenuation effects.
#
# Inputs :
#     - DEM (from [MAIN])
#     - FOREST (from [MAIN])
#
# Outputs :
#     - pra.tif              PRA probability field
#     - slope.tif            Slope (deg)
#     - aspect.tif           Aspect (deg)
#     - ruggC.tif            Terrain ruggedness
#     - windshelter.tif      Wind shelter index
#     - forestC.tif          Forest cover attenuation index
#
# Config :
#     [praDELINEATION]       Fuzzy-logic thresholds, radius, wind parameters
#
# Consumes :
#     - None
#
# Provides :
#     - PRA base layers for Step 02 (PRA Selection) and Step 05 (PRA Processing)
#
# Method Reference :
#     Veitinger, J., Purves, R. S., & Sovilla, B. (2016):
#       Multi-scale fuzzy-logic identification of potential slab-avalanche
#       release areas. NHESS 16(10), 2211–2225.
#       https://doi.org/10.5194/nhess-16-2211-2016
#
#     Schumacher, J. et al. (2022):
#       Use of forest attribute maps for automated ATES modelling.
#       Scandinavian Journal of Forest Research, 37(4), 264–275.
#       https://doi.org/10.1080/02827581.2022.2096921
#
# Implementation :
#     Multi-indicator fuzzy logic approach combining:
#         • slope suitability
#         • forest attenuation
#         • terrain ruggedness
#         • wind shelter effect
#     to construct a continuous PRA likelihood field.
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
import time
import logging

import numpy as np
import rasterio
from osgeo import gdal
from numba import njit, prange

import in1Utils.dataUtils as dataUtils
from in1Utils.dataUtils import timeIt

log = logging.getLogger(__name__)

import os
import time
import logging

import numpy as np
import rasterio
from osgeo import gdal
from numba import njit, prange

import in1Utils.dataUtils as dataUtils
from in1Utils.dataUtils import timeIt

log = logging.getLogger(__name__)


# ------------------ Utility Functions ------------------ #

def sectorMask(shape, centre, radius, angleRange):
    x, y = np.ogrid[:shape[0], :shape[1]]
    cx, cy = centre
    tMin, tMax = np.deg2rad(angleRange)
    if tMax < tMin:
        tMax += 2 * np.pi
    r2 = (x - cx) ** 2 + (y - cy) ** 2
    theta = np.arctan2(x - cx, y - cy) - tMin
    theta %= (2 * np.pi)
    circMask = r2 <= radius ** 2
    angleMask = theta <= (tMax - tMin)
    return circMask * angleMask


def windShelterPrep(radius, direction, tolerance, cellSize):
    xSize = ySize = 2 * radius + 1
    xArr, yArr = np.mgrid[0:xSize, 0:ySize]
    cellCenter = (radius, radius)
    dist = np.sqrt((xArr - cellCenter[0]) ** 2 + (yArr - cellCenter[1]) ** 2) * cellSize
    mask = sectorMask(dist.shape, (radius, radius), radius, (direction, tolerance))
    mask[radius, radius] = True
    return dist.astype(np.float32), mask.astype(np.bool_)


@njit(parallel=True)
def windShelterNumba(array, mask, dist, prob, radius, noData):
    ny, nx = array.shape
    ws = np.full((ny, nx), noData, dtype=np.float32)
    k = mask.shape[0]
    offset = radius
    maxn = k * k - 1

    for i in prange(offset, ny - offset):
        for j in range(offset, nx - offset):
            win = array[i - offset:i + offset + 1, j - offset:j + offset + 1]
            center = win[radius, radius]

            tmp = np.empty(maxn, dtype=np.float32)
            cnt = 0

            for ii in range(k):
                for jj in range(k):
                    if ii == radius and jj == radius:
                        continue
                    if mask[ii, jj]:
                        val = win[ii, jj]
                        if (val != noData) and (val != 0):
                            tmp[cnt] = np.arctan((val - center) / dist[ii, jj])
                            cnt += 1

            if cnt > 0:
                ws[i, j] = np.nanquantile(tmp[:cnt], prob)
            else:
                ws[i, j] = noData

    return ws


def bellCurve(arr, a, b, c):
    return 1 / (1 + ((arr - c) / a) ** (2 * b))


def slidingSum(arr):
    view = np.lib.stride_tricks.as_strided(
        arr,
        shape=(3, 3, arr.shape[0] - 2, arr.shape[1] - 2),
        strides=arr.strides * 2
    )
    return view.sum(axis=(0, 1))


# ------------------ Main ------------------ #

def runPraDelineation(cfg, workFlowDir):
    tAll = time.perf_counter()

    # --- Directories ---
    inputDir = workFlowDir["inputDir"]
    outputDir = workFlowDir["praDelineationDir"]
    cairosDir = workFlowDir["cairosDir"]
    os.makedirs(outputDir, exist_ok=True)

    # --- DEM & FOREST from [MAIN] ---
    demName = cfg["MAIN"]["DEM"]
    forestName = cfg["MAIN"]["FOREST"]
    demPath = os.path.join(inputDir, demName)
    forestPath = os.path.join(inputDir, forestName)

    # --- Parameters from [praDELINEATION] ---
    praCfg = cfg["praDELINEATION"]
    forestType = praCfg.get("forestType", "pcc")
    saveAllThresholds = praCfg.getboolean("saveAllThresholds", fallback=False)
    singleThreshold = praCfg.getfloat("singleThreshold", fallback=0.30)
    radius = praCfg.getint("radius", fallback=6)
    prob = praCfg.getfloat("prob", fallback=0.5)
    windDir = praCfg.getint("windDir", fallback=0)
    windTol = praCfg.getint("windTol", fallback=180)

    # --- Log main parameters in unified style ---
    relDemPath = os.path.relpath(demPath, start=cairosDir)
    relForestPath = os.path.relpath(forestPath, start=cairosDir)
    log.info(
        "...PRA delineation using: DEM=./%s, FOREST=./%s, forestType=%s, thr=%.2f, "
        "radius=%d, prob=%.2f, windDir=%d±%d",
        relDemPath,
        relForestPath,
        forestType,
        singleThreshold,
        radius,
        prob,
        windDir,
        windTol,
    )

    praDelineationDir = outputDir

    # --- Load DEM as reference (profile + nodata drives all outputs) ---
    dem, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demNoData = demProfile.get("nodata", 0)
    cellSize = demProfile["transform"][0]

    # ------------------------------------------------------------------
    # Step 1: Slope & Aspect (DEM → slope.tif, aspect.tif, aligned to DEM)
    # ------------------------------------------------------------------
    slopePath = os.path.join(praDelineationDir, "slope.tif")
    aspectPath = os.path.join(praDelineationDir, "aspect.tif")

    with timeIt("slope/aspect", level=logging.INFO):
        if not os.path.exists(slopePath) or not os.path.exists(aspectPath):
            demDs = gdal.Open(demPath)
            gdal.DEMProcessing(slopePath, demDs, "slope", computeEdges=True)
            gdal.DEMProcessing(aspectPath, demDs, "aspect", computeEdges=True)
            del demDs

            arr, _ = dataUtils.readRaster(slopePath, return_profile=True)
            dataUtils.saveRaster(
                demPath,
                slopePath,
                arr.astype("float32")[np.newaxis, ...],
                dtype="float32",
                nodata=demNoData,
            )
            arr, _ = dataUtils.readRaster(aspectPath, return_profile=True)
            dataUtils.saveRaster(
                demPath,
                aspectPath,
                arr.astype("float32")[np.newaxis, ...],
                dtype="float32",
                nodata=demNoData,
            )
            log.info("...slope/aspect created from DEM")
        else:
            log.info("...slope/aspect already exist → reusing existing rasters")

    # ------------------------------------------------------------------
    # Step 2: Windshelter (DEM + kernel geometry)
    # ------------------------------------------------------------------
    with timeIt("windshelter (numba)", level=logging.INFO):
        dist, mask = windShelterPrep(
            radius,
            windDir - windTol + 270,
            windDir + windTol + 270,
            cellSize,
        )
        wsArr = windShelterNumba(
            dem, mask.astype(np.float32), dist.astype(np.float32), prob, radius, demNoData
        )
        dataUtils.saveRaster(
            demPath,
            os.path.join(praDelineationDir, "windshelter.tif"),
            wsArr[np.newaxis, ...],
            dtype="float32",
            nodata=demNoData,
        )

    # ------------------------------------------------------------------
    # Step 3: Ruggedness (slope + aspect)
    # ------------------------------------------------------------------
    with timeIt("ruggedness", level=logging.INFO):
        aspect, _ = dataUtils.readRaster(aspectPath, return_profile=True)
        slope2d, _ = dataUtils.readRaster(slopePath, return_profile=True)

        slopeRad = slope2d * np.pi / 180.0
        aspectRad = aspect * np.pi / 180.0
        xyRaster = np.sin(slopeRad)
        zRaster = np.cos(slopeRad)
        xRaster = np.sin(aspectRad) * xyRaster
        yRaster = np.cos(aspectRad) * xyRaster

        ySumRaster = slidingSum(yRaster)
        xSumRaster = slidingSum(xRaster)
        zSumRaster = slidingSum(zRaster)

        resultRaster = np.sqrt(xSumRaster**2 + ySumRaster**2 + zSumRaster**2)
        ruggednessRaster = 1 - (resultRaster / 9.0)
        ruggednessRaster = np.pad(
            ruggednessRaster, (1, 1), "constant", constant_values=(0, 0)
        )
        rugg = ruggednessRaster.reshape(1, *ruggednessRaster.shape)
        dataUtils.saveRaster(
            demPath,
            os.path.join(praDelineationDir, "ruggedness.tif"),
            rugg,
            dtype="float32",
            nodata=demNoData,
        )

        ruggC = (rugg >= 0.02).astype("float32")
        dataUtils.saveRaster(
            demPath,
            os.path.join(praDelineationDir, "ruggC.tif"),
            ruggC,
            dtype="float32",
            nodata=demNoData,
        )

    # ------------------------------------------------------------------
    # Step 4: Curves (slopeC, windshelterC)
    # ------------------------------------------------------------------
    with timeIt("slopeC & windshelterC", level=logging.INFO):
        slopeBand1, _ = dataUtils.readRaster(slopePath, return_profile=True)
        windShelterBand1, _ = dataUtils.readRaster(
            os.path.join(praDelineationDir, "windshelter.tif"),
            return_profile=True,
        )
        slope = slopeBand1[np.newaxis, ...]
        windShelter = windShelterBand1[np.newaxis, ...]

        slopeC = bellCurve(slope, 11, 4, 43)
        slopeC[0] = np.where(slope[0] > 60, 0.0, slopeC[0])
        slopeC[0] = np.where(slope[0] < 28, 0.0, slopeC[0])
        dataUtils.saveRaster(
            demPath,
            os.path.join(praDelineationDir, "slopeC.tif"),
            slopeC,
            dtype="float32",
            nodata=demNoData,
        )

        windShelterC = bellCurve(windShelter, 2, 5, 2).astype("float32")
        dataUtils.saveRaster(
            demPath,
            os.path.join(praDelineationDir, "windshelterC.tif"),
            windShelterC,
            dtype="float32",
            nodata=demNoData,
        )

    # ------------------------------------------------------------------
    # Step 5: Forest contribution (forestC) + DEM/FOREST grid check
    # ------------------------------------------------------------------
    if forestType == "stems":
        a, b, c = 350, 2.5, -150
    elif forestType in ("pcc", "no_forest"):
        a, b, c = 40, 3.5, -15
    elif forestType == "bav":
        a, b, c = 20, 3.5, -10
    elif forestType == "sen2cc":
        a, b, c = 50, 1.5, 0
    else:
        raise ValueError("Unknown forest type.")

    with timeIt("forestC", level=logging.INFO):
        if forestType in ["pcc", "stems", "bav", "sen2cc"]:
            forest2d, forestProfile = dataUtils.readRaster(
                forestPath, return_profile=True
            )

            # DEM ↔ FOREST compatibility (log-only; no resampling here)
            issues = []
            if forestProfile.get("crs") != demProfile.get("crs"):
                issues.append("CRS")
            if forestProfile.get("transform") != demProfile.get("transform"):
                issues.append("transform")
            fw, fh = forestProfile.get("width"), forestProfile.get("height")
            dw, dh = demProfile.get("width"), demProfile.get("height")
            if (fw, fh) != (dw, dh):
                issues.append("dimensions")

            if issues:
                log.warning(
                    "DEM/FOREST grid mismatch (%s). DEM=(crs=%s, size=%sx%s), "
                    "FOREST=(crs=%s, size=%sx%s)",
                    ", ".join(issues),
                    demProfile.get("crs"),
                    dw,
                    dh,
                    forestProfile.get("crs"),
                    fw,
                    fh,
                )

            forest = forest2d[np.newaxis, ...]
        elif forestType == "no_forest":
            dem2d, _ = dataUtils.readRaster(demPath, return_profile=True)
            forest = np.where(dem2d > -100, 0, dem2d)[np.newaxis, ...]
        else:
            # Should not reach here due to earlier check
            forest = None  # type: ignore

        forestC = bellCurve(forest, a, b, c)
        forestC = np.where(forestC <= 0, 1, forestC).astype("float32")
        dataUtils.saveRaster(
            demPath,
            os.path.join(praDelineationDir, "forestC.tif"),
            forestC,
            dtype="float32",
            nodata=demNoData,
        )

    # ------------------------------------------------------------------
    # Step 6: Continuous PRA (pra.tif) from slopeC, windshelterC, forestC, ruggC
    # ------------------------------------------------------------------
    with timeIt("continuous PRA", level=logging.INFO):
        ruggC2d, _ = dataUtils.readRaster(
            os.path.join(praDelineationDir, "ruggC.tif"),
            return_profile=True,
        )
        ruggC = ruggC2d[np.newaxis, ...]

        # reload slopeC / windshelterC to avoid carrying large arrays around
        slopeC, _ = dataUtils.readRaster(
            os.path.join(praDelineationDir, "slopeC.tif"),
            return_profile=True,
        )
        slopeC = slopeC[np.newaxis, ...]
        windShelterC, _ = dataUtils.readRaster(
            os.path.join(praDelineationDir, "windshelterC.tif"),
            return_profile=True,
        )
        windShelterC = windShelterC[np.newaxis, ...]

        minVar = np.minimum(slopeC, windShelterC)
        minVar = np.minimum(minVar, forestC)

        pra = (1 - minVar) * minVar + minVar * (slopeC + windShelterC + forestC) / 3.0
        pra = pra - ruggC
        pra = np.float32(pra)
        pra[pra <= 0] = 0

        dataUtils.saveRaster(
            demPath,
            os.path.join(praDelineationDir, "pra.tif"),
            pra,
            dtype="float32",
            nodata=demNoData,
        )

    # ------------------------------------------------------------------
    # Step 7: PRA Thresholding (binary PRA masks)
    # ------------------------------------------------------------------
    with timeIt("PRA thresholding", level=logging.INFO):
        pra2d, _ = dataUtils.readRaster(
            os.path.join(praDelineationDir, "pra.tif"),
            return_profile=True,
        )
        praRead = pra2d[np.newaxis, ...]

        if saveAllThresholds:
            for praThreshold in np.arange(0.00, 1.01, 0.01):
                binary = (praRead >= praThreshold).astype("int16")
                fileName = f"pra_binary_th{int(praThreshold * 100):03d}.tif"
                dataUtils.saveRaster(
                    demPath,
                    os.path.join(praDelineationDir, fileName),
                    binary,
                    dtype="int16",
                    nodata=-9999,
                )
            log.info("...save all binary PRA thresholds - done")
        else:
            praThreshold = singleThreshold
            binary = (praRead >= praThreshold).astype("int16")
            fileName = f"pra_binary_th{int(praThreshold * 100):03d}.tif"
            dataUtils.saveRaster(
                demPath,
                os.path.join(praDelineationDir, fileName),
                binary,
                dtype="int16",
                nodata=-9999,
            )
            log.info("...save single binary PRA %.2f threshold - done", praThreshold)

    log.info("...PRA delineation - done: %.2fs", time.perf_counter() - tAll)
