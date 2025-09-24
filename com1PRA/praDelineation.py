import os
import numpy as np
import rasterio
from osgeo import gdal
from numba import njit, prange
import time
import logging

import in1Utils.dataUtils as dataUtils

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
    inputDir  = workFlowDir['inputDir']
    outputDir = workFlowDir['praDelineationDir']
    cairosDir = workFlowDir['cairosDir']

    # --- DEM & FOREST from [MAIN] ---
    demName    = cfg['MAIN']['DEM']
    forestName = cfg['MAIN']['FOREST']
    demPath    = os.path.join(inputDir, demName)
    forestPath = os.path.join(inputDir, forestName)

    # --- Parameters from [praDELINEATION] ---
    praCfg             = cfg['praDELINEATION']
    forestType         = praCfg.get('forestType', 'pcc')
    saveAllThresholds  = praCfg.getboolean('saveAllThresholds', fallback=False)
    singleThreshold    = praCfg.getfloat('singleThreshold', fallback=0.30)
    radius             = praCfg.getint('radius', fallback=6)
    prob               = praCfg.getfloat('prob', fallback=0.5)
    windDir            = praCfg.getint('windDir', fallback=0)
    windTol            = praCfg.getint('windTol', fallback=180)

    # --- Log main parameters in unified style ---
    relDemPath    = os.path.relpath(demPath, start=cairosDir)
    relForestPath = os.path.relpath(forestPath, start=cairosDir)
    log.info(
        "...PRA delineation using: DEM=./%s, FOREST=./%s, forestType=%s, thr=%.2f, radius=%d, prob=%.2f, windDir=%d±%d",
        relDemPath, relForestPath, forestType, singleThreshold, radius, prob, windDir, windTol
    )

    praDelineationDir = outputDir

    # --- Load DEM as reference (need profile + nodata) ---
    dem, demProfile = dataUtils.readRaster(demPath, return_profile=True)
    demNoData = demProfile.get('nodata', 0)
    cellSize  = demProfile['transform'][0]

    # Step: Slope & Aspect
    t = time.perf_counter()
    slopePath  = os.path.join(praDelineationDir, "slope.tif")
    aspectPath = os.path.join(praDelineationDir, "aspect.tif")
    if not os.path.exists(slopePath) or not os.path.exists(aspectPath):
        demDs = gdal.Open(demPath)
        gdal.DEMProcessing(slopePath, demDs, "slope", computeEdges=True)
        gdal.DEMProcessing(aspectPath, demDs, "aspect", computeEdges=True)
        del demDs
        arr, _ = dataUtils.readRaster(slopePath, return_profile=True)
        dataUtils.saveRaster(demPath, slopePath, arr.astype("float32"), dtype="float32", nodata=demNoData)
        arr, _ = dataUtils.readRaster(aspectPath, return_profile=True)
        dataUtils.saveRaster(demPath, aspectPath, arr.astype("float32"), dtype="float32", nodata=demNoData)
        log.info("...slope/aspect - done: %.2fs", time.perf_counter() - t)

    # Windshelter
    t = time.perf_counter()
    dist, mask = windShelterPrep(radius, windDir - windTol + 270, windDir + windTol + 270, cellSize)
    wsArr = windShelterNumba(dem, mask.astype(np.float32), dist.astype(np.float32), prob, radius, demNoData)
    dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, 'windshelter.tif'),
                         wsArr[np.newaxis, ...], dtype="float32", nodata=demNoData)
    log.info("...windshelter (numba) - done: %.2fs", time.perf_counter() - t)

    # Ruggedness
    t = time.perf_counter()
    aspect, _ = dataUtils.readRaster(aspectPath, return_profile=True)
    slope2d, _ = dataUtils.readRaster(slopePath, return_profile=True)

    slopeRad = slope2d * np.pi / 180
    aspectRad = aspect * np.pi / 180
    xyRaster = np.sin(slopeRad)
    zRaster  = np.cos(slopeRad)
    xRaster  = np.sin(aspectRad) * xyRaster
    yRaster  = np.cos(aspectRad) * xyRaster

    ySumRaster = slidingSum(yRaster)
    xSumRaster = slidingSum(xRaster)
    zSumRaster = slidingSum(zRaster)

    resultRaster = np.sqrt(xSumRaster**2 + ySumRaster**2 + zSumRaster**2)
    ruggednessRaster = (1 - (resultRaster / 9))
    ruggednessRaster = np.pad(ruggednessRaster, (1, 1), "constant", constant_values=(0, 0))
    rugg = ruggednessRaster.reshape(1, *ruggednessRaster.shape)
    dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, "ruggedness.tif"),
                         rugg, dtype="float32", nodata=demNoData)
    log.info("...ruggedness - done: %.2fs", time.perf_counter() - t)

    t = time.perf_counter()
    ruggC = (rugg >= 0.02).astype("float32")
    dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, "ruggC.tif"),
                         ruggC, dtype="float32", nodata=demNoData)
    log.info("...ruggC - done: %.2fs", time.perf_counter() - t)

    # Curves
    t = time.perf_counter()
    slopeBand1, _ = dataUtils.readRaster(slopePath, return_profile=True)
    windShelterBand1, _ = dataUtils.readRaster(os.path.join(praDelineationDir, "windshelter.tif"), return_profile=True)
    slope       = slopeBand1[np.newaxis, ...]
    windShelter = windShelterBand1[np.newaxis, ...]

    slopeC = bellCurve(slope, 11, 4, 43)
    slopeC[0] = np.where(slope[0] > 60, 0., slopeC[0])
    slopeC[0] = np.where(slope[0] < 28, 0., slopeC[0])
    dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, "slopeC.tif"),
                         slopeC, dtype="float32", nodata=demNoData)
    log.info("...slopeC - done: %.2fs", time.perf_counter() - t)

    t = time.perf_counter()
    windShelterC = bellCurve(windShelter, 2, 5, 2).astype('float32')
    dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, "windshelterC.tif"),
                         windShelterC, dtype="float32", nodata=demNoData)
    log.info("...windshelterC - done: %.2fs", time.perf_counter() - t)

    # Forest
    if forestType == 'stems':
        a, b, c = 350, 2.5, -150
    elif forestType in ('pcc', 'no_forest'):
        a, b, c = 40, 3.5, -15
    elif forestType == 'bav':
        a, b, c = 20, 3.5, -10
    elif forestType == 'sen2cc':
        a, b, c = 50, 1.5, 0
    else:
        raise ValueError("Unknown forest type.")

    t = time.perf_counter()
    if forestType in ['pcc', 'stems', 'bav', 'sen2cc']:
        forest2d, _ = dataUtils.readRaster(forestPath, return_profile=True)
        forest = forest2d[np.newaxis, ...]
    elif forestType == 'no_forest':
        dem2d, _ = dataUtils.readRaster(demPath, return_profile=True)
        forest = np.where(dem2d > -100, 0, dem2d)[np.newaxis, ...]
    forestC = bellCurve(forest, a, b, c)
    forestC = np.where(forestC <= 0, 1, forestC).astype('float32')
    dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, "forestC.tif"),
                         forestC, dtype="float32", nodata=demNoData)
    log.info("...forestC - done: %.2fs", time.perf_counter() - t)

    # PRA
    t = time.perf_counter()
    ruggC2d, _ = dataUtils.readRaster(os.path.join(praDelineationDir, "ruggC.tif"), return_profile=True)
    ruggC = ruggC2d[np.newaxis, ...]
    minVar = np.minimum(slopeC, windShelterC)
    minVar = np.minimum(minVar, forestC)
    pra = (1 - minVar) * minVar + minVar * (slopeC + windShelterC + forestC) / 3
    pra = pra - ruggC
    pra = np.float32(pra)
    pra[pra <= 0] = 0
    dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, 'pra.tif'),
                         pra, dtype="float32", nodata=demNoData)
    log.info("...continuous PRA - done: %.2fs", time.perf_counter() - t)

    # PRA Thresholding
    t = time.perf_counter()
    pra2d, _ = dataUtils.readRaster(os.path.join(praDelineationDir, 'pra.tif'), return_profile=True)
    praRead = pra2d[np.newaxis, ...]
    if saveAllThresholds:
        for praThreshold in np.arange(0.00, 1.01, 0.01):
            binary = (praRead >= praThreshold).astype('int16')
            fileName = f"pra_binary_th{int(praThreshold*100):03d}.tif"
            dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, fileName),
                                 binary, dtype="int16", nodata=-9999)
        log.info("...save all binary PRA thresholds - done")
    else:
        praThreshold = singleThreshold
        binary = (praRead >= praThreshold).astype('int16')
        fileName = f"pra_binary_th{int(praThreshold*100):03d}.tif"
        dataUtils.saveRaster(demPath, os.path.join(praDelineationDir, fileName),
                             binary, dtype="int16", nodata=-9999)
        log.info("...save single binary PRA %.2f threshold - done", praThreshold)

    log.info("...PRA delineation - done: %.2fs", time.perf_counter() - tAll)
