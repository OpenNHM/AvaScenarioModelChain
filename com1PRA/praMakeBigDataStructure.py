# praMakeBigDataStructure.py

import os
import re
import glob
import time
import shutil
import logging
import contextlib
from typing import Optional


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

def _subfolderName(streamThreshold, minLength, smoothingWindowSize, sizeFilter):
    return f"BnCh2_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}"

def _discoverInputFolder(cfg, workFlowDir, usePraBoundary, streamThreshold, minLength, smoothingWindowSize, sizeFilter):
    """
    Return the Step-07 output folder that contains per-(band,size) files.
    NOTE: Boundary rasters (-praBound) live in the SAME folder, only filenames differ.
    """
    cairosDir = workFlowDir["cairosDir"]
    praPrepForFlowPyDir = workFlowDir.get("praPrepForFlowPyDir") or os.path.join(cairosDir, "08_praPrepForFlowPy")
    subName = _subfolderName(streamThreshold, minLength, smoothingWindowSize, sizeFilter)
    return os.path.join(praPrepForFlowPyDir, subName)

def _ensureOutputRoot(cfg, workFlowDir, streamThreshold, minLength, smoothingWindowSize, sizeFilter, usePraBoundary):
    cairosDir = workFlowDir["cairosDir"]
    bigDataRoot = workFlowDir.get("praMakeBigDataStructureDir") or os.path.join(cairosDir, "09_flowPyBigDataStructure")
    os.makedirs(bigDataRoot, exist_ok=True)

    base = _subfolderName(streamThreshold, minLength, smoothingWindowSize, sizeFilter)
    folderName = base + "-praBound" if usePraBoundary else base
    outCaseDir = os.path.join(bigDataRoot, folderName)
    os.makedirs(outCaseDir, exist_ok=True)
    return bigDataRoot, outCaseDir

def _iterTifs(inputFolder):
    return sorted(glob.glob(os.path.join(inputFolder, "**", "*.tif"), recursive=True))


def _extractSizeNumberFromBase(baseName: str) -> Optional[int]:
    """
    Extracts the size class number, which is always the token between
    the 3rd and 4th '-' in filenames like:

      pra030secN-0000-1800-2.geojson
      pra030secN-2000-2200-5-praAreaM.tif
      pra030secN-2200-2400-3-praID.tif

    Returns int or None if not found.
    """
    parts = baseName.split("-")
    if len(parts) >= 4:
        try:
            return int(parts[3])
        except ValueError:
            return None
    return None



def _makeCaseTreeForRaster(outCaseDir, folderBase, maxSize, minSize=2):
    """
    Create:
      outCaseDir/<folderBase>/SizeN/{wet,dry}/Inputs/{REL, RELID, RELJSON}
    """
    caseRoot = os.path.join(outCaseDir, folderBase)
    os.makedirs(caseRoot, exist_ok=True)
    for size in range(minSize, maxSize + 1):
        for flowType in ("wet", "dry"):
            relDir     = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "REL")
            relIdDir   = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "RELID")
            relJsonDir = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "RELJSON")
            os.makedirs(relDir, exist_ok=True)
            os.makedirs(relIdDir, exist_ok=True)
            os.makedirs(relJsonDir, exist_ok=True)
            log.debug("Created: %s, %s, %s", relDir, relIdDir, relJsonDir)
    return caseRoot

def _logDirectoryTree(baseDir, cairosDir, level=logging.INFO):
    baseDir = os.path.abspath(baseDir)
    log.log(level, "Directory tree for ./%s", relPath(baseDir, cairosDir))
    for root, dirs, files in os.walk(baseDir):
        depth = os.path.relpath(root, start=baseDir).count(os.sep)
        indent = "    " * depth
        log.log(level, "%s%s/", indent, os.path.basename(root))
        subIndent = "    " * (depth + 1)
        for f in sorted(files):
            log.log(level, "%s%s", subIndent, f)

# ------------------ Main driver ------------------ #

def runPraMakeBigDataStructure(cfg, workFlowDir):
    """
    Step 08: Build FlowPy input folder structure and copy PRA rasters and GeoJSONs.
    Supports separate min/max size caps for dry and wet avalanches.
    """
    tAll = time.perf_counter()

    # --- Config ---
    streamThreshold     = cfg["praSUBCATCHMENTS"].getint("streamThreshold", fallback=500)
    minLength           = cfg["praSUBCATCHMENTS"].getint("minLength", fallback=100)
    smoothingWindowSize = cfg["praSUBCATCHMENTS"].getint("smoothingWindowSize", fallback=5)
    sizeFilter          = cfg["praSEGMENTATION"].getfloat("sizeFilter", fallback=500.0)

    sect = cfg["praMAKEBIGDATASTRUCTURE"]
    usePraBoundary   = sect.getboolean("usePraBoundary", fallback=False)
    minDrySizeClass  = sect.getint("minDrySizeClass", fallback=2)
    maxDrySizeClass  = sect.getint("maxDrySizeClass", fallback=5)
    minWetSizeClass  = sect.getint("minWetSizeClass", fallback=2)
    maxWetSizeClass  = sect.getint("maxWetSizeClass", fallback=4)
    logDirectoryTree = sect.getboolean("logDirectoryTree", fallback=False)

    # --- Directories ---
    cairosDir = workFlowDir["cairosDir"]
    inputFolder = _discoverInputFolder(cfg, workFlowDir, usePraBoundary,
                                       streamThreshold, minLength, smoothingWindowSize, sizeFilter)
    outputRoot, outCaseDir = _ensureOutputRoot(cfg, workFlowDir,
                                               streamThreshold, minLength, smoothingWindowSize, sizeFilter,
                                               usePraBoundary)

    log.info("...MakeBigData using: in=./%s, out=./%s, streamThr=%s, minLen=%s, smoothWin=%s, sizeF=%s, usePraBoundary=%s",
             relPath(inputFolder, cairosDir), relPath(outCaseDir, cairosDir),
             streamThreshold, minLength, smoothingWindowSize, int(sizeFilter),
             usePraBoundary)

    # --- Collect rasters ---
    allTifs = _iterTifs(inputFolder)
    if not allTifs:
        log.error("No .tif rasters found in ./%s", relPath(inputFolder, cairosDir))
        return

    def _isPraCandidate(path):
        name = os.path.basename(path)
        if not name.startswith("pra"):
            return False
        hasBound = name.endswith("-praBound.tif")
        return hasBound if usePraBoundary else (not hasBound)

    tifs = [t for t in allTifs if _isPraCandidate(t)]
    if not tifs:
        exp = "with -praBound suffix" if usePraBoundary else "without -praBound suffix"
        log.error("No 'pra*.tif' rasters found %s in ./%s", exp, relPath(inputFolder, cairosDir))
        return

    for t in tifs:
        log.debug("Using raster: ./%s", relPath(t, cairosDir))

    # --- Build structure and copy rasters ---
    nFoldersCreated = nCopied = nSkipped = 0

    for tifPath in tifs:
        try:
            with timeIt(f"makeCase({os.path.basename(tifPath)})"):
                fileStem = os.path.splitext(os.path.basename(tifPath))[0]
                folderBase = fileStem

                # strip legacy boundary suffixes
                for suffix in ("-praID-praBound", "-praID", "-praBound"):
                    if folderBase.endswith(suffix):
                        folderBase = folderBase[:-len(suffix)]

                # strip attribute suffixes so all variants share the same folder
                for suffix in ("-praAreaM", "-praAreaSized", "-ElevBands-Sized", "-praID"):
                    if folderBase.endswith(suffix):
                        folderBase = folderBase[: -len(suffix)]

                # extract size number
                sizeNum = _extractSizeNumberFromBase(folderBase)
                if sizeNum is None:
                    nSkipped += 1
                    log.warning("Could not extract size number from '%s'; skipping.", fileStem)
                    continue

                # ensure case root
                caseRoot = os.path.join(outCaseDir, folderBase)
                os.makedirs(caseRoot, exist_ok=True)

                for flowType in ("dry", "wet"):
                    if flowType == "dry":
                        minSize = minDrySizeClass
                        maxSize = min(maxDrySizeClass, sizeNum)
                    else:  # wet
                        minSize = minWetSizeClass
                        maxSize = min(maxWetSizeClass, sizeNum)

                    for size in range(minSize, maxSize + 1):
                        # Create REL/RELID/RELJSON dirs
                        relDir     = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "REL")
                        relIdDir   = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "RELID")
                        relJsonDir = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "RELJSON")
                        os.makedirs(relDir, exist_ok=True)
                        os.makedirs(relIdDir, exist_ok=True)
                        os.makedirs(relJsonDir, exist_ok=True)
                        nFoldersCreated += 1

                        # Copy PRA raster
                        if fileStem.endswith("-praID") or "-praID" in fileStem:
                            dstPath = os.path.join(relIdDir, os.path.basename(tifPath))
                        else:
                            dstPath = os.path.join(relDir, os.path.basename(tifPath))

                        try:
                            shutil.copy2(tifPath, dstPath)
                            nCopied += 1
                            log.debug("Copied: ./%s -> ./%s",
                                      relPath(tifPath, cairosDir), relPath(dstPath, cairosDir))
                        except Exception:
                            log.exception("Copy failed to ./%s", relPath(relDir, cairosDir))

                        # Copy matching GeoJSON (if exists)
                        geoBase = folderBase + ".geojson"
                        geojsonSearch = os.path.join(inputFolder, geoBase)
                        if os.path.exists(geojsonSearch):
                            dstJson = os.path.join(relJsonDir, os.path.basename(geojsonSearch))
                            try:
                                shutil.copy2(geojsonSearch, dstJson)
                                log.debug("Copied GeoJSON: ./%s -> ./%s",
                                          relPath(geojsonSearch, cairosDir), relPath(dstJson, cairosDir))
                            except Exception:
                                log.exception("Copy failed for GeoJSON to ./%s", relPath(relJsonDir, cairosDir))
                        else:
                            log.debug("No GeoJSON found for base=%s", folderBase)

        except Exception:
            log.exception("Case creation failed for ./%s", relPath(tifPath, cairosDir))

    if logDirectoryTree:
        _logDirectoryTree(outCaseDir, cairosDir)

    log.info("...MakeBigData stats: cases=%d, rasters_copied=%d, skipped=%d",
             nFoldersCreated, nCopied, nSkipped)
    log.info("...MakeBigData - done: %.2fs", time.perf_counter() - tAll)
