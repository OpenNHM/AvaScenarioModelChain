# ------------------ Step 08: Make Big Data Structure ------------------- #
#
# Purpose :
#     Build the FlowPy Big Data directory structure by aggregating and arranging
#     all per-PRA, per-size, and per-elevation-band inputs produced in Step 07.
#     The output follows the standardized AvaFrame/FlowPy folder hierarchy:
#
#         SizeN/
#             dry|wet/
#                 Inputs/
#                     REL/
#                     RELID/
#                     RELJSON/
#
# Inputs :
#     - PRA rasters and GeoJSONs prepared in Step 07
#       (./08_praPrepForFlowPy/*.tif / *.geojson)
#
# Outputs :
#     - Fully structured FlowPy Big Data directory:
#         ./09_flowPyBigDataStructure/
#             pra<ID>-<elevRange>-<sizeClass>/SizeN/{dry,wet}/Inputs/{REL,RELID,RELJSON}/
#
# Config :
#     [praMAKEBIGDATASTRUCTURE]
#         • usePraBoundary
#         • min/max size class per scenario (dry / wet)
#         • logging settings
#
# Consumes :
#     - Step 07 outputs (PRA → FlowPy preparation)
#
# Provides :
#     - FlowPy-ready directory tree for Step 09 (parameterization) and Step 10 (FlowPy run)
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
import re
import glob
import time
import shutil
import logging
from typing import Optional

import ati.mod0Helper.dataUtils as dataUtils

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)

def _discoverInputFolder(workFlowDir) -> str:
    """Return the flat Step-07 output directory containing per-(band,size) files."""
    cairosDir = workFlowDir["cairosDir"]
    return workFlowDir.get("praPrepForFlowPyDir") or os.path.join(
        cairosDir, "08_praPrepForFlowPy"
    )


def _ensureOutputRoot(workFlowDir):
    """Ensure and return the flat Step-08 output root."""
    cairosDir = workFlowDir["cairosDir"]
    bigDataRoot = workFlowDir.get("praMakeBigDataStructureDir") or os.path.join(
        cairosDir, "09_flowPyBigDataStructure"
    )
    os.makedirs(bigDataRoot, exist_ok=True)
    return bigDataRoot


def _iterTifs(inputFolder):
    """List direct Step-07 TIFF outputs, ignoring obsolete nested layouts."""
    return sorted(glob.glob(os.path.join(inputFolder, "*.tif")))


def _extractSizeNumberFromBase(baseName: str) -> Optional[int]:
    """Extract size class number (4th token in praXXX-YYYY-ZZZZ-N filenames)."""
    parts = baseName.split("-")
    if len(parts) >= 4:
        try:
            return int(parts[3])
        except ValueError:
            return None
    return None


def _logDirectoryTree(baseDir, cairosDir, level=logging.INFO):
    """Optional full directory tree logger."""
    baseDir = os.path.abspath(baseDir)
    log.log(level, "Directory tree for ./%s", dataUtils.relPath(baseDir, cairosDir))
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
    streamThreshold = cfg["praSUBCATCHMENTS"].getint("streamThreshold", fallback=500)
    minLength = cfg["praSUBCATCHMENTS"].getint("minLength", fallback=100)
    smoothingWindowSize = cfg["praSUBCATCHMENTS"].getint("smoothingWindowSize", fallback=5)
    sizeFilter = cfg["praSEGMENTATION"].getfloat("sizeFilter", fallback=500.0)

    sect = cfg["praMAKEBIGDATASTRUCTURE"]
    usePraBoundary = sect.getboolean("usePraBoundary", fallback=False)
    minDrySizeClass = sect.getint("minDrySizeClass", fallback=2)
    maxDrySizeClass = sect.getint("maxDrySizeClass", fallback=5)
    minWetSizeClass = sect.getint("minWetSizeClass", fallback=2)
    maxWetSizeClass = sect.getint("maxWetSizeClass", fallback=4)
    logDirectoryTree = sect.getboolean("logDirectoryTree", fallback=False)

    # --- Directories ---
    cairosDir = workFlowDir["cairosDir"]
    inputFolder = _discoverInputFolder(workFlowDir)
    outCaseDir = _ensureOutputRoot(workFlowDir)

    log.info(
        "...MakeBigData using: in=./%s, out=./%s, streamThr=%s, minLen=%s, smoothWin=%s, sizeF=%s, usePraBoundary=%s",
        dataUtils.relPath(inputFolder, cairosDir),
        dataUtils.relPath(outCaseDir, cairosDir),
        streamThreshold,
        minLength,
        smoothingWindowSize,
        int(sizeFilter),
        usePraBoundary,
    )

    # --- Collect rasters ---
    allTifs = _iterTifs(inputFolder)
    if not allTifs:
        log.error("No .tif rasters found in ./%s", dataUtils.relPath(inputFolder, cairosDir))
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
        log.error(
            "No 'pra*.tif' rasters found %s in ./%s",
            exp,
            dataUtils.relPath(inputFolder, cairosDir),
        )
        return

    for t in tifs:
        log.debug("Using raster: ./%s", dataUtils.relPath(t, cairosDir))

    # --- Build structure and copy rasters ---
    nFoldersCreated = nCopied = nSkipped = 0

    for tifPath in tifs:
        try:
            with dataUtils.timeIt(f"makeCase({os.path.basename(tifPath)})"):
                fileStem = os.path.splitext(os.path.basename(tifPath))[0]

                # --- clean folderBase robustly (remove technical suffixes) ---
                folderBase = re.sub(r"-ElevBands-Sized", "", fileStem)
                folderBase = re.sub(r"-(pra(ID|AreaM|AreaSized|Bound)).*$", "", folderBase)
                log.debug("Scenario folder base parsed: %s -> %s", fileStem, folderBase)

                # --- extract size number ---
                sizeNum = _extractSizeNumberFromBase(folderBase)
                if sizeNum is None:
                    nSkipped += 1
                    log.warning("Could not extract size number from '%s'; skipping.", fileStem)
                    continue

                # --- ensure case root ---
                caseRoot = os.path.join(outCaseDir, folderBase)
                os.makedirs(caseRoot, exist_ok=True)

                # --- per-flowType + size subtrees ---
                for flowType in ("dry", "wet"):
                    if flowType == "dry":
                        minSize = minDrySizeClass
                        maxSize = min(maxDrySizeClass, sizeNum)
                    else:
                        minSize = minWetSizeClass
                        maxSize = min(maxWetSizeClass, sizeNum)

                    for size in range(minSize, maxSize + 1):
                        relDir = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "REL")
                        relAreaDir = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "RELArea")
                        relIdDir = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "RELID")
                        relJsonDir = os.path.join(caseRoot, f"Size{size}", flowType, "Inputs", "RELJSON")
                        os.makedirs(relDir, exist_ok=True)
                        os.makedirs(relIdDir, exist_ok=True)
                        os.makedirs(relJsonDir, exist_ok=True)
                        os.makedirs(relAreaDir, exist_ok=True)
                        nFoldersCreated += 1

                        # --- Copy PRA raster ---
                        if fileStem.endswith("-praID") or "-praID" in fileStem:
                            dstPath = os.path.join(relIdDir, os.path.basename(tifPath))
                        else:
                            dstPath = os.path.join(relDir, os.path.basename(tifPath))

                        try:
                            shutil.copy2(tifPath, dstPath)
                            nCopied += 1
                            log.debug(
                                "Copied: ./%s -> ./%s",
                                dataUtils.relPath(tifPath, cairosDir),
                                dataUtils.relPath(dstPath, cairosDir),
                            )
                        except Exception:
                            log.exception("Copy failed to ./%s", dataUtils.relPath(relDir, cairosDir))

                        # --- Copy matching GeoJSON (if exists) ---
                        geoBase = folderBase + ".geojson"
                        geojsonSearch = os.path.join(inputFolder, geoBase)
                        if os.path.exists(geojsonSearch):
                            dstJson = os.path.join(relJsonDir, os.path.basename(geojsonSearch))
                            try:
                                shutil.copy2(geojsonSearch, dstJson)
                                log.debug(
                                    "Copied GeoJSON: ./%s -> ./%s",
                                    dataUtils.relPath(geojsonSearch, cairosDir),
                                    dataUtils.relPath(dstJson, cairosDir),
                                )
                            except Exception:
                                log.exception(
                                    "Copy failed for GeoJSON to ./%s",
                                    dataUtils.relPath(relJsonDir, cairosDir),
                                )
                        else:
                            log.debug("No GeoJSON found for base=%s", folderBase)

                        if fileStem.endswith("praAreaM") or "praAreaM" in fileStem:
                            dstPath = os.path.join(relAreaDir, os.path.basename(tifPath))
                        else:
                            dstPath = os.path.join(relAreaDir, os.path.basename(tifPath))

                        try:
                            shutil.copy2(tifPath, dstPath)
                            nCopied += 1
                            log.debug(
                                "Copied: ./%s -> ./%s",
                                dataUtils.relPath(tifPath, cairosDir),
                                dataUtils.relPath(dstPath, cairosDir),
                            )
                        except Exception:
                            log.exception(
                                "Copy failed to ./%s", dataUtils.relPath(relAreaDir, cairosDir)
                            )

        except Exception:
            log.exception("Case creation failed for ./%s", dataUtils.relPath(tifPath, cairosDir))

    # --- optional directory tree log ---
    if logDirectoryTree:
        _logDirectoryTree(outCaseDir, cairosDir)

    log.info(
        "...MakeBigData stats: cases=%d, rasters_copied=%d, skipped=%d", nFoldersCreated, nCopied, nSkipped
    )
    log.info("...MakeBigData - done: %.2fs", time.perf_counter() - tAll)
