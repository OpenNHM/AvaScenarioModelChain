# ------------- workflowUtils.py ---------------------------------------- #
#
# Purpose :
#     Provide shared workflow helper functions used across all modules of the
#     Avalanche Scenario Model Chain. This includes unified step control,
#     directory discovery, input validation, logging management, timing helpers,
#     and FlowPy-specific execution wrappers.
#
# Inputs :
#     - Configuration parser (ConfigParser)
#     - workFlowDir dictionary containing all step directories
#
# Outputs :
#     - Utility functions for:
#         • step activation logic
#         • consistent logging and timing
#         • FlowPy leaf discovery and filtering
#         • input presence and CRS validation
#         • safe execution wrappers for FlowPy
#
# Config :
#     [WORKFLOW]
#     [MAIN]
#     [avaPARAMETER]
#     [praMAKEBIGDATASTRUCTURE]
#
# Consumes :
#     - None directly (utility layer)
#
# Provides :
#     - Core orchestration tools used by:
#         • runAvaScenModelChain.py (master driver)
#         • All PRA preprocessing steps (01–08)
#         • FlowPy integration steps (09–12)
#         • AvaDirectory construction steps (13–15)
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


from __future__ import annotations
from typing import Optional
import pathlib
import logging
import os
import time
from contextlib import contextmanager
from logging.handlers import MemoryHandler

log = logging.getLogger(__name__)

# ------------------ Step control helpers ------------------ #

def stepEnabled(flags, key: str, master: bool = False, default: bool = False) -> bool:
    """Master flag overrides per-step flags; else read from [WORKFLOW]."""
    return True if master else flags.getboolean(key, fallback=default)


def caseFolderName(cfg) -> str:
    """Matches Step-08/09 naming convention, including optional '-praBound'."""
    sub = cfg["praSUBCATCHMENTS"] if "praSUBCATCHMENTS" in cfg else {}
    seg = cfg["praSEGMENTATION"] if "praSEGMENTATION" in cfg else {}
    mk  = cfg["praMAKEBIGDATASTRUCTURE"] if "praMAKEBIGDATASTRUCTURE" in cfg else {}

    streamThreshold     = int(sub.get("streamThreshold", 500))
    minLength           = int(sub.get("minLength", 100))
    smoothingWindowSize = int(sub.get("smoothingWindowSize", 5))
    sizeFilter          = float(seg.get("sizeFilter", 500.0))
    usePraBoundary      = str(mk.get("usePraBoundary", "False")).lower() in ("1","true","yes")

    base = f"BnCh2_subC{streamThreshold}_{minLength}_{smoothingWindowSize}_sizeF{int(sizeFilter)}"
    return base + "-praBound" if usePraBoundary else base


def parseFlowTypes(val: str) -> list[str]:
    """Parse comma-separated flow type list."""
    v = (val or "").strip()
    items = [s.strip() for s in v.split(",") if s.strip()]
    return items or ["dry"]


def parseSizeRange(val: str) -> list[int]:
    """Parse size range like '2-5' or CSV list '2,3,5'."""
    v = (val or "").strip()
    if "-" in v:
        a, b = v.split("-", 1)
        lo, hi = int(a.strip()), int(b.strip())
        if lo > hi:
            lo, hi = hi, lo
        return list(range(lo, hi + 1))
    parts = [p.strip() for p in v.split(",") if p.strip()]
    return [int(p) for p in parts] if parts else [2, 3, 4, 5]


def demForLeaf(use_big_data: bool, input_dir: pathlib.Path, dem_name: str) -> Optional[str]:
    """Return absolute DEM path if using BigData; else None to use <Inputs>/DEM.tif."""
    if use_big_data:
        p = pathlib.Path(input_dir) / dem_name
        return str(p)
    return None


# ------------------ AvaFrame leaf discovery ------------------ #

def discoverAvaDirs(cfg, workFlowDir):
    """
    Discover available AvaFrame case leaf directories (SizeN/dry|wet),
    respecting [avaPARAMETER] (flowTypes, sizeRange) and
    [praMAKEBIGDATASTRUCTURE] (per-flow-type min/max).
    """
    avaDirs = []

    avaParams = cfg["avaPARAMETER"] if "avaPARAMETER" in cfg else cfg["MAIN"]
    flowTypes = parseFlowTypes(avaParams.get("flowTypes", "dry"))
    sizeList  = parseSizeRange(avaParams.get("sizeRange", "2-5"))

    sect = cfg["praMAKEBIGDATASTRUCTURE"] if "praMAKEBIGDATASTRUCTURE" in cfg else {}
    minDry = int(sect.get("minDrySizeClass", 2))
    maxDry = int(sect.get("maxDrySizeClass", 5))
    minWet = int(sect.get("minWetSizeClass", 2))
    maxWet = int(sect.get("maxWetSizeClass", 5))

    parentCase = caseFolderName(cfg)
    rootPath = pathlib.Path(workFlowDir["flowPyRunDir"]) / parentCase

    if rootPath.exists():
        for case in sorted(p for p in rootPath.iterdir() if p.is_dir()):
            for N in sizeList:
                for scen in flowTypes:
                    scen_lower = scen.lower()

                    # enforce per-flow-type min/max
                    if scen_lower == "dry" and not (minDry <= N <= maxDry):
                        continue
                    if scen_lower == "wet" and not (minWet <= N <= maxWet):
                        continue

                    cand = case / f"Size{N}" / scen_lower
                    if cand.is_dir():
                        avaDirs.append(cand)
                        log.info(
                            "Discovered leaf: ./%s (size=%d, scen=%s)",
                            os.path.relpath(str(cand), start=workFlowDir["cairosDir"]),
                            N, scen_lower,
                        )

    if not avaDirs:
        log.warning(
            "discoverAvaDirs: No valid leaves found for flowTypes=%s, sizeRange=%s",
            flowTypes, sizeList,
        )

    return avaDirs


# ------------------ General logging & runtime helpers ------------------ #

def closeEarlyBuffer(buf: MemoryHandler, root_logger: logging.Logger) -> None:
    """Safely close and remove a MemoryHandler used for early buffered logs."""
    try:
        root_logger.removeHandler(buf)
        buf.close()
    except Exception:
        pass


def filterSingleTestDirs(cfg, dirs: list[pathlib.Path], stepLabel: str) -> list[pathlib.Path]:
    """Restrict FlowPy or AvaDirectory leaves to a single directory if makeSingleTestRun=True."""
    if not dirs:
        return dirs

    wf = cfg["WORKFLOW"]
    if not wf.getboolean("makeSingleTestRun", fallback=False):
        return dirs

    singleDir = wf.get("singleTestDir", "").strip()
    if not singleDir:
        log.warning("%s: makeSingleTestRun=True but no singleTestDir specified.", stepLabel)
        return dirs

    filtered = [d for d in dirs if singleDir in {d.name, d.parent.name, d.parent.parent.name}]
    if not filtered:
        log.warning("%s: singleTestDir '%s' not found among discovered leaves.", stepLabel, singleDir)
        return []

    log.info("%s: Single test mode → %s (%d leaves)", stepLabel, singleDir, len(filtered))
    return filtered


@contextmanager
def stepTimer(label: str):
    """Context manager for timing a workflow step with unified logging."""
    t0 = time.perf_counter()
    log.info("Start %s...", label)
    try:
        yield
        log.info("Finish %s in %.2fs", label, time.perf_counter() - t0)
    except Exception:
        log.exception("%s failed.", label)
        raise


def validateInputs(cfg, workFlowDir):
    """Check existence and validity of DEM, FOREST, and BOUNDARY inputs."""
    import pathlib
    from in1Utils import dataUtils

    inputDir = pathlib.Path(workFlowDir["inputDir"])
    dem = cfg["MAIN"].get("DEM", "").strip()
    forest = cfg["MAIN"].get("FOREST", "").strip()
    boundary = cfg["MAIN"].get("BOUNDARY", "").strip()

    missing = []
    for label, fname in (("DEM", dem), ("FOREST", forest), ("BOUNDARY", boundary)):
        if not fname:
            missing.append(f"{label}=<empty in INI>")
        else:
            fpath = inputDir / fname
            if not fpath.exists():
                missing.append(f"{label}={fname}")

    if missing:
        log.error("Step 00: Required input files are missing in ./%s:",
                  os.path.relpath(str(inputDir), start=workFlowDir["cairosDir"]))
        for m in missing:
            log.error("  - %s", m)
        log.error("\n\n          ... Please provide the required input files and run again ...\n")
        return False

    # --- Validate rasters (DEM, FOREST) ---
    for label, fname in (("DEM", dem), ("FOREST", forest)):
        if not fname:
            continue
        fpath = inputDir / fname
        try:
            dataUtils.enforceNumericNoData(fpath, fallback=-9999.0, force_epsg=25832)
            log.info("Step 00: Input %s validated: nodata + CRS check done.", label)
        except Exception:
            log.exception("Step 00: Failed to normalize %s: %s", label, fpath)
            return False

    log.info("Step 00: All raster inputs validated: DEM + FOREST nodata/CRS checked and safe.")
    return True



def runStep(stepKey: str, stepLabel: str, func, cfg, workFlowDir, stepStats, wf, masterFlag) -> bool:
    """Generic step runner with flag control, timing, and unified logging."""
    if not stepEnabled(wf, stepKey, masterFlag, default=False):
        log.info("Step %s: ...%s skipped (flag is False)", stepKey, stepLabel)
        return True

    t0 = time.perf_counter()
    log.info("Step %s: Start %s...", stepKey, stepLabel)
    try:
        func(cfg, workFlowDir)
        stepStats[f"Step {stepKey}"] = time.perf_counter() - t0
        log.info("Step %s: Finish %s in %.2fs", stepKey, stepLabel, stepStats[f"Step {stepKey}"])
        return True
    except Exception:
        log.exception("Step %s: %s failed.", stepKey, stepLabel)
        return False





# ------------------ FlowPy-specific helpers ------------------ #

def discoverAndFilterAvaDirs(cfg, workFlowDir, stepLabel="Step 10"):
    """
    Discover and filter FlowPy/AvaFrame leaves for processing steps.
    Combines discoverAvaDirs() + filterSingleTestDirs() with unified logging.
    """
    avaDirs = discoverAvaDirs(cfg, workFlowDir)
    avaDirs = filterSingleTestDirs(cfg, avaDirs, stepLabel)
    if not avaDirs:
        log.error("%s: No valid FlowPy directories found.", stepLabel)
    return avaDirs


@contextmanager
def preserveLoggingForFlowPy():
    """
    Context manager to safely execute FlowPy while preserving CAIROS log handlers.

    FlowPy resets or overrides the root logger configuration, which can
    suppress subsequent CAIROS logs from appearing on stdout.
    This helper temporarily clones the main FileHandler for FlowPy output
    and restores the original logging state after completion.
    """
    root_logger = logging.getLogger()
    handlers_backup = list(root_logger.handlers)
    file_handlers = [h for h in handlers_backup if isinstance(h, logging.FileHandler)]
    flowpy_handler = None

    try:
        # Attach a temporary log handler so FlowPy output is mirrored to CAIROS log
        if file_handlers:
            fh = file_handlers[0]
            flowpy_handler = logging.FileHandler(fh.baseFilename, mode="a", encoding="utf-8")
            flowpy_handler.setLevel(logging.INFO)
            flowpy_handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            )
            root_logger.addHandler(flowpy_handler)

        yield  # --- run FlowPy inside this context ---

    finally:
        # Restore pre-existing handlers
        if flowpy_handler:
            root_logger.removeHandler(flowpy_handler)
            flowpy_handler.close()
        root_logger.handlers = handlers_backup
