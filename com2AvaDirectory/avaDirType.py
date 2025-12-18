# ------------ Step 14: AvaDirectory Type ------------------------------- #
#
# Purpose :
#     Combine all per-scenario com4_* directories produced in Step 13 into a
#     unified AvaDirectoryType dataset. Each PRA–scenario combination is
#     consolidated into a single feature containing geometry, run metadata,
#     and key simulation descriptors, enabling consistent scenario-level
#     classification and filtering.
#
# Inputs :
#     - 11_avaDirectoryData/<caseFolder>/com4_*/praID*.geojson
#
# Outputs :
#     - 12_avaDirectory/<caseFolder>/avaDirectoryType.csv
#     - 12_avaDirectory/<caseFolder>/avaDirectoryType.geojson
#     - 12_avaDirectory/<caseFolder>/avaDirectoryType.parquet
#       (format depends on configuration)
#
# Config :
#     [avaDIRECTORY]
#     [WORKFLOW]
#
# Consumes :
#     - Step 13 outputs (AvaDirectory Build From FlowPy)
#
# Provides :
#     - Unified AvaDirectoryType dataset for:
#         • Step 15 (AvaDirectory Results)
#         • Scenario selection, enrichment, and classification workflows
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
import logging
from pathlib import Path

import pandas as pd
import geopandas as gpd

try:
    import pyogrio
    _HAS_PYOGRIO = True
except Exception:
    _HAS_PYOGRIO = False

from in1Utils.dataUtils import relPath
import in1Utils.workflowUtils as workflowUtils

import sys
from functools import partial
from tqdm import tqdm as _tqdm

tqdm = partial(
    _tqdm,
    ncols=60,                
    dynamic_ncols=False,    
    leave=False,
    mininterval=0.2,
    ascii=True,             
    bar_format="{l_bar}{bar:15}{r_bar}",  
    file=sys.stderr,          
)


log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.WARNING)


# ------------------ Helpers ------------------ #
def _read_gdf(path: Path):
    if path.suffix == ".parquet":
        return gpd.read_parquet(path)
    return pyogrio.read_dataframe(path) if _HAS_PYOGRIO else gpd.read_file(path)


def _write_gdf(gdf, path: Path, driver="GeoJSON"):
    if _HAS_PYOGRIO:
        pyogrio.write_dataframe(gdf, path, driver=driver)
    else:
        gdf.to_file(path, driver=driver)


# ------------------ Entry Point ------------------ #
def runAvaDirType(cfg, workFlowDir):
    """Step 14: Merge scenario outputs into unified AvaDirectoryType dataset."""
    log.info("Step 14: Start AvaDirectory Type build...")

    wf = cfg["WORKFLOW"]
    main = cfg["MAIN"]
    avaCfg = cfg["avaDIRECTORY"]

    # --- Resolve directories dynamically ---
    caseFolder = workflowUtils.caseFolderName(cfg)
    rootDir = Path(main["workDir"]) / main["project"] / main["ID"]
    cairosDir = Path(workFlowDir["cairosDir"])

    avaDirLib = rootDir / "12_avaDirectory" / caseFolder
    avaDirLib.mkdir(parents=True, exist_ok=True)

    # --- Mode flags ---
    readSingleAvaGeoJSON = avaCfg.getboolean("readSingleAvaGeoJSON", True)
    readScenarioParquet  = avaCfg.getboolean("readScenarioParquet", False)

    # Output flags
    writeCsv     = avaCfg.getboolean("writeTypeCsv", True)
    writeGeoJSON = avaCfg.getboolean("writeTypeGeoJSON", True)
    writeParquet = avaCfg.getboolean("writeTypeParquet", True)

    log.info(
        "Step 14: Input mode → readSingleAvaGeoJSON=%s, readScenarioParquet=%s",
        readSingleAvaGeoJSON, readScenarioParquet
    )
    log.info(
        "Step 14: Output flags → CSV=%s, GeoJSON=%s, Parquet=%s",
        writeCsv, writeGeoJSON, writeParquet
    )

    # --- Single-test mode info ---
    if wf.getboolean("makeSingleTestRun", False):
        singleDir = wf.get("singleTestDir", "").strip()
        log.info("Step 14: Single-test mode active (singleTestDir=%s)", singleDir or "<not set>")

    # ------------------ Collect input files ------------------ #
    inputFiles = []

    if readScenarioParquet:
        # Scenario parquet files are written by Step 13 into FlowPy folder tree:
        # 09_flowPyBigDataStructure/<caseFolder>/pra*/Size*/{dry,wet}/Map/singleAvaDir/com4_*/avaScenario.parquet
        flowPyRoot = rootDir / "09_flowPyBigDataStructure" / caseFolder
        if not flowPyRoot.exists():
            log.error("Step 14: FlowPy root missing: %s", relPath(flowPyRoot, cairosDir))
            return

        pattern = str(flowPyRoot / "pra*" / "Size*" / "*" / "Map" / "singleAvaDir" / "com4_*" / "avaScenLeaf_com4_*.parquet")
        inputFiles = sorted(glob.glob(pattern))
        log.info("Step 14: Found %d scenario parquet files", len(inputFiles))

    elif readSingleAvaGeoJSON:
        # Legacy mode: read from 11_avaDirectoryData/<caseFolder>/com4_*/praID*.geojson
        avaDirData = rootDir / "11_avaDirectoryData" / caseFolder
        if not avaDirData.exists():
            log.warning("Step 14: Expected AvaDirectoryData missing: %s", relPath(avaDirData, cairosDir))
            return

        com4Folders = sorted(glob.glob(str(avaDirData / "com4_*")))
        if not com4Folders:
            log.warning("Step 14: No com4_* folders found in %s", relPath(avaDirData, cairosDir))
            return

        for com4Dir in com4Folders:
            inputFiles.extend(glob.glob(os.path.join(com4Dir, "praID*.geojson")))

        log.info("Step 14: Found %d legacy praID*.geojson files", len(inputFiles))

    else:
        log.error("Step 14: No input mode enabled. Set readScenarioParquet=True or readSingleAvaGeoJSON=True.")
        return

    if not inputFiles:
        log.warning("Step 14: No input files found → nothing to merge.")
        return

    # ------------------ Read & merge ------------------ #
    allChunks = []
    for fp in tqdm(
        inputFiles,
        desc="Step 14: Merge leaves",
        unit="file",
    ):

        try:
            gdf = _read_gdf(Path(fp))
            allChunks.append(gdf)
        except Exception:
            log.exception("Step 14: Failed to read %s", relPath(Path(fp), cairosDir))

    if not allChunks:
        log.warning("Step 14: All reads failed → no output created.")
        return

    merged = pd.concat(allChunks, ignore_index=True)
    log.info("Step 14: Merged %d rows from %d files", len(merged), len(allChunks))

    # --- Drop legacy columns ---
    for col in ["resId", "PRA_id", "Sector"]:
        if col in merged.columns:
            merged = merged.drop(columns=[col])

    # --- Normalize flow values ---
    if "flow" in merged.columns:
        merged["flow"] = merged["flow"].replace({"Dry": "dry", "Wet": "wet"}).astype("string")

    # --- Convert numeric columns ---
    intCols = ["praID", "praAreaSized", "LKGebietID", "subC", "elevMin", "elevMax", "ppm", "pem", "rSize"]
    for col in intCols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("Int64")

    # --- De-duplicate (preserve rel/res distinction) ---
    dedupCols = [c for c in ["praID", "resultID", "flow", "modType"] if c in merged.columns]
    if dedupCols:
        before = len(merged)
        merged = merged.drop_duplicates(subset=dedupCols, keep="first").reset_index(drop=True)
        log.info("Step 14: Removed %d duplicates based on %s", before - len(merged), dedupCols)

    # ------------------ Write outputs ------------------ #
    csvPath = avaDirLib / "avaDirectoryType.csv"
    geojsonPath = avaDirLib / "avaDirectoryType.geojson"
    parquetPath = avaDirLib / "avaDirectoryType.parquet"

    if writeCsv:
        try:
            merged.drop(columns="geometry", errors="ignore").to_csv(csvPath, index=False)
            log.info("Step 14: Wrote CSV to %s", relPath(csvPath, cairosDir))
        except Exception as e:
            log.warning("Step 14: CSV write warning: %s", e)

    if writeGeoJSON:
        try:
            _write_gdf(merged, geojsonPath)
            log.info("Step 14: Wrote GeoJSON to %s", relPath(geojsonPath, cairosDir))
        except Exception as e:
            log.warning("Step 14: GeoJSON write warning: %s", e)

    if writeParquet:
        try:
            merged.to_parquet(parquetPath, index=False)
            log.info("Step 14: Wrote Parquet to %s", relPath(parquetPath, cairosDir))
        except Exception as e:
            log.warning("Step 14: Parquet write warning: %s", e)

    log.info("Step 14: AvaDirectoryType build complete.")
    return avaDirLib
