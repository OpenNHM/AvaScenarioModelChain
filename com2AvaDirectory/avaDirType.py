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

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.WARNING)


# ------------------ Helpers ------------------ #
def _read_gdf(path):
    return pyogrio.read_dataframe(path) if _HAS_PYOGRIO else gpd.read_file(path)

def _write_gdf(gdf, path, driver="GeoJSON"):
    if _HAS_PYOGRIO:
        pyogrio.write_dataframe(gdf, path, driver=driver)
    else:
        gdf.to_file(path, driver=driver)


# ------------------ Entry Point ------------------ #
def runAvaDirType(cfg, workFlowDir):
    """Step 14: Merge all com4_* folders into unified AvaDirectoryType dataset."""
    log.info("Step 14: Start AvaDirectory Type build...")

    wf = cfg["WORKFLOW"]
    main = cfg["MAIN"]

    # --- Resolve directories dynamically ---
    caseFolder = workflowUtils.caseFolderName(cfg)
    rootDir = Path(main["workDir"]) / main["project"] / main["ID"]
    avaDirData = rootDir / "11_avaDirectoryData" / caseFolder
    avaDirLib  = rootDir / "12_avaDirectory" / caseFolder
    cairosDir  = Path(workFlowDir["cairosDir"])

    if not avaDirData.exists():
        log.warning("Step 14: Expected AvaDirectoryData missing: %s", relPath(avaDirData, cairosDir))
        return

    avaDirLib.mkdir(parents=True, exist_ok=True)
    log.info("Step 14: Using AvaDirectoryData=%s", relPath(avaDirData, cairosDir))
    log.info("Step 14: Writing outputs to AvaDirectory=%s", relPath(avaDirLib, cairosDir))

    # --- Single-test mode info ---
    if wf.getboolean("makeSingleTestRun", False):
        singleDir = wf.get("singleTestDir", "").strip()
        log.info("Step 14: Single-test mode active (singleTestDir=%s)", singleDir or "<not set>")

    # --- Discover all com4_* folders ---
    com4Folders = sorted(glob.glob(str(avaDirData / "com4_*")))
    if not com4Folders:
        log.warning("Step 14: No com4_* folders found in %s", relPath(avaDirData, cairosDir))
        return
    log.info("Step 14: Found %d com4_* folders", len(com4Folders))

    # --- Read all praID*.geojson and merge ---
    all_chunks, total_files = [], 0
    for com4Dir in com4Folders:
        praFiles = glob.glob(os.path.join(com4Dir, "praID*.geojson"))
        if not praFiles:
            continue
        for pf in praFiles:
            try:
                gdf = _read_gdf(pf)
                all_chunks.append(gdf)
                total_files += 1
            except Exception:
                log.exception("Step 14: Failed to read %s", relPath(pf, cairosDir))

    if not all_chunks:
        log.warning("Step 14: No praID*.geojson files found under %s", relPath(avaDirData, cairosDir))
        return

    merged = pd.concat(all_chunks, ignore_index=True)
    log.info("Step 14: Merged %d rows from %d files", len(merged), total_files)

    # --- Drop legacy columns ---
    for col in ["resId", "PRA_id", "Sector"]:
        if col in merged.columns:
            merged = merged.drop(columns=[col])

    # --- Normalize flow values ---
    if "flow" in merged.columns:
        merged["flow"] = merged["flow"].replace({"Dry": "dry", "Wet": "wet"}).astype("string")

    # --- Convert numeric columns ---
    int_cols = [
        "praID", "praAreaSized", "LKGebietID", "subC",
        "elevMin", "elevMax", "ppm", "pem", "rSize"
    ]
    for col in int_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("Int64")

    # --- De-duplicate (preserve rel/res distinction) ---
    dedupCols = [c for c in ["praID", "resultID", "flow", "modType"] if c in merged.columns]
    if dedupCols:
        before = len(merged)
        merged = merged.drop_duplicates(subset=dedupCols, keep="first").reset_index(drop=True)
        log.info("Step 14: Removed %d duplicates based on %s", before - len(merged), dedupCols)

    # --- Write outputs to AvaDirectory (library) ---
    csvPath = avaDirLib / "avaDirectoryType.csv"
    geojsonPath = avaDirLib / "avaDirectoryType.geojson"
    parquetPath = avaDirLib / "avaDirectoryType.parquet"

    merged.drop(columns="geometry", errors="ignore").to_csv(csvPath, index=False)
    try:
        _write_gdf(merged, geojsonPath)
    except Exception as e:
        log.warning("Step 14: GeoJSON write warning: %s", e)
    try:
        merged.to_parquet(parquetPath, index=False)
    except Exception as e:
        log.warning("Step 14: Parquet write warning: %s", e)

    log.info(
        "Step 14: Wrote %d features to %s (CSV, GeoJSON, Parquet)",
        len(merged), relPath(avaDirLib, cairosDir)
    )
    log.info("Step 14: AvaDirectoryType build complete.")
    return avaDirLib
