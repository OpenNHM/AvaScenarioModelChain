# ------------------ Step 14: AvaDirectory Type ------------------ #
# Purpose: Merge all com4_* folders into a unified AvaDirectoryType dataset.
# Inputs : 11_avaDirectory/<caseFolder>/com4_*/praID*.geojson
# Outputs: avaDirectoryType.csv, avaDirectoryType.geojson, avaDirectoryType.parquet
# Config : [avaDIRECTORY] + [WORKFLOW]
# Consumes: Step 13 output
# Provides: Master table for scenario classification / Step 15

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
def runAvaDirectoryType(cfg, workFlowDir):
    """Step 14: Merge all com4_* folders into unified AvaDirectoryType dataset."""
    log.info("Step 14: Start AvaDirectory Type build...")

    wf = cfg["WORKFLOW"]
    main = cfg["MAIN"]

    # --- Resolve core directories dynamically ---
    caseFolder = workflowUtils.caseFolderName(cfg)
    rootDir = Path(main["workDir"]) / main["project"] / main["ID"]
    avaRoot = rootDir / "11_avaDirectory" / caseFolder
    cairosDir = Path(workFlowDir["cairosDir"])

    if not avaRoot.exists():
        log.warning("Step 14: Expected AvaDirectory root missing: %s", relPath(avaRoot, cairosDir))
        return

    log.info("Step 14: Using AvaDirectory root: %s", relPath(avaRoot, cairosDir))

    # --- Single-test mode filtering (optional, for cleanliness) ---
    if wf.getboolean("makeSingleTestRun", False):
        singleDir = wf.get("singleTestDir", "").strip()
        log.info("Step 14: Single-test mode active (singleTestDir=%s).", singleDir or "<not set>")

    # --- Discover all com4_* folders ---
    com4Folders = sorted(glob.glob(str(avaRoot / "com4_*")))
    if not com4Folders:
        log.warning("Step 14: No com4_* folders found in %s", relPath(avaRoot, cairosDir))
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
                log.exception("Failed to read %s", relPath(pf, cairosDir))

    if not all_chunks:
        log.warning("Step 14: No praID*.geojson files found under %s", relPath(avaRoot, cairosDir))
        return

    merged = pd.concat(all_chunks, ignore_index=True)
    log.info("Step 14: Merged %d rows from %d files", len(merged), total_files)

    # --- Drop legacy or redundant columns ---
    for col in ["resId", "PRA_id", "Sector"]:
        if col in merged.columns:
            merged = merged.drop(columns=[col])

    # --- Normalize flow column ---
    if "flow" in merged.columns:
        merged["flow"] = (
            merged["flow"]
            .replace({"Dry": "dry", "Wet": "wet"})
            .astype("string")
        )

    # --- Convert key numeric columns to nullable Int64 ---
    int_cols = [
        "praID", "praAreaSized", "LKGebietID", "subC",
        "elevMin", "elevMax", "ppm", "pem", "rSize"
    ]
    for col in int_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("Int64")

    # --- De-duplicate by essential keys (keep rel/res separate) ---
    dedupCols = [c for c in ["praID", "resultID", "flow", "modType"] if c in merged.columns]
    if dedupCols:
        before = len(merged)
        merged = merged.drop_duplicates(subset=dedupCols, keep="first").reset_index(drop=True)
        log.info("Step 14: Removed %d duplicates using %s", before - len(merged), dedupCols)

    # --- Write outputs ---
    csvPath = avaRoot / "avaDirectoryType.csv"
    merged.drop(columns="geometry", errors="ignore").to_csv(csvPath, index=False)

    geojsonPath = avaRoot / "avaDirectoryType.geojson"
    try:
        _write_gdf(merged, geojsonPath)
    except Exception as e:
        log.warning("Step 14: GeoJSON write warning: %s", e)

    parquetPath = avaRoot / "avaDirectoryType.parquet"
    try:
        merged.to_parquet(parquetPath, index=False)
    except Exception as e:
        log.warning("Step 14: Parquet write warning: %s", e)

    log.info(
        "Step 14: ✅ Wrote %d features → CSV, GeoJSON, Parquet in %s",
        len(merged), relPath(avaRoot, cairosDir)
    )
    return avaRoot
