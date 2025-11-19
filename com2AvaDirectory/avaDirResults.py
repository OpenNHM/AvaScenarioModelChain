# ------------ Step 15: AvaDirectory Results ---------------------------- #
#
# Purpose :
#     Enrich the AvaDirectoryType table (from Step 14) with paths to all
#     raster outputs associated with each PRA and simulation scenario.
#     Produces a complete, scenario-resolved AvaDirectoryResults dataset
#     used for visualization, mapping, and statistical post-processing.
#
# Inputs :
#     - 12_avaDirectory/<caseFolder>/avaDirectoryType.parquet
#     - 11_avaDirectoryData/<caseFolder>/com4_*/   (FlowPy result rasters)
#
# Outputs :
#     - 12_avaDirectory/<caseFolder>/avaDirectoryResults.csv  | .geojson | .parquet
#     - 12_avaDirectory/<caseFolder>/indexAvaFiles.pkl
#
# Config :
#     [avaDIRECTORY]
#     [WORKFLOW]
#
# Consumes :
#     - Step 14 outputs (AvaDirectoryType)
#
# Provides :
#     - Master AvaDirectoryResults dataset for:
#         • Scenario map creation (Step 16 / Step 17 usage)
#         • Statistical or spatial analysis
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
import pickle
import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from in1Utils.dataUtils import relPath
import in1Utils.workflowUtils as workflowUtils

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.WARNING)

try:
    import pyogrio
    _HAS_PYOGRIO = True
except Exception:
    _HAS_PYOGRIO = False


# ------------------ Main Entry Point ------------------ #
def runAvaDirResults(cfg, workFlowDir):
    """Step 15: Build avaDirectoryResults.* with raster paths and attributes."""
    log.info("Step 15: Start AvaDirectory Results build...")

    avaCfg = cfg["avaDIRECTORY"]
    main = cfg["MAIN"]

    # --- Resolve core directories dynamically ---
    caseFolder = workflowUtils.caseFolderName(cfg)
    rootDir = Path(main["workDir"]) / main["project"] / main["ID"]

    # Source: FlowPy output rasters (raw data)
    avaDirData = rootDir / "11_avaDirectoryData" / caseFolder
    # Destination: library of merged outputs
    avaDirLib = rootDir / "12_avaDirectory" / caseFolder
    # Ensure destination exists
    avaDirLib.mkdir(parents=True, exist_ok=True)

    cairosDir = Path(workFlowDir["cairosDir"])
    log.info("Step 15: Using AvaDirectoryData = %s", relPath(avaDirData, cairosDir))
    log.info("Step 15: Writing outputs to AvaDirectory = %s", relPath(avaDirLib, cairosDir))

    # --- Check that input AvaDirectoryType exists ---
    avaTypeParquet = avaDirLib / "avaDirectoryType.parquet"
    avaTypeGeoJSON = avaDirLib / "avaDirectoryType.geojson"
    if not avaTypeParquet.exists() and not avaTypeGeoJSON.exists():
        log.error("Step 15: No avaDirectoryType.* found in %s", relPath(avaDirLib, cairosDir))
        return

    # --- Define outputs ---
    outCsv = avaDirLib / "avaDirectoryResults.csv"
    outGeoJson = avaDirLib / "avaDirectoryResults.geojson"
    outParquet = avaDirLib / "avaDirectoryResults.parquet"
    indexAvaFiles = avaDirLib / "indexAvaFiles.pkl"

    # --- Flags from config ---
    forceRebuildIndex = avaCfg.getboolean("forceRebuildIndex", False)
    forceRebuildResults = avaCfg.getboolean("forceRebuildResults", False)

    # --- Raster filename patterns ---
    typePatterns = {
        "inputPRA": "-area_m.tif",
        "cellCounts": "_cellCounts_lzw.tif",
        "zDelta": "_zdelta_lzw.tif",
        "zDelta_sized": "_zdelta_sized_lzw.tif",
        "travelLengthMax": "_travelLengthMax_lzw.tif",
        "travelLengthMax_sized": "_travelLengthMax_sized_lzw.tif",
        "travelAngleMax": "_fpTravelAngleMax_lzw.tif",
        "travelAngleMax_sized": "_fpTravelAngleMax_sized_lzw.tif",
    }

    # --- Build or load raster file index ---
    index = _loadOrBuildFileIndex(
        avaDirData, indexAvaFiles, typePatterns, forceRebuildIndex, cairosDir
    )
    if not index:
        log.warning("Step 15: No raster index entries found — outputs will be attributes only.")

    # --- Merge AvaDirectoryType with index ---
    avaDir = _makeAvaDirResults(
        avaDirLib=avaDirLib,
        avaTypeParquet=avaTypeParquet,
        avaTypeGeoJSON=avaTypeGeoJSON,
        fileIndex=index,
        outCsv=outCsv,
        outGeoJson=outGeoJson,
        outParquet=outParquet,
        forceRebuild=forceRebuildResults,
        cairosDir=cairosDir,
    )

    resCount = (avaDir["modType"] == "res").sum() if "modType" in avaDir.columns else 0
    relCount = (avaDir["modType"] == "rel").sum() if "modType" in avaDir.columns else 0
    log.info("Step 15: AvaDirectoryResults written: %d features (res=%d, rel=%d)", len(avaDir), resCount, relCount)
    log.info("Step 15: Completed successfully.")
    return avaDir


# ------------------ Helpers ------------------ #
def _read_gdf(path):
    return pyogrio.read_dataframe(path) if (_HAS_PYOGRIO and path.suffix == ".geojson") else gpd.read_file(path)


def _buildFileIndex(avaDirData: Path, typePatterns: dict) -> dict:
    """Scan all com4_* folders and map raster paths by (praID, resultID)."""
    index = {}
    com4Dirs = list(avaDirData.glob("com4_*"))
    if not com4Dirs:
        log.warning("Step 15: No com4_* folders found in %s", avaDirData)
        return index

    for com4Dir in tqdm(com4Dirs, desc="Building file index", unit="folders"):
        rid = com4Dir.name.split("com4_")[1]
        for tifPath in com4Dir.glob("*.tif"):
            fname = tifPath.name
            if "praID" not in fname:
                continue
            praStr = fname.split("_")[0].replace("praID", "")
            try:
                pra = int(praStr)
            except ValueError:
                continue
            for t, pat in typePatterns.items():
                if pat in fname:
                    index.setdefault((pra, rid), {})[t] = str(tifPath.resolve())
                    break
    return index


def _loadOrBuildFileIndex(avaDirData, indexFile, typePatterns, forceRebuild, cairosDir):
    """Load cached index or rebuild from rasters."""
    if indexFile.exists() and not forceRebuild:
        try:
            with open(indexFile, "rb") as f:
                index = pickle.load(f)
            log.info("Step 15: Loaded cached index (%d entries) from %s", len(index), relPath(indexFile, cairosDir))
            return index
        except Exception as e:
            log.warning("Step 15: Failed to load cached index (%s), rebuilding...", e)

    log.info("Step 15: Building file index from %s", relPath(avaDirData, cairosDir))
    index = _buildFileIndex(avaDirData, typePatterns)
    with open(indexFile, "wb") as f:
        pickle.dump(index, f)
    log.info("Step 15: File index built → %d PRA/resultID combinations", len(index))
    return index


def _makeAvaDirResults(
    avaDirLib,
    avaTypeParquet,
    avaTypeGeoJSON,
    fileIndex,
    outCsv,
    outGeoJson,
    outParquet,
    forceRebuild,
    cairosDir,
):
    """Merge AvaDirectoryType with raster index into AvaDirectoryResults."""
    if outParquet.exists() and not forceRebuild:
        try:
            avaDir = gpd.read_parquet(outParquet)
            log.info("Step 15: Loaded cached AvaDirectoryResults (%d features)", len(avaDir))
            return avaDir
        except Exception as e:
            log.warning("Step 15: Failed to load cached Results (%s), rebuilding...", e)

    # --- Load AvaDirectoryType base ---
    avaDir = gpd.read_parquet(avaTypeParquet) if avaTypeParquet.exists() else _read_gdf(avaTypeGeoJSON)

    # --- Cleanup ID fields ---
    if "resId" in avaDir.columns:
        if "resultID" in avaDir.columns:
            avaDir["resultID"] = avaDir["resultID"].fillna(avaDir["resId"])
            avaDir = avaDir.drop(columns=["resId"])
        else:
            avaDir = avaDir.rename(columns={"resId": "resultID"})

    avaDir["praID"] = pd.to_numeric(avaDir.get("praID", pd.NA), errors="coerce").astype("Int64")
    avaDir["resultID"] = avaDir.get("resultID", pd.Series([pd.NA] * len(avaDir))).astype("string")

    before = len(avaDir)
    avaDir = avaDir[avaDir["praID"].notna() & avaDir["resultID"].notna()].copy()
    dropped = before - len(avaDir)
    if dropped:
        log.info("Step 15: Dropped %d rows missing praID/resultID", dropped)

    # --- Add raster path columns ---
    allTypes = sorted({t for v in fileIndex.values() for t in v.keys()})
    log.info("Step 15: Found %d raster types: %s", len(allTypes), allTypes)
    for t in allTypes:
        avaDir[f"path{t.capitalize()}"] = None

    def _rel(p):
        try:
            return os.path.relpath(p, start=avaDirLib) if p else None
        except Exception:
            return None

    for i, row in avaDir.iterrows():
        key = (int(row["praID"]), str(row["resultID"]))
        entry = fileIndex.get(key)
        if not entry:
            continue
        for t, pathVal in entry.items():
            avaDir.at[i, f"path{t.capitalize()}"] = _rel(pathVal)

    # --- Write outputs ---
    avaDir.drop(columns="geometry", errors="ignore").to_csv(outCsv, index=False)
    avaDir.to_file(outGeoJson, driver="GeoJSON")
    avaDir.to_parquet(outParquet, index=False)

    log.info("Step 15: Wrote AvaDirectoryResults (%d features) to %s", len(avaDir), relPath(avaDirLib, cairosDir))
    return avaDir
