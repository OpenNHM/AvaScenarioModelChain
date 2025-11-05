# ------------------ Step 15: AvaDirectory Results ------------------ #
# Purpose: Enrich AvaDirectoryType with raster paths for each PRA/result combination.
# Inputs : 11_avaDirectory/<caseFolder>/avaDirectoryType.parquet + com4_*/.tif
# Outputs: avaDirectoryResults.csv, .geojson, .parquet
# Config : [avaDIRECTORY] + [WORKFLOW]
# Consumes: Step 14 outputs (AvaDirectoryType)
# Provides: Master AvaDirectoryResults table (for scenario maps / Step 16)

import os
import glob
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
def runAvaDirectoryResults(cfg, workFlowDir):
    """Step 15: Build avaDirectoryResults.* with raster paths and attributes."""
    log.info("Step 15: Start AvaDirectory Results build...")

    avaCfg = cfg["avaDIRECTORY"]
    main = cfg["MAIN"]

    # --- Resolve core paths dynamically ---
    caseFolder = workflowUtils.caseFolderName(cfg)
    rootDir = Path(main["workDir"]) / main["project"] / main["ID"]
    avaDirectoryRoot = rootDir / "11_avaDirectory" / caseFolder
    outAvaScenMapDir = rootDir / "12_avaScenMaps"
    outAvaScenMapDir.mkdir(parents=True, exist_ok=True)

    cairosDir = Path(workFlowDir["cairosDir"])

    # --- Core inputs ---
    avaTypeParquet = avaDirectoryRoot / "avaDirectoryType.parquet"
    avaTypeGeoJSON = avaDirectoryRoot / "avaDirectoryType.geojson"

    if not avaTypeParquet.exists() and not avaTypeGeoJSON.exists():
        log.warning(
            "Step 15: Missing AvaDirectoryType outputs in %s",
            relPath(avaDirectoryRoot, cairosDir),
        )
        return

    # --- Outputs ---
    outCsv = avaDirectoryRoot / "avaDirectoryResults.csv"
    outGeoJson = avaDirectoryRoot / "avaDirectoryResults.geojson"
    outParquet = avaDirectoryRoot / "avaDirectoryResults.parquet"

    # --- Cache for file index ---
    indexAvaFiles = outAvaScenMapDir / "indexAvaFiles.pkl"
    forceRebuildIndex = avaCfg.getboolean("forceRebuildIndex", False)
    forceRebuildResults = avaCfg.getboolean("forceRebuildResults", False)

    # --- Raster name patterns ---
    typePatterns = {
        "inputPRA": "-area_m.tif",
        "cellCounts": "__cellCounts_lzw.tif",
        "zDelta": "_zdelta_lzw.tif",
        "zDelta_sized": "_zdelta_sized_lzw.tif",
        "travelLengthMax": "_travelLengthMax_lzw.tif",
        "travelLengthMax_sized": "_travelLengthMax_sized_lzw.tif",
        "travelAngleMax": "_fpTravelAngleMax_lzw.tif",
        "travelAngleMax_sized": "_fpTravelAngleMax_sized_lzw.tif",
    }

    # --- Build or load file index ---
    index = _loadOrBuildFileIndex(
        avaDirectoryRoot, indexAvaFiles, typePatterns, forceRebuildIndex, cairosDir
    )

    # --- Build results ---
    avaDir = _makeAvaDirectoryResults(
        avaDirectoryRoot=avaDirectoryRoot,
        avaTypeParquet=avaTypeParquet,
        avaTypeGeoJSON=avaTypeGeoJSON,
        fileIndex=index,
        outCsv=outCsv,
        outGeoJson=outGeoJson,
        outParquet=outParquet,
        forceRebuild=forceRebuildResults,
        cairosDir=cairosDir,
    )

    # --- Final summary ---
    resCount = (avaDir["modType"] == "res").sum() if "modType" in avaDir.columns else 0
    relCount = (avaDir["modType"] == "rel").sum() if "modType" in avaDir.columns else 0
    log.info("Step 15: AvaDirectoryResults written: %d features (res=%d, rel=%d)", len(avaDir), resCount, relCount)
    log.info("Step 15: Completed successfully.")
    return avaDir


# ------------------ Helpers ------------------ #
def _read_gdf(path):
    """Read GeoDataFrame with pyogrio if available."""
    return pyogrio.read_dataframe(path) if (_HAS_PYOGRIO and path.suffix == ".geojson") else gpd.read_file(path)


def _buildFileIndex(avaDirectoryRoot: Path, typePatterns: dict) -> dict:
    """Scan all com4_* folders and map raster paths by (praID, resultID)."""
    index = {}
    com4Dirs = list(avaDirectoryRoot.glob("com4_*"))
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


def _loadOrBuildFileIndex(avaDirectoryRoot, indexFile, typePatterns, forceRebuild, cairosDir):
    """Load cached index from pickle or rebuild."""
    if indexFile.exists() and not forceRebuild:
        try:
            with open(indexFile, "rb") as f:
                index = pickle.load(f)
            log.info("Loaded cached file index (%d entries) from %s", len(index), relPath(indexFile, cairosDir))
            return index
        except Exception as e:
            log.warning("Failed to load cached index (%s), rebuilding...", e)

    log.info("Step 15: Building file index from com4_* folders...")
    index = _buildFileIndex(avaDirectoryRoot, typePatterns)
    with open(indexFile, "wb") as f:
        pickle.dump(index, f)
    log.info("Step 15: File index built with %d PRA/resultID combinations", len(index))
    return index


def _makeAvaDirectoryResults(
    avaDirectoryRoot,
    avaTypeParquet,
    avaTypeGeoJSON,
    fileIndex,
    outCsv,
    outGeoJson,
    outParquet,
    forceRebuild,
    cairosDir,
):
    """Merge AvaDirectoryType + raster path index into full AvaDirectoryResults."""
    if outParquet.exists() and not forceRebuild:
        try:
            avaDir = gpd.read_parquet(outParquet)
            log.info("Step 15: Loaded cached AvaDirectoryResults (%d features)", len(avaDir))
            return avaDir
        except Exception as e:
            log.warning("Step 15: Failed to load cached Results (%s), rebuilding...", e)

    # --- Load AvaDirectoryType base ---
    avaDir = gpd.read_parquet(avaTypeParquet) if avaTypeParquet.exists() else _read_gdf(avaTypeGeoJSON)

    # --- Key cleanup ---
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

    # --- Discover raster types ---
    allTypes = sorted({t for v in fileIndex.values() for t in v.keys()})
    log.info("Step 15: Found %d raster types: %s", len(allTypes), allTypes)

    # --- Create path columns and fill ---
    for t in allTypes:
        avaDir[f"path{t.capitalize()}"] = None

    def _rel(p):
        try:
            return os.path.relpath(p, start=avaDirectoryRoot) if p else None
        except Exception:
            return None

    for i, row in avaDir.iterrows():
        key = (int(row["praID"]), str(row["resultID"]))
        entry = fileIndex.get(key)
        if not entry:
            continue
        for t in allTypes:
            p = entry.get(t)
            if p:
                avaDir.at[i, f"path{t.capitalize()}"] = _rel(p)

    # --- Write outputs ---
    avaDir.drop(columns="geometry", errors="ignore").to_csv(outCsv, index=False)
    avaDir.to_file(outGeoJson, driver="GeoJSON")
    avaDir.to_parquet(outParquet, index=False)

    log.info("Step 15: AvaDirectoryResults written (%d features) → %s", len(avaDir), relPath(avaDirectoryRoot, cairosDir))
    return avaDir
