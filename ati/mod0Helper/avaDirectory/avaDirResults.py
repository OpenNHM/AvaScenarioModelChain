# ------------ Step 15: AvaDirectory Results ---------------------------- #
#
# Purpose :
#     Enrich the AvaDirectoryType table (from Step 14) with paths to all
#     raster outputs associated with each PRA and simulation scenario.
#     Produces a complete, scenario-resolved AvaDirectoryResults dataset
#     used for visualization, mapping, and statistical post-processing.
#
# Inputs :
#     - 12_avaDirectory/avaDirectoryType.parquet
#     - 11_avaDirectoryData/com4_*/   (FlowPy result rasters)
#
# Outputs :
#     - 12_avaDirectory/avaDirectoryResults.csv  | .geojson | .parquet
#     - 12_avaDirectory/indexAvaFiles.pkl
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
# Date & Version :
#   2025-11 - 1.0
#
# ----------------------------------------------------------------------- #

import os
import pickle
import logging
from pathlib import Path

import pandas as pd
import geopandas as gpd

import ati.mod0Helper.dataUtils as dataUtils

log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.WARNING)

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


# ------------------ Main Entry Point ------------------ #
def runAvaDirResults(cfg, workFlowDir):
    """Step 15: Build avaDirectoryResults.* with raster paths and attributes."""
    log.info("Step 15: Start AvaDirectory Results build...")

    avaCfg = cfg["avaDIRECTORY"]
    main = cfg["MAIN"]

    # --- Resolve core directories dynamically ---
    rootDir = Path(main["workDir"]) / main["project"] / main["ID"]

    # Source: FlowPy output rasters (raw data)
    avaDirData = rootDir / "11_avaDirectoryData"
    # Destination: library of merged outputs
    avaDirLib = rootDir / "12_avaDirectory"
    # Ensure destination exists
    avaDirLib.mkdir(parents=True, exist_ok=True)

    cairosDir = Path(workFlowDir["cairosDir"])
    log.info("Step 15: Using AvaDirectoryData = %s", dataUtils.relPath(avaDirData, cairosDir))
    log.info(
        "Step 15: Writing outputs to AvaDirectory = %s", dataUtils.relPath(avaDirLib, cairosDir)
    )

    # --- Check that input AvaDirectoryType exists ---
    avaTypeParquet = avaDirLib / "avaDirectoryType.parquet"
    avaTypeGeoJSON = avaDirLib / "avaDirectoryType.geojson"
    if not avaTypeParquet.exists() and not avaTypeGeoJSON.exists():
        log.error(
            "Step 15: No avaDirectoryType.* found in %s",
            dataUtils.relPath(avaDirLib, cairosDir),
        )
        return

    # --- Define outputs ---
    outCsv = avaDirLib / "avaDirectoryResults.csv"
    outGeoJson = avaDirLib / "avaDirectoryResults.geojson"
    outParquet = avaDirLib / "avaDirectoryResults.parquet"
    indexAvaFiles = avaDirLib / "indexAvaFiles.pkl"

    # --- Flags from config ---
    forceRebuildIndex = avaCfg.getboolean("forceRebuildIndex", False)
    forceRebuildResults = avaCfg.getboolean("forceRebuildResults", False)

    writeCsv = avaCfg.getboolean("writeResultsCsv", True)
    writeGeoJSON = avaCfg.getboolean("writeResultsGeoJSON", True)
    writeParquet = avaCfg.getboolean("writeResultsParquet", True)

    log.info(
        "Step 15: Output flags → CSV=%s, GeoJSON=%s, Parquet=%s",
        writeCsv,
        writeGeoJSON,
        writeParquet,
    )

    # --- Raster filename patterns ---
    typePatterns = {
        "inputPRA": ("-praAreaM.tif", "-area_m.tif"),
        "cellCounts": "_cellCounts_lzw.tif",
        "zDelta": "_zdelta_lzw.tif",
        "zDelta_sized": "_zdelta_sized_lzw.tif",
        "travelLengthMax": "_travelLengthMax_lzw.tif",
        "travelLengthMax_sized": "_travelLengthMax_sized_lzw.tif",
        "travelAngleMax": "_fpTravelAngleMax_lzw.tif",
        "travelAngleMax_sized": "_fpTravelAngleMax_sized_lzw.tif",
    }

    # --- Build or load raster file index (optional) ---
    buildIndex = avaCfg.getboolean("buildResultsRasterIndex", True)

    index = {}
    if buildIndex:
        index = _loadOrBuildFileIndex(
            avaDirData,
            indexAvaFiles,
            typePatterns,
            forceRebuildIndex,
            cairosDir,
        )
        if not index:
            log.warning(
                "Step 15: No raster index entries found — outputs will be attributes only."
            )
    else:
        log.info(
            "Step 15: buildResultsRasterIndex=False → skipping raster scan; "
            "path columns will remain empty."
        )

    # --- Merge AvaDirectoryType with index ---
    avaDir = _makeAvaDirResults(
        avaDirLib=avaDirLib,
        avaTypeParquet=avaTypeParquet,
        avaTypeGeoJSON=avaTypeGeoJSON,
        fileIndex=index,
        typePatterns=typePatterns,
        outCsv=outCsv,
        outGeoJson=outGeoJson,
        outParquet=outParquet,
        forceRebuild=forceRebuildResults,
        writeCsv=writeCsv,
        writeGeoJSON=writeGeoJSON,
        writeParquet=writeParquet,
        cairosDir=cairosDir,
    )

    resCount = (avaDir["modType"] == "res").sum() if "modType" in avaDir.columns else 0
    relCount = (avaDir["modType"] == "rel").sum() if "modType" in avaDir.columns else 0
    log.info(
        "Step 15: AvaDirectoryResults written: %d features (res=%d, rel=%d)",
        len(avaDir),
        resCount,
        relCount,
    )
    log.info("Step 15: Completed successfully.")
    return avaDir


def _buildFileIndex(avaDirData: Path, typePatterns: dict) -> dict:
    """Scan all com4_* folders and map raster paths by (praID, resultID)."""
    index = {}
    com4Dirs = list(avaDirData.glob("com4_*"))
    if not com4Dirs:
        log.warning("Step 15: No com4_* folders found in %s", avaDirData)
        return index

    for com4Dir in tqdm(
        com4Dirs,
        desc="Step 15: Building file index",
        unit="folder",
    ):
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
            for t, patterns in typePatterns.items():
                if isinstance(patterns, str):
                    patterns = (patterns,)
                if any(pattern in fname for pattern in patterns):
                    index.setdefault((pra, rid), {})[t] = str(tifPath.resolve())
                    break
    return index


def _loadOrBuildFileIndex(avaDirData, indexFile, typePatterns, forceRebuild, cairosDir):
    """Load cached index or rebuild from rasters."""
    if indexFile.exists() and not forceRebuild:
        try:
            with open(indexFile, "rb") as f:
                index = pickle.load(f)
            indexedInputPra = any("inputPRA" in entry for entry in index.values())
            inputPraPatterns = typePatterns["inputPRA"]
            availableInputPra = any(
                any(com4Dir.glob(f"*{pattern}"))
                for com4Dir in Path(avaDirData).glob("com4_*")
                for pattern in inputPraPatterns
            )
            if availableInputPra and not indexedInputPra:
                log.info("Step 15: Cached index has no input PRA paths; rebuilding index.")
            else:
                log.info(
                    "Step 15: Loaded cached index (%d entries) from %s",
                    len(index),
                    dataUtils.relPath(indexFile, cairosDir),
                )
                return index
        except Exception as e:
            log.warning("Step 15: Failed to load cached index (%s), rebuilding...", e)

    log.info("Step 15: Building file index from %s", dataUtils.relPath(avaDirData, cairosDir))
    index = _buildFileIndex(avaDirData, typePatterns)
    with open(indexFile, "wb") as f:
        pickle.dump(index, f)
    log.info(
        "Step 15: File index built → %d PRA/resultID combinations",
        len(index),
    )
    return index


def _makeAvaDirResults(
    avaDirLib,
    avaTypeParquet,
    avaTypeGeoJSON,
    fileIndex,
    typePatterns,
    outCsv,
    outGeoJson,
    outParquet,
    forceRebuild,
    writeCsv,
    writeGeoJSON,
    writeParquet,
    cairosDir,
):
    """Merge AvaDirectoryType with raster index into AvaDirectoryResults."""
    if outParquet.exists() and not forceRebuild and writeParquet:
        try:
            avaDir = gpd.read_parquet(outParquet)
            indexedInputPra = any("inputPRA" in entry for entry in fileIndex.values())
            cachedInputPra = (
                "pathInputpra" in avaDir.columns
                and avaDir["pathInputpra"].notna().any()
            )
            if indexedInputPra and not cachedInputPra:
                log.info(
                    "Step 15: Cached results have no input PRA paths; rebuilding results."
                )
            else:
                log.info(
                    "Step 15: Loaded cached AvaDirectoryResults (%d features)",
                    len(avaDir),
                )
                return avaDir
        except Exception as e:
            log.warning("Step 15: Failed to load cached Results (%s), rebuilding...", e)

    # --- Load AvaDirectoryType base ---
    if avaTypeParquet.exists():
        avaDir = gpd.read_parquet(avaTypeParquet)
    else:
        avaDir = dataUtils.readGeoData(avaTypeGeoJSON)

    # --- Cleanup ID fields ---
    if "resId" in avaDir.columns:
        if "resultID" in avaDir.columns:
            avaDir["resultID"] = avaDir["resultID"].fillna(avaDir["resId"])
            avaDir = avaDir.drop(columns=["resId"])
        else:
            avaDir = avaDir.rename(columns={"resId": "resultID"})

    avaDir["praID"] = pd.to_numeric(
        avaDir.get("praID", pd.NA),
        errors="coerce",
    ).astype("Int64")
    avaDir["resultID"] = avaDir.get(
        "resultID",
        pd.Series([pd.NA] * len(avaDir)),
    ).astype("string")

    before = len(avaDir)
    avaDir = avaDir[avaDir["praID"].notna() & avaDir["resultID"].notna()].copy()
    dropped = before - len(avaDir)
    if dropped:
        log.info("Step 15: Dropped %d rows missing praID/resultID", dropped)

    # --- Add raster path columns (force schema, even if empty) ---
    allTypes = sorted(typePatterns.keys())
    log.info(
        "Step 15: Forcing raster path columns (%d): %s",
        len(allTypes),
        allTypes,
    )

    for t in allTypes:
        col = f"path{t.capitalize()}"
        if col not in avaDir.columns:
            avaDir[col] = None

    def _rel(p):
        try:
            return os.path.relpath(p, start=avaDirLib) if p else None
        except Exception:
            return None

    # NOTE: iterrows is simple but not ideal for 100M Zeilen → ggf. später vectorisieren
    for i, row in avaDir.iterrows():
        key = (int(row["praID"]), str(row["resultID"]))
        entry = fileIndex.get(key)
        if not entry:
            continue
        for t, pathVal in entry.items():
            avaDir.at[i, f"path{t.capitalize()}"] = _rel(pathVal)

    # --- Write outputs according to flags ---
    if writeCsv:
        try:
            avaDir.drop(columns="geometry", errors="ignore").to_csv(outCsv, index=False)
            log.info(
                "Step 15: Wrote CSV AvaDirectoryResults to %s",
                dataUtils.relPath(outCsv, cairosDir),
            )
        except Exception as e:
            log.warning("Step 15: CSV write warning: %s", e)

    if writeGeoJSON:
        try:
            avaDir.to_file(outGeoJson, driver="GeoJSON")
            log.info(
                "Step 15: Wrote GeoJSON AvaDirectoryResults to %s",
                dataUtils.relPath(outGeoJson, cairosDir),
            )
        except Exception as e:
            log.warning("Step 15: GeoJSON write warning: %s", e)

    if writeParquet:
        try:
            avaDir.to_parquet(outParquet, index=False)
            log.info(
                "Step 15: Wrote Parquet AvaDirectoryResults to %s",
                dataUtils.relPath(outParquet, cairosDir),
            )
        except Exception as e:
            log.warning("Step 15: Parquet write warning: %s", e)

    log.info(
        "Step 15: Wrote AvaDirectoryResults (%d features) to %s",
        len(avaDir),
        dataUtils.relPath(avaDirLib, cairosDir),
    )
    return avaDir
