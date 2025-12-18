# ------------ Step 13: AvaDirectory Build From FlowPy ------------------ #
#
# Purpose :
#     Construct the per-scenario AvaDirectory structure based on FlowPy
#     simulation outputs. This step organizes all runout results, metadata,
#     and raster products into a unified directory tree for downstream
#     classification and visualization.
#
# Inputs :
#     - 09_flowPyBigDataStructure/{caseFolder}/pra*/Size*/{dry,wet}/Outputs/com4FlowPy
#
# Outputs :
#     - 11_avaDirectory/{caseFolder}/Map/singleAvaDir/
#           com4_<scenario>/praID*.geojson
#           runout rasters (e.g. fpTravel, fpMax, thickness …)
#           avaDirectory.csv (global metadata index)
#
# Config :
#     [avaDIRECTORY]
#     [WORKFLOW]
#     [praSUBCATCHMENTS], [praSEGMENTATION], [praMAKEBIGDATASTRUCTURE]
#
# Consumes :
#     - FlowPy Big Data outputs aggregated in Step 12
#
# Provides :
#     - Structured AvaDirectory datasets for:
#         • Step 14 (AvaDirectory Type)
#         • Step 15 (AvaDirectory Results)
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
import time
import shutil
import logging
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.mask import mask
import fiona

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


warnings.filterwarnings("ignore", category=UserWarning)
log = logging.getLogger(__name__)
logging.getLogger("pyogrio").setLevel(logging.WARNING)


# ------------------ Entry Point ------------------ #
def runAvaDirBuildFromFlowPy(cfg, workFlowDir):
    log.info("Step 13: Start AvaDirectory build...")

    avaCfg = cfg["avaDIRECTORY"]
    caseFolder = workflowUtils.caseFolderName(cfg)

    workDir = Path(cfg["MAIN"]["workDir"])
    project = cfg["MAIN"]["project"]
    modelID = cfg["MAIN"]["ID"]

    # --- Core directories ---
    baseDir = workDir / project / modelID / "09_flowPyBigDataStructure" / caseFolder
    avaDirData = Path(workFlowDir["avaDirDir"])      # 11_avaDirectoryData
    avaDirLib  = Path(workFlowDir["avaDirTypeDir"])  # 12_avaDirectory
    cairosDir  = Path(workFlowDir["cairosDir"])

    log.info("Step 13: Using baseDir=%s",    relPath(baseDir, cairosDir))
    log.info("Step 13: Using avaDirData=%s", relPath(avaDirData, cairosDir))
    log.info("Step 13: Using avaDirLib=%s",  relPath(avaDirLib, cairosDir))

    if not baseDir.exists():
        log.warning("Step 13: Expected FlowPy BigData directory does not exist: %s", relPath(baseDir, cairosDir))
        return

    # --- Behaviour flags ---
    doProcess          = avaCfg.getboolean("doProcess", True)
    doSplit            = avaCfg.getboolean("doSplit", True)  # legacy singleAva split
    doMergeReljson     = avaCfg.getboolean("doMergeReljson", True)
    doEnrich           = avaCfg.getboolean("doEnrich", True)
    doExtractMetadata  = avaCfg.getboolean("doExtractMetadata", True)
    doClipRasters      = avaCfg.getboolean("doClipRasters", True)
    doCollectSingleAva = avaCfg.getboolean("doCollectSingleAva", True)
    maxClipWorkers     = avaCfg.getint("maxClipWorkers", 4)

    # --- New: output mode flags ---
    writeSingleAvaGeoJSON = avaCfg.getboolean("writeSingleAvaGeoJSON", True)
    writeScenarioParquet  = avaCfg.getboolean("writeScenarioParquet", False)

    log.info(
        "Step 13: Flags → doProcess=%s, doSplit=%s, doClipRasters=%s, maxClipWorkers=%d",
        doProcess, doSplit, doClipRasters, maxClipWorkers
    )
    log.info(
        "Step 13: Output mode → writeSingleAvaGeoJSON=%s, writeScenarioParquet=%s",
        writeSingleAvaGeoJSON, writeScenarioParquet
    )

    # --- Discover PRA directories and apply single-test filter ---
    praDirs = sorted(baseDir.glob("pra*/"))
    praDirs = _filterSingleTestDirs(cfg, praDirs, "Step 13")

    if not praDirs:
        log.warning("Step 13: No PRA directories found.")
        return
    log.info("Step 13: Found %d PRA directories", len(praDirs))

    # --- Build global task list (for overall progress bar) ---
    tasks = []
    for praDir in praDirs:
        for flowChoice in ["dry", "wet"]:
            pattern = os.path.join(praDir, f"Size*/{flowChoice}/Outputs/com4FlowPy")
            outputDirs = [p for p in glob.glob(pattern) if os.path.isdir(p)]
            for outputsDir in outputDirs:
                tasks.append((praDir, flowChoice, outputsDir))

    if not tasks:
        log.warning("Step 13: No Outputs/com4FlowPy directories found under %s", relPath(baseDir, cairosDir))
        return

    log.info("Step 13: Prepared %d FlowPy scenarios (com4_* folders) for processing.", len(tasks))

    lastPraDir = None
    for praDir, flowChoice, outputsDir in tqdm(
        tasks,
        desc="Step 13: FlowPy → AvaDir",
        unit="com4",
    ):
        if praDir is not None and praDir != lastPraDir:
            log.info("Step 13: Processing %s", relPath(praDir, cairosDir))
            lastPraDir = praDir

        gdf, targetDir, resId = (None, None, None)
        if doProcess:
            gdf, targetDir, resId = processScenario(outputsDir, cairosDir)
            if gdf is None:
                continue

        reljsonPath = _findRelJson(outputsDir)

        # --- Big-data mode: build one scenario table (res/rel rows per praID) and write GeoParquet ---
        if writeScenarioParquet:
            try:
                scenGdf = buildScenarioGdf(
                    gdf_res=gdf,
                    reljsonPath=reljsonPath,
                    doMergeReljson=doMergeReljson,
                    resId=resId,
                    doEnrich=doEnrich,
                    doExtractMetadata=doExtractMetadata,
                    outputsDir=outputsDir,
                    cairosDir=cairosDir,
                )

                outName = f"avaScenLeaf_com4_{resId}.parquet"
                outParquet = os.path.join(targetDir, outName)

                scenGdf.to_parquet(outParquet, index=False)
                log.info("Step 13: Wrote scenario parquet → %s", relPath(outParquet, cairosDir))
            except Exception:
                log.exception(
                    "Step 13: Failed to write scenario parquet for %s",
                    relPath(outputsDir, cairosDir),
                )


        # --- Legacy mode: split into many praID*.geojson ---
        if writeSingleAvaGeoJSON and doSplit and gdf is not None:
            splitGeojsonByPraId(gdf, targetDir, reljsonPath, doMergeReljson, cairosDir)

            if doEnrich or doExtractMetadata:
                for pf in glob.glob(os.path.join(targetDir, "praID*.geojson")):
                    if doEnrich:
                        enrichAvalancheFeature(pf, resId=resId, cairosDir=cairosDir)
                    if doExtractMetadata:
                        _attachScenarioMetadata(pf, cairosDir)

            if doClipRasters:
                clipRastersByMasks(
                    maskDir=targetDir,
                    outputsDir=outputsDir,
                    outputDir=targetDir,
                    cairosDir=cairosDir,
                    max_workers=maxClipWorkers,
                )

        # If we are NOT writing singleAva GeoJSONs, raster clipping has no masks to use → skip
        if (not writeSingleAvaGeoJSON) and doClipRasters:
            log.info("Step 13: doClipRasters=True but writeSingleAvaGeoJSON=False → skipping raster clipping (no masks).")

    # --- Collect to Library directory (copies com4_* folders) ---
    if doCollectSingleAva:
        collectSingleAvaDirs(baseDir, avaDirData, avaDirLib, cairosDir)

    log.info("Step 13: AvaDirectory build complete.")


# ------------------ Function: processScenario ------------------ #
def processScenario(outputsDir, cairosDir):
    """Locate com4FlowPy results and return (gdf, targetDir, resId)."""
    geojsonFiles = glob.glob(os.path.join(outputsDir, "peakFiles", "res_*", "*_pathPolygons.geojson"))
    if not geojsonFiles:
        log.warning("No pathPolygons.geojson found in %s", relPath(outputsDir, cairosDir))
        return None, None, None

    geojsonPath = geojsonFiles[0]
    resFolder = os.path.dirname(geojsonPath)
    resId = os.path.basename(resFolder).replace("res_", "")
    flowDir = os.path.dirname(os.path.dirname(outputsDir))
    mapDir = os.path.join(flowDir, "Map", "singleAvaDir", f"com4_{resId}")
    os.makedirs(mapDir, exist_ok=True)

    try:
        gdf = _read_gdf(geojsonPath)
        gdf = _normalize_ids(gdf)
        log.info("Loaded %d features from %s", len(gdf), relPath(geojsonPath, cairosDir))
        return gdf, mapDir, resId
    except Exception:
        log.exception("Failed to read GeoJSON %s", relPath(geojsonPath, cairosDir))
        return None, None, resId


# ------------------ Big-data helper: build scenario table ------------------ #
def buildScenarioGdf(
    gdf_res: gpd.GeoDataFrame,
    reljsonPath: str | None,
    doMergeReljson: bool,
    resId: str | None,
    doEnrich: bool,
    doExtractMetadata: bool,
    outputsDir: str,
    cairosDir: Path,
) -> gpd.GeoDataFrame:
    """
    Create a scenario-wide GeoDataFrame:
      - one 'res' row per praID
      - optional one 'rel' row per praID (if RELJSON exists and merge is enabled)
    """
    if gdf_res is None or gdf_res.empty:
        return gpd.GeoDataFrame()

    gdf_res = _normalize_ids(gdf_res)

    # --- extract key from res ---
    if "praID" in gdf_res.columns:
        gdf_res["_key"] = gdf_res["praID"].apply(_extract_int_like)
    elif "PRA_id" in gdf_res.columns:
        gdf_res["_key"] = gdf_res["PRA_id"].apply(_extract_int_like)
    else:
        raise ValueError("No PRA_id/praID column in FlowPy results.")

    # keep only first res geometry per key (matches legacy behaviour)
    res_first = gdf_res.dropna(subset=["_key"]).groupby("_key", as_index=False).head(1).copy()
    res_first["praID"] = res_first["_key"].astype(int)
    res_first["modType"] = "res"

    # --- merge reljson (optional) ---
    rel_first = None
    if doMergeReljson and reljsonPath and os.path.isfile(reljsonPath):
        rel_gdf = _read_gdf(reljsonPath)
        rel_gdf = _normalize_ids(rel_gdf)

        if "praID" in rel_gdf.columns:
            rel_gdf["_key"] = rel_gdf["praID"].apply(_extract_int_like)
        elif "PRA_id" in rel_gdf.columns:
            rel_gdf["_key"] = rel_gdf["PRA_id"].apply(_extract_int_like)
        else:
            log.warning("RELJSON %s has no praID/PRA_id column → skipping merge.", relPath(reljsonPath, cairosDir))
            rel_gdf = None

        if rel_gdf is not None and not rel_gdf.empty:
            rel_first = rel_gdf.dropna(subset=["_key"]).groupby("_key", as_index=False).head(1).copy()
            rel_first["praID"] = rel_first["_key"].astype(int)
            rel_first["modType"] = "rel"

            # union-copy rel attributes into res (legacy semantics)
            rel_cols = set(rel_first.columns)
            res_cols = set(res_first.columns)
            union_cols = (rel_cols | res_cols) - {"geometry", "PRA_id", "Sector"}

            for col in sorted(union_cols):
                if col not in res_first.columns and col in rel_first.columns:
                    res_first[col] = None

            rel_map = rel_first.set_index("_key")

            for idx, row in res_first.iterrows():
                k = row["_key"]
                if k not in rel_map.index:
                    continue
                for col in union_cols:
                    if col in ("geometry", "PRA_id", "Sector"):
                        continue
                    if col in rel_map.columns:
                        val = res_first.at[idx, col] if col in res_first.columns else None
                        if val is None or (isinstance(val, float) and np.isnan(val)):
                            res_first.at[idx, col] = rel_map.at[k, col]

    # --- drop legacy helper cols ---
    for df in (res_first, rel_first):
        if df is None:
            continue
        drop_cols = [c for c in ("PRA_id", "Sector") if c in df.columns]
        df.drop(columns=drop_cols, errors="ignore", inplace=True)

    combined = [res_first]
    if rel_first is not None and not rel_first.empty:
        combined.append(rel_first)

    out = gpd.GeoDataFrame(pd.concat(combined, ignore_index=True), crs=res_first.crs)

    # --- enrich + metadata (scenario-wide, no per-file I/O) ---
    if doEnrich and resId is not None:
        out["resultID"] = str(resId)

    if doExtractMetadata:
        _attachScenarioMetadataToGdf(out, outputsDir)

    # cleanup key
    out.drop(columns=[c for c in ("_key",) if c in out.columns], inplace=True, errors="ignore")
    return out


def _attachScenarioMetadataToGdf(gdf: gpd.GeoDataFrame, outputsDir: str) -> None:
    """Attach scenario metadata based on folder path (same logic as per-file extraction)."""
    import re
    path = str(outputsDir).replace("\\", "/")

    m = re.search(r"subC(\d+)", path)
    subC = int(m.group(1)) if m else None
    sector = next((s for s in ["N", "E", "S", "W"] if f"sec{s}" in path), None)
    m = re.search(r"(\d{4})-(\d{4,5})", path)
    elevMin, elevMax = (int(m.group(1)), int(m.group(2))) if m else (None, None)
    flow = "dry" if "/dry/" in path else "wet" if "/wet/" in path else None
    m = re.search(r"-(\d)(?:/|$)", path)
    ppm = int(m.group(1)) if m else None
    m = re.search(r"Size(\d)", path)
    pem = int(m.group(1)) if m else None
    rSize = None
    if ppm is not None and pem is not None:
        diff = ppm - pem
        rSize = max(1, 5 - diff)

    gdf["subC"] = subC
    gdf["sector"] = sector
    gdf["elevMin"] = elevMin
    gdf["elevMax"] = elevMax
    gdf["flow"] = flow
    gdf["ppm"] = ppm
    gdf["pem"] = pem
    gdf["rSize"] = rSize


# ------------------ Legacy: splitGeojsonByPraId (unchanged) ------------------ #
def splitGeojsonByPraId(gdf_res, targetDir, reljsonPath=None, doMergeReljson=True, cairosDir=None):
    if gdf_res is None or gdf_res.empty:
        log.warning("Nothing to split in %s", relPath(targetDir, cairosDir))
        return

    gdf_res = _normalize_ids(gdf_res)
    key_series = None
    if "praID" in gdf_res.columns:
        key_series = gdf_res["praID"].apply(_extract_int_like)
    elif "PRA_id" in gdf_res.columns:
        key_series = gdf_res["PRA_id"].apply(_extract_int_like)
    else:
        log.warning("No PRA_id/praID column in %s", relPath(targetDir, cairosDir))
        return

    keys = sorted({k for k in key_series.tolist() if k is not None})
    rel_lookup = {}
    if doMergeReljson and reljsonPath and os.path.isfile(reljsonPath):
        try:
            reljson_gdf = _read_gdf(reljsonPath)
            reljson_gdf = _normalize_ids(reljson_gdf)

            if "praID" in reljson_gdf.columns:
                rel_key = reljson_gdf["praID"]
            elif "PRA_id" in reljson_gdf.columns:
                rel_key = reljson_gdf["PRA_id"]
            else:
                log.warning("RELJSON %s has no praID/PRA_id column → skipping merge.", relPath(reljsonPath, cairosDir))
                rel_key = None

            if rel_key is not None:
                reljson_gdf["_key"] = rel_key.apply(_extract_int_like)
                for k, idx in reljson_gdf.groupby("_key").groups.items():
                    if k is None:
                        continue
                    rel_lookup[k] = reljson_gdf.loc[idx].copy()

                log.info("RELJSON merged: %d keyed release features.", len(rel_lookup))
        except Exception:
            log.exception("Failed to read RELJSON %s", relPath(reljsonPath, cairosDir))

    gdf_res["_key"] = key_series
    for k in keys:
        try:
            res_feat = gdf_res[gdf_res["_key"] == k].iloc[[0]].copy()
            res_feat["praID"] = int(k)
            res_feat["modType"] = "res"

            if k in rel_lookup:
                rel_feat = rel_lookup[k].iloc[[0]].copy()
                rel_feat["modType"] = "rel"

                rel_cols = set(rel_feat.columns)
                res_cols = set(res_feat.columns)
                union_cols = (rel_cols | res_cols) - {"geometry", "PRA_id", "Sector"}

                for col in sorted(union_cols):
                    if col not in res_feat.columns and col in rel_feat.columns:
                        res_feat[col] = rel_feat.iloc[0].get(col)
                    elif col in res_feat.columns and col in rel_feat.columns:
                        val = res_feat.iloc[0].get(col)
                        if val is None or (isinstance(val, float) and np.isnan(val)):
                            res_feat.loc[res_feat.index[0], col] = rel_feat.iloc[0].get(col)

                combined = gpd.GeoDataFrame(pd.concat([res_feat, rel_feat], ignore_index=True), crs=res_feat.crs)
            else:
                combined = res_feat

            drop_cols = [c for c in ("PRA_id", "Sector", "_key") if c in combined.columns]
            combined = combined.drop(columns=drop_cols, errors="ignore")

            outPath = os.path.join(targetDir, f"praID{k}.geojson")
            _write_gdf(combined, outPath)
        except Exception:
            log.exception("Failed to split/write praID%s", k)


def enrichAvalancheFeature(praFile, resId=None, cairosDir=None):
    if not os.path.exists(praFile):
        return
    try:
        gdf = _read_gdf(praFile)
        gdf = _normalize_ids(gdf)
        if "modType" not in gdf.columns and len(gdf) == 2:
            gdf.loc[gdf.index[0], "modType"] = "res"
            gdf.loc[gdf.index[1], "modType"] = "rel"
        if resId is not None:
            gdf["resultID"] = str(resId)
        _write_gdf(gdf, praFile)
    except Exception:
        log.exception("Failed to enrich avalanche feature %s", relPath(praFile, cairosDir))


def _attachScenarioMetadata(praFile, cairosDir):
    import re
    try:
        gdf_pf = _read_gdf(praFile)
        path = str(praFile).replace("\\", "/")

        m = re.search(r"subC(\d+)", path)
        subC = int(m.group(1)) if m else None
        sector = next((s for s in ["N", "E", "S", "W"] if f"sec{s}" in path), None)
        m = re.search(r"(\d{4})-(\d{4,5})", path)
        elevMin, elevMax = (int(m.group(1)), int(m.group(2))) if m else (None, None)
        flow = "dry" if "/dry/" in path else "wet" if "/wet/" in path else None
        m = re.search(r"-(\d)(?:/|$)", path)
        ppm = int(m.group(1)) if m else None
        m = re.search(r"Size(\d)", path)
        pem = int(m.group(1)) if m else None
        rSize = None
        if ppm is not None and pem is not None:
            diff = ppm - pem
            rSize = max(1, 5 - diff)

        gdf_pf["subC"] = subC
        gdf_pf["sector"] = sector
        gdf_pf["elevMin"] = elevMin
        gdf_pf["elevMax"] = elevMax
        gdf_pf["flow"] = flow
        gdf_pf["ppm"] = ppm
        gdf_pf["pem"] = pem
        gdf_pf["rSize"] = rSize

        _write_gdf(gdf_pf, praFile)
    except Exception:
        log.exception("Failed to extract metadata for %s", relPath(praFile, cairosDir))


def clipRastersByMasks(maskDir, outputsDir, outputDir, cairosDir, max_workers=4):
    maskFiles = sorted(glob.glob(os.path.join(maskDir, "praID*.geojson")))
    if not maskFiles:
        log.warning("No PRA masks found in %s", relPath(maskDir, cairosDir))
        return

    rasterFiles = []
    for subdir in ["peakFiles", "sizeFiles"]:
        searchDir = os.path.join(outputsDir, subdir)
        if os.path.isdir(searchDir):
            rasterFiles.extend(glob.glob(os.path.join(searchDir, "**", "*.tif"), recursive=True))

    relDir = os.path.join(os.path.dirname(os.path.dirname(outputsDir)), "Inputs", "REL")
    relRasterFiles = glob.glob(os.path.join(relDir, "*.tif")) if os.path.isdir(relDir) else []

    if not rasterFiles and not relRasterFiles:
        log.warning("No rasters found under %s", relPath(outputsDir, cairosDir))
        return

    def _clip_one_raster(rasterFile, use_res_only=False):
        done = 0
        try:
            with rasterio.Env(GDAL_CACHEMAX=512):
                with rasterio.open(rasterFile) as src:
                    for mf in maskFiles:
                        feats = list(fiona.open(mf))
                        geoms = [f["geometry"] for f in feats if (not use_res_only) or f["properties"].get("modType") == "res"]
                        if not geoms:
                            continue
                        outName = f"praID{os.path.basename(mf).replace('.geojson','')}_{os.path.basename(rasterFile)}"
                        outPath = os.path.join(outputDir, outName)
                        if os.path.exists(outPath):
                            continue
                        outImage, outTransform = mask(src, geoms, crop=True)
                        outMeta = src.meta.copy()
                        outMeta.update(
                            driver="GTiff",
                            height=outImage.shape[1],
                            width=outImage.shape[2],
                            transform=outTransform,
                            compress="LZW",
                        )
                        with rasterio.open(outPath, "w", **outMeta) as dest:
                            dest.write(outImage)
                        done += 1
        except Exception:
            log.exception("Failed to clip %s", relPath(rasterFile, cairosDir))
        return done

    log.info("Clipping %d simulation rasters + %d REL rasters...", len(rasterFiles), len(relRasterFiles))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_clip_one_raster, rf, False) for rf in rasterFiles]
        if relRasterFiles:
            futures += [ex.submit(_clip_one_raster, rf, True) for rf in relRasterFiles]
        total = sum(f.result() for f in as_completed(futures))
    log.info("Wrote %d clipped rasters in %s", total, relPath(outputDir, cairosDir))


def collectSingleAvaDirs(baseDir, avaDirData, avaDirLib, cairosDir):
    lastName = os.path.basename(str(baseDir).rstrip("/"))
    targetRoot = avaDirData / lastName
    libRoot = avaDirLib / lastName
    targetRoot.mkdir(parents=True, exist_ok=True)
    libRoot.mkdir(parents=True, exist_ok=True)
    log.info("Collecting com4_* folders into %s", relPath(targetRoot, cairosDir))

    com4Folders = []
    for pattern in [
        os.path.join(baseDir, "pra*/Size*/dry/Map/singleAvaDir/com4_*"),
        os.path.join(baseDir, "pra*/Size*/wet/Map/singleAvaDir/com4_*"),
    ]:
        com4Folders.extend(glob.glob(pattern))

    for src in com4Folders:
        dst = targetRoot / os.path.basename(src)
        if not dst.exists():
            try:
                shutil.copytree(src, dst)
            except Exception:
                log.exception("Failed to copy %s", relPath(src, cairosDir))

    # keep legacy csv (small cases). You already have flags to disable CSV elsewhere.
    allRecords = []
    for com4Dir in sorted(glob.glob(os.path.join(targetRoot, "com4_*"))):
        for pf in glob.glob(os.path.join(com4Dir, "praID*.geojson")):
            try:
                gdf = _read_gdf(pf)
                df = gdf.drop(columns="geometry", errors="ignore").copy()
                df["com4Dir"] = os.path.basename(com4Dir)
                allRecords.append(df)
            except Exception:
                log.exception("Failed to read %s", relPath(pf, cairosDir))

    if allRecords:
        merged = pd.concat(allRecords, ignore_index=True)
        csvPath = libRoot / "avaDirectory.csv"
        merged.to_csv(csvPath, index=False)
        log.info("Merged %d rows into %s", len(merged), relPath(csvPath, cairosDir))
    else:
        log.info("No praID*.geojson found for legacy avaDirectory.csv creation (this is OK in parquet-only mode).")


# ------------------ Helper utilities ------------------ #
def _read_gdf(path, columns=None):
    if _HAS_PYOGRIO:
        return pyogrio.read_dataframe(path, columns=columns)
    return gpd.read_file(path)

def _write_gdf(gdf, path, driver="GeoJSON"):
    if _HAS_PYOGRIO:
        pyogrio.write_dataframe(gdf, path, driver=driver)
    else:
        gdf.to_file(path, driver=driver)

def _normalize_ids(df):
    cols = {c: c for c in df.columns}
    for c in df.columns:
        lc = c.lower()
        if lc == "pra_id":
            cols[c] = "PRA_id"
        elif lc == "praid":
            cols[c] = "praID"
    if list(cols.values()) != list(df.columns):
        df = df.rename(columns=cols)
    return df

def _extract_int_like(v):
    try:
        return int(float(v))
    except Exception:
        return None

def _findRelJson(outputsDir):
    flowDir = os.path.dirname(os.path.dirname(outputsDir))
    reljsonPattern = os.path.join(flowDir, "Inputs", "RELJSON", "*.geojson")
    reljsonFiles = glob.glob(reljsonPattern)
    return reljsonFiles[0] if reljsonFiles else None

def _filterSingleTestDirs(cfg, dirs, stepLabel):
    wf = cfg["WORKFLOW"]
    if not wf.getboolean("makeSingleTestRun", False):
        return dirs
    singleDir = wf.get("singleTestDir", "").strip()
    if not singleDir:
        log.warning("%s: makeSingleTestRun=True but no singleTestDir specified → processing all.", stepLabel)
        return dirs
    filtered = [d for d in dirs if d.name == singleDir]
    if not filtered:
        log.warning("%s: singleTestDir %s not found → processing all.", stepLabel, singleDir)
        return dirs
    log.info("%s: Single test mode active → processing %s", stepLabel, singleDir)
    return filtered
