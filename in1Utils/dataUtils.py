# cairos/in1Utils/dataUtils.py

import os
import pathlib
import shutil
import zipfile
import logging
from typing import Union, List, Tuple

import rasterio
import geopandas as gpd
import numpy as np
from rasterio.features import rasterize

import contextlib
import time
import in1Utils.cfgUtils as cfgUtils

log = logging.getLogger(__name__)
PathLike = Union[str, pathlib.Path]

# ----------------------------------------------------------------------
# Generic helpers
# ----------------------------------------------------------------------

def relPath(path, cairosDir):
    """Return path relative to cairosDir (or unchanged if error)."""
    try:
        return os.path.relpath(path, start=cairosDir)
    except Exception:
        return path


@contextlib.contextmanager
def timeIt(label, level=logging.DEBUG):
    """Context manager for timing a block of code with logging."""
    t0 = time.perf_counter()
    try:
        yield
    finally:
        log.log(level, "%s finished in %.2fs", label, time.perf_counter() - t0)



# ----------------------------------------------------------------------
# Raster helpers
# ----------------------------------------------------------------------

def getInputPath(path: PathLike) -> pathlib.Path:
    """Return the Inputs folder under the given path."""
    return pathlib.Path(path) / "Inputs"


def readRaster(path: PathLike, return_profile: bool = False) -> Tuple:
    """
    Read a raster (first band) and optionally return its profile.

    If return_profile is True → (array, profile)
    else → (array, raster_path)
    """
    p = pathlib.Path(path)

    if p.is_file():
        with rasterio.open(p) as src:
            arr = src.read(1)
            prof = src.profile
        return (arr, prof) if return_profile else (arr, p)

    if p.is_dir():
        # prefer .asc, else .tif
        files = sorted(p.glob("*.asc")) or sorted(p.glob("*.tif"))
        if not files:
            raise FileNotFoundError(f"No .asc or .tif found in {p}")
        if len(files) > 1:
            log.warning("Multiple rasters found in %s, using first: %s", p, files[0])
        with rasterio.open(files[0]) as src:
            arr = src.read(1)
            prof = src.profile
        return (arr, prof) if return_profile else (arr, files[0])

    raise FileNotFoundError(f"{p} is not a valid file or folder")


def saveRaster(refRaster: PathLike, outPath: PathLike, raster,
               dtype=None, nodata=None,
               compress=None, tiled=None, blocksize=None) -> pathlib.Path:
    """Save raster to GeoTIFF, inheriting CRS/transform from reference raster."""
    refRaster = pathlib.Path(refRaster)
    outPath = pathlib.Path(outPath)
    outPath.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(refRaster) as src_ref:
        profile = src_ref.profile.copy()
        profile.update({
            "driver": "GTiff",
            "height": raster.shape[-2],
            "width": raster.shape[-1],
            "count": 1 if raster.ndim == 2 else raster.shape[0],
            "dtype": dtype or getattr(raster, "dtype", profile.get("dtype", "float32")),
            "crs": src_ref.crs,
            "transform": src_ref.transform,
            "nodata": -9999 if nodata is None else nodata,
        })
        if compress:
            profile["compress"] = compress
        if tiled is not None:
            profile["tiled"] = tiled
        if blocksize:
            profile["blockxsize"] = blocksize
            profile["blockysize"] = blocksize

    with rasterio.open(outPath, "w", **profile) as dst:
        if raster.ndim == 2:
            dst.write(raster, 1)
        else:
            dst.write(raster)

    log.info("Raster written: %s", outPath)
    return outPath


# ----------------------------------------------------------------------
# Folder / file helpers
# ----------------------------------------------------------------------

def createParameterFolders(path: PathLike) -> None:
    """Create UMAX, EXP, ALPHA folders inside Inputs/."""
    path_inputs = getInputPath(path)
    for parameter in ["UMAX", "EXP", "ALPHA"]:
        (path_inputs / parameter).mkdir(parents=True, exist_ok=True)


def makeOutputDir(mainPath: PathLike) -> pathlib.Path:
    """Ensure Outputs/cairos exists and return its path."""
    outPath = pathlib.Path(mainPath) / "Outputs" / "cairos"
    outPath.mkdir(parents=True, exist_ok=True)
    log.info("Ensured output dir: %s", outPath)
    return outPath


def getFlowPyOutputPath(path: PathLike, variable: str, flowPyUid: str = "") -> List[pathlib.Path]:
    """
    Return list of FlowPy result rasters for a variable, robust to:
      - Min/Max suffixes
      - filename casing differences
    """
    token = str(variable).strip().lower()
    base = pathlib.Path(path) / "Outputs" / "com4FlowPy" / "peakFiles"

    if flowPyUid:
        candidates = (base / f"res_{flowPyUid}").glob("*.tif")
    else:
        candidates = base.glob("res_*/*.tif")

    return [p for p in candidates if token in p.name.lower()]


def makeSizeFilesFolder(simResultFile: PathLike) -> pathlib.Path:
    """Ensure sizeFiles/<resFolder> exists alongside peakFiles."""
    simResultFile = pathlib.Path(simResultFile)
    if "peakFiles" not in str(simResultFile):
        raise ValueError("Provided path does not contain a FlowPy peakFiles folder")

    sizePath = simResultFile.parents[2] / "sizeFiles"
    resFolder = simResultFile.parent.name
    resPath = sizePath / resFolder
    resPath.mkdir(parents=True, exist_ok=True)
    return resPath


# ----------------------------------------------------------------------
# PRA preparation helpers
# ----------------------------------------------------------------------

def filterAndWriteForFlowPy(
    inFiles,
    outDir,
    elevBandLabels,
    cairosDir,
    sizeClassesToKeep=(2, 3, 4, 5),
    cfg=None
):
    """
    For each input GeoJSON, write per-(praElevBand, praAreaSized) filtered GeoJSONs.
    Adds:
      - aspect attribute (N/S/E/W from filename)
      - deterministic 7-digit praID (2-digit group [10–99] + 5-digit feature counter [00001–99999])
    Also writes praID_translation.csv in the output dir.
    """
    import csv

    zeroFeatureFiles = {}
    nOk = nFail = totalPolys = 0
    translation_rows = []

    # counters per group_id
    group_counters = {}

    for inPath in inFiles:
        try:
            with timeIt(f"prepFlowPy({os.path.basename(inPath)})"):
                gdf = gpd.read_file(inPath)

                # schema check
                missing = [c for c in ("praElevBand", "praAreaSized") if c not in gdf.columns]
                if missing:
                    log.warning("Missing attributes %s in ./%s; skipping.",
                                ", ".join(missing), relPath(inPath, cairosDir))
                    nFail += 1
                    continue

                totalPolys += len(gdf)
                aspect = cfgUtils.extractAspect(inPath)

                for bandLabel in elevBandLabels:
                    gBand = gdf[gdf["praElevBand"] == bandLabel]

                    for sizeClass in sizeClassesToKeep:
                        gSel = gBand[gBand["praAreaSized"] == sizeClass].copy()
                        if len(gSel) == 0:
                            zeroFeatureFiles.setdefault(inPath, []).append(f"{bandLabel}-{sizeClass}")
                            continue

                        # add aspect column
                        gSel["Sector"] = aspect

                        # group ID (2-digit)
                        group_id = cfgUtils.hashGroup(bandLabel, sizeClass, aspect)

                        if group_id not in group_counters:
                            group_counters[group_id] = 0

                        praIDs = []
                        for _ in range(len(gSel)):
                            group_counters[group_id] += 1
                            praID = group_id + str(group_counters[group_id]).zfill(5)
                            praIDs.append(praID)

                        gSel["praID"] = praIDs

                        # reorder columns → praID first
                        cols = ["praID"] + [c for c in gSel.columns if c != "praID"]
                        gSel = gSel[cols]

                        # collect translation rows
                        for _, row in gSel.iterrows():
                            translation_rows.append({
                                "praID": row["praID"],
                                "praElevBand": row.get("praElevBand", ""),
                                "praAreaSized": row.get("praAreaSized", ""),
                                "Sector": row.get("Sector", "")
                            })

                        # save GeoJSON
                        baseName = os.path.basename(inPath)
                        newName = baseName
                        if "ElevBands" in newName:
                            newName = newName.replace("ElevBands", bandLabel)
                        if "Sized" in newName:
                            newName = newName.replace("Sized", f"{sizeClass}")
                        if newName == baseName:  # fallback
                            name, ext = os.path.splitext(baseName)
                            newName = f"{name}-{bandLabel}-{sizeClass}{ext}"

                        outPath = os.path.join(outDir, newName)
                        gSel.to_file(outPath, driver="GeoJSON")

                nOk += 1

        except Exception:
            nFail += 1
            log.exception("FlowPy prep failed for ./%s", relPath(inPath, cairosDir))

    # write translation CSV
    if translation_rows:
        csvPath = os.path.join(outDir, "praID_translation.csv")
        with open(csvPath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=translation_rows[0].keys())
            writer.writeheader()
            writer.writerows(translation_rows)
        log.info("Saved praID translation CSV: ./%s", relPath(csvPath, cairosDir))

    return nOk, nFail, totalPolys, zeroFeatureFiles



# ----------------------------------------------------------------------
# Archiving / compression
# ----------------------------------------------------------------------

def folderToZip(folderPath: PathLike, zipName: str = "") -> pathlib.Path:
    """Zip entire folder. Returns the zip path."""
    folderPath = pathlib.Path(folderPath)
    if zipName:
        outFilePath = folderPath.parent / f"{zipName}.zip"
    else:
        outFilePath = pathlib.Path(str(folderPath) + ".zip")

    with zipfile.ZipFile(outFilePath, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folderPath):
            for file in files:
                filePath = pathlib.Path(root) / file
                arcname = filePath.relative_to(folderPath)
                zipf.write(filePath, arcname)
    log.info("Zipped %s -> %s", folderPath, outFilePath)
    return outFilePath


def tifCompress(folder_path: PathLike, delete_original: bool = False) -> List[pathlib.Path]:
    """
    Compress all .tif/.tiff in folder (recursive) with LZW.
    Returns list of compressed files.
    """
    folder_path = pathlib.Path(folder_path)
    tif_files = [p for p in folder_path.rglob("*.tif*") if not p.name.lower().endswith("_lzw.tif")]

    if not tif_files:
        log.warning("No .tif files found to compress in %s", folder_path)
        return []

    log.info("Found %d .tif files to compress under %s", len(tif_files), folder_path)
    compressed: List[pathlib.Path] = []

    for tif_path in tif_files:
        try:
            with rasterio.open(tif_path) as src:
                profile = src.profile.copy()
                data = src.read()
            profile.update({
                "compress": "lzw",
                "BIGTIFF": "IF_SAFER",
                "nodata": profile.get("nodata", -9999)
            })
            compressed_path = tif_path.with_name(f"{tif_path.stem}_lzw.tif")
            with rasterio.open(compressed_path, "w", **profile) as dst:
                dst.write(data)
            log.info("Compressed: %s", compressed_path)
            compressed.append(compressed_path)
            if delete_original and compressed_path.exists():
                os.remove(tif_path)
                log.info("Deleted original: %s", tif_path)
        except Exception as e:
            log.warning("Failed to compress %s: %s", tif_path, e)

    return compressed


def deleteTempFolder(folder_path: PathLike) -> int:
    """Delete all 'temp' folders under folder_path. Returns count deleted."""
    folder_path = pathlib.Path(folder_path)
    count = 0
    for temp_path in folder_path.rglob("temp"):
        if temp_path.is_dir():
            try:
                shutil.rmtree(temp_path)
                log.info("Deleted temp folder: %s", temp_path)
                count += 1
            except Exception as e:
                log.warning("Failed to delete temp folder %s: %s", temp_path, e)
    return count


# ----------------------------------------------------------------------
# No Data handling & rasterization helpers
# ----------------------------------------------------------------------

def enforceNumericNoData(rasterPath: PathLike, fallback: float = -9999.0) -> None:
    """
    Ensure the raster has a valid numeric nodata value (float32).
    If nodata is None or NaN, overwrite file with fallback.
    """
    rasterPath = pathlib.Path(rasterPath)
    if not rasterPath.exists():
        raise FileNotFoundError(rasterPath)

    with rasterio.open(rasterPath, "r+") as src:
        prof = src.profile
        nodata = prof.get("nodata")

        if nodata is None or (isinstance(nodata, float) and np.isnan(nodata)):
            log.warning("Raster %s has invalid nodata (%s). Resetting to %s.",
                        rasterPath, nodata, fallback)

            arr = src.read(1)
            arr = np.where(np.isnan(arr), fallback, arr)

            prof.update(nodata=fallback, dtype="float32")

            with rasterio.open(rasterPath, "w", **prof) as dst:
                dst.write(arr, 1)

            log.info("Overwritten %s with enforced nodata=%s", rasterPath, fallback)
        else:
            log.info("Raster %s already has valid nodata=%s", rasterPath, nodata)


def readBoundaryInDemCrs(boundPath: PathLike, demCrs):
    """Read PRA boundary vector and reproject to DEM CRS if needed."""
    b = gpd.read_file(boundPath)
    if b.crs != demCrs:
        b = b.to_crs(demCrs)
    return b


def prepareGdfForRasterize(gdf: gpd.GeoDataFrame, demCrs, boundaryGdfDEM: gpd.GeoDataFrame):
    """
    Reproject vector to DEM CRS and clip to boundary (if possible).
    Returns a GeoDataFrame ready for rasterization.
    """
    if gdf.crs != demCrs:
        gdf = gdf.to_crs(demCrs)
    try:
        gdf = gpd.clip(gdf, boundaryGdfDEM)
    except Exception as e:
        log.warning("Vector clip to boundary failed: %s. Proceeding un-clipped.", e)
    return gdf


def selectRasterizeSpec(mode: str, gdf: gpd.GeoDataFrame,
                        attribute: str, classField: str):
    """
    Decide values for rasterization.
    Always returns float32 dtype with nodata=-9999.0 to match FlowPy I/O.
    """
    mode = (mode or "attribute").lower()
    if mode == "presence":
        vals = np.ones(len(gdf), dtype=np.float32)
    elif mode == "classid":
        if classField not in gdf.columns:
            raise KeyError(f"classId mode requires field '{classField}'")
        vals = gdf[classField].fillna(0).astype(int).astype(np.float32).values
    else:  # attribute (default)
        if attribute not in gdf.columns:
            raise KeyError(f"attribute mode requires field '{attribute}'")
        vals = gdf[attribute].fillna(0.0).astype(float).astype(np.float32).values

    return "float32", -9999.0, vals


def rasterizeGeojsonToTif(gdf: gpd.GeoDataFrame,
                          demPath: PathLike,
                          boundaryGdfDEM: gpd.GeoDataFrame,
                          outPath: PathLike,
                          mode: str,
                          attribute: str,
                          classField: str,
                          allTouched: bool,
                          compress: bool):
    """
    Rasterize a vector to DEM grid and save to GeoTIFF.
    Output is always float32 with nodata=-9999.0 and optional LZW compression.
    """
    _, demProfile = readRaster(demPath, return_profile=True)
    demCrs = demProfile["crs"]
    height = demProfile["height"]
    width = demProfile["width"]
    transform = demProfile["transform"]

    gdf = prepareGdfForRasterize(gdf, demCrs, boundaryGdfDEM)
    dtype, nodata, vals = selectRasterizeSpec(mode, gdf, attribute, classField)

    if len(gdf) == 0:
        arr = np.full((height, width), nodata, dtype=np.float32)
    else:
        shapes = list(zip(gdf.geometry, vals))
    arr = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=float(nodata),  
        all_touched=bool(allTouched),
        dtype="float32"
    )

    saveRaster(demPath, outPath, arr, dtype="float32", nodata=-9999.0,
               compress=("LZW" if compress else None))
    return outPath