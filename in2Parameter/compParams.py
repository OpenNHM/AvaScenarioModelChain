# AvaScenarioModelChain/in2Parameter/compParams.py
# Author: Paula Spannring (BFW)
# Modified: Christoph Hesselbach (BFW)

import pathlib
import os
import numpy as np
import logging
from typing import Optional, Union

import in1Utils.dataUtils as dataUtils
import in2Parameter.sizeParameters as sP

log = logging.getLogger(__name__)


# ── helper for relative logging ──────────────────────────────────────────────
def _rel(path: Union[str, pathlib.Path], base: pathlib.Path) -> str:
    if not path:
        return "<none>"
    try:
        return "./" + os.path.relpath(str(path), start=str(base))
    except Exception:
        return str(path)


def _resolve_dem_path(
    ava_dir: pathlib.Path,
    cfg_ava_param,
    dem_override: Optional[Union[str, pathlib.Path]] = None,
) -> pathlib.Path:
    """
    Resolve DEM path with simple precedence:
    1) dem_override if provided
    2) [avaPARAMETER].customDemDir = True -> demDir
    3) AvaFrame structure -> <avaDir>/Inputs/DEM.tif
    (Left here for convenience; the main function keeps the original DEM logic.)
    """
    if dem_override:
        return pathlib.Path(dem_override)

    if cfg_ava_param.getboolean("customDemDir", fallback=False):
        dem_dir = (cfg_ava_param.get("demDir", "") or "").strip()
        if not dem_dir:
            raise ValueError("customDemDir=True but [avaPARAMETER].demDir is empty")
        return pathlib.Path(dem_dir)

    # Default: DEM inside AvaFrame Inputs
    return ava_dir / "Inputs" / "DEM.tif"


# ────────────────────────────────────────────────────────────────────────────
# Backward- & forward-compatible parameter computation
# ────────────────────────────────────────────────────────────────────────────
def computeAndSaveParameters(
    avaDir: Union[str, pathlib.Path],
    cfgAvaParam,     # e.g. cfg["avaPARAMETER"]
    cfgAvaSize,      # e.g. cfg["avaSIZE"]
    *,
    demOverride: Optional[Union[str, pathlib.Path]] = None,
    compressFiles: bool = False,
):
    ava_dir = pathlib.Path(avaDir)

    # Ensure parameter subfolders exist
    dataUtils.createParameterFolders(ava_dir)
    pathInput = pathlib.Path(dataUtils.getInputPath(ava_dir))

    # --- DEM ---
    if demOverride:
        dem_path = pathlib.Path(demOverride)
    elif cfgAvaParam.getboolean("customDemDir", fallback=False):
        dem_path = pathlib.Path(cfgAvaParam.get("demDir", "") or "")
    else:
        dem_path = pathInput  # let dataUtils pick DEM inside <Inputs>
    dem, _ = dataUtils.readRaster(dem_path)

    # --- PRA: 'pra*.tif' in Inputs/REL (case-insensitive) ---
    rel_dir = pathlib.Path(pathInput) / "REL"
    pra_candidates = sorted(rel_dir.glob("[Pp][Rr][Aa]*.[Tt][Ii][Ff]"))
    if not pra_candidates:
        raise FileNotFoundError(f"No PRA raster matching 'pra*.tif' in {rel_dir}")
    pra_path = pra_candidates[0]
    log.info("...running PRA raster: %s", "./" + os.path.relpath(str(pra_path), start=str(ava_dir)))
    pra, praPath = dataUtils.readRaster(pra_path)

    # --- continuous size from PRA ---
    sizePRA = sP.praToVRelSize(pra, dem, cfgAvaSize)
    sizePRA = np.asarray(sizePRA, dtype=np.float32)

    # --- clamp by folder's SizeN: sizeClamped = min(sizePRA, N) ---
    sizeN = None
    parent = ava_dir.parent.name.lower()  # .../SizeN/<dry|wet>
    if parent.startswith("size"):
        try:
            sizeN = int(parent[4:])
        except ValueError:
            sizeN = None
    if sizeN is None:
        for q in ava_dir.parents:
            nm = q.name.lower()
            if nm.startswith("size"):
                try:
                    sizeN = int(nm[4:])
                    break
                except ValueError:
                    pass

    if sizeN is not None:
        sizeClamped = np.minimum(sizePRA, float(sizeN), dtype=np.float32)
    else:
        sizeClamped = sizePRA

    # --- map clamped size -> parameters (continuous) ---
    alpha = sP.sizeToAlpha(sizeClamped, dem, cfgAvaSize).astype(np.float32, copy=False)
    uMax  = sP.sizeToUmax (sizeClamped, dem, cfgAvaSize).astype(np.float32,  copy=False)
    exp   = sP.sizeToExp  (sizeClamped, dem, cfgAvaSize).astype(np.float32,  copy=False)

    # valid where PRA > 0
    mask_valid = np.isfinite(pra) & (pra > 0)
    NODATA = -9999.0
    alpha[~mask_valid] = NODATA
    uMax[~mask_valid]  = NODATA
    exp[~mask_valid]   = NODATA

    # --- save ---
    out_umax  = pathInput / "UMAX"  / "umax.tif"
    out_alpha = pathInput / "ALPHA" / "alpha.tif"
    out_exp   = pathInput / "EXP"   / "exp.tif"

    dataUtils.saveRaster(praPath, str(out_umax),  uMax,  nodata=NODATA)
    dataUtils.saveRaster(praPath, str(out_alpha), alpha, nodata=NODATA)
    dataUtils.saveRaster(praPath, str(out_exp),   exp,   nodata=NODATA)

    log.info("...saved FlowPy input rasters: %s, %s, %s",
             "./" + os.path.relpath(str(out_umax),  start=str(ava_dir)),
             "./" + os.path.relpath(str(out_alpha), start=str(ava_dir)),
             "./" + os.path.relpath(str(out_exp),   start=str(ava_dir)))

    if compressFiles:
        for folder in ("UMAX", "ALPHA", "EXP"):
            dataUtils.tifCompress(pathInput / folder, delete_original=True)
        log.info("...finished compression of input parameter rasters")

# ────────────────────────────────────────────────────────────────────────────
# FlowPy result → size backmapping (Step 11)
# ────────────────────────────────────────────────────────────────────────────
def computeAndSaveSize(
    avaDir: Union[str, pathlib.Path],
    cfgAvaSize,
    *,
    flowPyUid: str = "",
):
    ava_dir = pathlib.Path(avaDir)
    resParams = (cfgAvaSize.get("resParamsToSize", "") or "").split("|")
    resParams = [p.strip() for p in resParams if p.strip()]
    if not resParams:
        raise ValueError("[avaSIZE].resParamsToSize is empty")

    for variable in resParams:
        var_key = variable.lower()
        if var_key == "zdelta":
            search_key = "zdelta"
        elif var_key in ("fptravelanglemax", "fptravelanglemin", "fptravelangle"):
            search_key = "fptravelangle"
        elif var_key in ("travellength", "travellengthmax", "travellengthmin"):
            search_key = "travellength"
        else:
            search_key = variable

        simResultFiles = dataUtils.getFlowPyOutputPath(ava_dir, search_key, flowPyUid=flowPyUid)
        if not simResultFiles:
            raise ValueError(f"The '{variable}' parameter is not in the com4FlowPy output for {ava_dir}")

        for simRaster in simResultFiles:
            data, _ = dataUtils.readRaster(simRaster)
            data = data.astype(np.float32, copy=False)
            # Treat 0 / -9999 as nodata for sizing
            data[data == 0] = np.nan
            data[data == -9999] = np.nan

            if var_key in ("fptravelanglemax", "fptravelanglemin", "fptravelangle"):
                sizeRaster = sP.alphaToSize(data, cfgAvaSize)
            elif var_key in ("travellength", "travellengthmax", "travellengthmin"):
                sizeRaster = sP.travelLengthToSize(data)
            elif var_key == "zdelta":
                sizeRaster = sP.zDeltaToSize(data, cfgAvaSize)
            else:
                raise ValueError(f"Unknown variable for size conversion: {variable}")

            fileName = os.path.basename(simRaster)
            base, ext = os.path.splitext(fileName)
            resPath = dataUtils.makeSizeFilesFolder(simRaster)
            outPath = f"{resPath}/{base}_sized{ext}"

            dataUtils.saveRaster(simRaster, outPath, sizeRaster)
            log.info("...saved size raster: %s", _rel(outPath, ava_dir))
