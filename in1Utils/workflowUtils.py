# cairos/in1Utils/workflowUtils.py

'''
helpers for runCairos.py
'''

from __future__ import annotations
from typing import Optional, Iterable
import pathlib


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def stepEnabled(flags, key: str, master: bool = False, default: bool = False) -> bool:
    """Master flag overrides per-step flags; else read from [WORKFLOW]."""
    return True if master else flags.getboolean(key, fallback=default)

def caseFolderName(cfg) -> str:
    """Matches Step-08/09 naming, including optional '-praBound'."""
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
    v = (val or "").strip()
    items = [s.strip() for s in v.split(",") if s.strip()]
    return items or ["dry"]

def parseSizeRange(val: str) -> list[int]:
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
    """
    Return absolute DEM path (as string) if using BigData (00_input/DEM), else None to use <Inputs>/DEM.tif.
    """
    if use_big_data:
        p = pathlib.Path(input_dir) / dem_name
        return str(p)
    return None




def discoverAvaDirs(cfg, workFlowDir):
    """Discover available AvaFrame case leaf directories (SizeN/dry|wet)."""
    avaDirs = []
    avaParams = cfg["avaPARAMETER"] if "avaPARAMETER" in cfg else cfg["MAIN"]
    flowTypes = parseFlowTypes(avaParams.get("flowTypes", "dry"))
    sizeList  = parseSizeRange(avaParams.get("sizeRange", "2-5"))
    parentCase = caseFolderName(cfg)
    rootPath = pathlib.Path(workFlowDir["flowPyRunDir"]) / parentCase

    if rootPath.exists():
        for case in sorted(p for p in rootPath.iterdir() if p.is_dir()):
            for N in sizeList:
                for scen in flowTypes:
                    cand = case / f"Size{N}" / scen
                    if cand.is_dir():
                        avaDirs.append(cand)
    return avaDirs
