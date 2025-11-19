# AvaScenarioModelChain/in1Utils/cfgUtils.py

import configparser
import pathlib
import os
import pandas as pd
import logging
import json
import platform
import subprocess
from datetime import datetime
from typing import Union, Dict, Any, List, Optional
import hashlib
import re
import sys
import pathlib
from osgeo import gdal



log = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Basic config helpers
# ----------------------------------------------------------------------

def getConfig(modName: str = "avaScenModelChain") -> configparser.ConfigParser:
    """
    Load config from either local_<modName>Cfg.ini or <modName>Cfg.ini in CWD.
    Returns a ConfigParser (case-preserving).
    """
    modPath = pathlib.Path(os.getcwd())
    localFile = modPath / f"local_{modName}Cfg.ini"
    defaultFile = modPath / f"{modName}Cfg.ini"

    if localFile.is_file():
        iniFile = localFile
    elif defaultFile.is_file():
        iniFile = defaultFile
    else:
        raise FileNotFoundError(
            f"Config file not found in {modPath}: {localFile.name} or {defaultFile.name}"
        )

    log.info("Reading config: %s", os.path.abspath(iniFile))
    return readConfig(iniFile)


def readConfig(iniFile: Union[str, pathlib.Path]) -> configparser.ConfigParser:
    """Read configuration file (without comparing to a default)."""
    modCfg = configparser.ConfigParser()
    modCfg.optionxform = (lambda option: option)  # type: ignore[attr-defined]
    modCfg.read(str(iniFile))
    return modCfg


def writeConfigToCsv(outPath: Union[str, pathlib.Path], cfgSection: dict) -> None:
    """
    Append (or create) a CSV with the provided section dict.
    Each run adds one row.
    """
    df = pd.DataFrame([cfgSection])
    outPath = pathlib.Path(outPath)
    outPath.mkdir(parents=True, exist_ok=True)
    csvPath = outPath / "configs.csv"
    if csvPath.is_file():
        df.to_csv(csvPath, mode="a", index=False)
    else:
        df.to_csv(csvPath, index=False)
    log.info("config section written to %s", csvPath)


def overwriteCfg(cfg: configparser.ConfigParser,
                 filePath: Union[str, pathlib.Path],
                 section: str,
                 name: str,
                 value: str) -> None:
    """
    Overwrite a single value in the INI on disk and in the given ConfigParser.
    """
    if section not in cfg:
        cfg[section] = {}
    cfg.set(section, name, value)
    with open(filePath, "w") as configfile:
        cfg.write(configfile)
    log.info("config updated [%s] %s=%s in %s", section, name, value, filePath)


# ----------------------------------------------------------------------
# Typed accessors
# ----------------------------------------------------------------------

def getStr(cfg: configparser.ConfigParser, section: str, key: str, default: str = "") -> str:
    return cfg.get(section, key, fallback=default).strip()

def getBool(cfg: configparser.ConfigParser, section: str, key: str, default: bool = False) -> bool:
    return cfg.getboolean(section, key, fallback=default)

def getInt(cfg: configparser.ConfigParser, section: str, key: str, default: int = 0) -> int:
    try:
        return cfg.getint(section, key, fallback=default)
    except ValueError:
        return default

def getFloat(cfg: configparser.ConfigParser, section: str, key: str, default: float = 0.0) -> float:
    try:
        return cfg.getfloat(section, key, fallback=default)
    except ValueError:
        return default


# ----------------------------------------------------------------------
# Value parsers
# ----------------------------------------------------------------------

def parseCsvList(val: str, default: Optional[List[str]] = None) -> List[str]:
    """
    Parse comma-separated string into a list of strings.
    Empty → default (or []).
    """
    v = (val or "").strip()
    items = [s.strip() for s in v.split(",") if s.strip()]
    return items or (default if default is not None else [])


def parseIntRangeExpr(expr: str, default: Optional[List[int]] = None) -> List[int]:
    """
    Parse an int expression like '2-5' or '2,3,7-9' into a list of ints.
    Empty → default (or []).
    """
    expr = (expr or "").strip()
    if not expr:
        return default if default is not None else []
    result: List[int] = []
    for part in expr.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                lo, hi = int(a.strip()), int(b.strip())
                if lo > hi:
                    lo, hi = hi, lo
                result.extend(range(lo, hi + 1))
            except ValueError:
                continue
        else:
            try:
                result.append(int(part))
            except ValueError:
                continue
    return result or (default if default is not None else [])


# ----------------------------------------------------------------------
# Archiving helpers
# ----------------------------------------------------------------------

def writeEffectiveConfig(cfg: configparser.ConfigParser,
                         cairosDir: Union[str, pathlib.Path],
                         filename: str = "avaScenModelChain_effective.ini") -> pathlib.Path:
    """
    Write the exact ConfigParser (including any in-memory overrides) to cairosDir.
    """
    cairosDir = pathlib.Path(cairosDir)
    cairosDir.mkdir(parents=True, exist_ok=True)
    outPath = cairosDir / filename

    cfg_out = configparser.ConfigParser()
    cfg_out.optionxform = (lambda option: option)  # type: ignore[attr-defined]
    for sec in cfg.sections():
        if sec not in cfg_out:
            cfg_out.add_section(sec)
        for k, v in cfg[sec].items():
            cfg_out[sec][k] = v

    with open(outPath, "w") as f:
        cfg_out.write(f)

    relPath = os.path.relpath(outPath, start=cairosDir)
    log.info("effective config written: ./%s", relPath)
    return outPath


def writeRunManifest(cairosDir: Union[str, pathlib.Path],
                     cfg: configparser.ConfigParser,
                     filename: str = "run_manifest.json",
                     extra: Optional[Dict[str, Any]] = None) -> pathlib.Path:
    """
    Write a JSON manifest with environment + key flags for reproducibility.
    """
    cairosDir = pathlib.Path(cairosDir)
    cairosDir.mkdir(parents=True, exist_ok=True)
    outPath = cairosDir / filename

    manifest: Dict[str, Any] = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "workflow_flags": dict(cfg["WORKFLOW"]) if "WORKFLOW" in cfg else {},
    }
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        manifest["git_commit"] = sha
    except Exception:
        pass

    if extra:
        manifest.update(extra)

    outPath.write_text(json.dumps(manifest, indent=2))
    relPath = os.path.relpath(outPath, start=cairosDir)
    log.info("run manifest written: ./%s", relPath)
    return outPath



def extractAspect(filename: str) -> str:
    """
    Extract aspect (N/E/S/W) from filename like 'pra030secE-2200-2400-4.geojson'.
    Defaults to 'N' if not found.
    """
    m = re.search(r"sec([NSEW])", filename)
    return m.group(1) if m else "Unknown"


def hashGroup(bandLabel: str, sizeClass: int, aspect: str) -> str:
    """
    Deterministic 2-digit group code in range [10–99].
    Uses stable hash of (bandLabel, sizeClass, aspect).
    """
    key = f"{bandLabel}-{sizeClass}-{aspect}"
    h = int(hashlib.sha1(key.encode("utf-8")).hexdigest(), 16)
    group_num = 10 + (h % 90)  # ensures 10–99
    return f"{group_num:02d}"




# ----------------------------------------------------------------------
# GDAL / PROJ environment setup
# ----------------------------------------------------------------------

def setupGdalEnv(verbose: bool = False) -> None:
    """
    Configure GDAL and PROJ environment variables for Pixi-based installations.

    Ensures that:
      - GDAL_DATA and PROJ_LIB point to the correct <env>/share directories
      - PAM is disabled (no .aux.xml creation)
      - Directory scanning is off for speed
      - PROJ networking is disabled for reproducibility
      - GDAL exceptions are enabled to avoid FutureWarnings

    Call once at program startup (e.g., in runAvaScenModelChain.py).
    """


    env_prefix = pathlib.Path(sys.prefix)
    gdal_data = env_prefix / "share" / "gdal"
    proj_data = env_prefix / "share" / "proj"

    os.environ.update({
        "PROJ_LIB": str(proj_data),
        "GDAL_DATA": str(gdal_data),
        "GDAL_PAM_ENABLED": "NO",
        "GDAL_DISABLE_READDIR_ON_OPEN": "YES",
        "GTIFF_SRS_SOURCE": "EPSG",
        "CPL_DEBUG": "OFF",
        "CPL_LOG": "/dev/null",
        "PROJ_NETWORK": "OFF",
    })

    # Enable GDAL exceptions to avoid FutureWarning in 4.0+
    try:
        gdal.UseExceptions()
    except Exception:
        log.debug("setupGdalEnv: Could not enable GDAL exceptions")

    # Optional verification & debug output
    if verbose:
        log.info("GDAL/PROJ environment initialized with prefix: %s", env_prefix)
        for key in ("GDAL_DATA", "PROJ_LIB"):
            val = os.environ.get(key, "")
            exists = pathlib.Path(val).exists()
            log.info("  %s = %s%s", key, val, "" if exists else "  [⚠ not found]")

    # Log concise debug message at default verbosity
    log.debug("GDAL/PROJ environment initialized with prefix: %s", env_prefix)
