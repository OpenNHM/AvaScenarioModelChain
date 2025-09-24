# runInitWorkDir.py

from typing import Union
import os
import pathlib
import configparser
import logging

# reuse your existing config reader
from in1Utils.cfgUtils import readConfig

log = logging.getLogger(__name__)

def initWorkDir(config_or_path: Union[str, pathlib.Path, configparser.ConfigParser]):
    """
    Create the CAIROS workflow directory structure based on config.

    Accepts:
      - str/pathlib.Path: path to an INI file
      - ConfigParser: in-memory config (preferred for reproducibility)

    Returns a dict of absolute paths for all step directories.

    Note: No effective.ini, no manifest. Only creates directories.
    """
    # Load/normalize config
    if isinstance(config_or_path, (str, pathlib.Path)):
        cfg = readConfig(config_or_path)
    elif isinstance(config_or_path, configparser.ConfigParser):
        cfg = config_or_path
    else:
        raise TypeError("initWorkDir expects a path or a ConfigParser")

    if "MAIN" not in cfg:
        raise KeyError("Config missing [MAIN] section")

    # Respect initWorkDir flag
    if not cfg["MAIN"].getboolean("initWorkDir", fallback=False):
        log.info("initWorkDir=False -> no directories created")
        return {}

    workDir = (cfg["MAIN"].get("workDir", "") or "").strip()
    project = (cfg["MAIN"].get("project", "") or "").strip()
    ID      = (cfg["MAIN"].get("ID", "") or "").strip()

    if not workDir or not project or not ID:
        raise ValueError(
            f"MAIN fields must be set (workDir, project, ID). "
            f"Got workDir='{workDir}', project='{project}', ID='{ID}'."
        )

    # Compose and create base dir
    cairosDir = pathlib.Path(workDir) / project.lstrip("/").rstrip("/") / ID.lstrip("/").rstrip("/")
    cairosDir.mkdir(parents=True, exist_ok=True)

    # Define step subfolders
    steps = [
        ("input",                  "00_input"),
        ("praDelineation",         "01_praDelineation"),
        ("praSelection",           "02_praSelection"),
        ("praBottleneckSmoothing", "03_praBottleneckSmoothing"),
        ("praSubcatchments",       "04_praSubcatchments"),
        ("praProcessing",          "05_praProcessing"),
        ("praSegmentation",        "06_praSegmentation"),
        ("praAssignElevSize",      "07_praAssignElevSize"),
        ("praPrepForFlowPy",       "08_praPrepForFlowPy"),
        ("praMakeBigDataStructure","09_flowPyBigDataStructure"),
        ("flowPySizeParameters",   "09_flowPyBigDataStructure"),
        ("flowPyRun",              "09_flowPyBigDataStructure"),
        ("flowPyResToSize",        "10_flowPyOutput"),
        ("flowPyOutput",           "10_flowPyOutput"),
        ("avaDirectory",           "11_avaDirectory"),
        ("avaScenMaps",            "12_avaScenMaps"),
        ("plots",                  "91_plots"),
        ("gis",                    "92_GIS"),
    ]

    workFlowDir = {"cairosDir": str(cairosDir)}
    for flag, folder in steps:
        varName = f"{flag}Dir"
        dirPath = cairosDir / folder
        dirPath.mkdir(parents=True, exist_ok=True)
        workFlowDir[varName] = str(dirPath)


    log.info("cairosDir: %s", workFlowDir["cairosDir"])

    for key, path in workFlowDir.items():
        relPath = os.path.relpath(path, start=workFlowDir['cairosDir'])
        log.info(f"...{key}: ./{relPath}")

    return workFlowDir
