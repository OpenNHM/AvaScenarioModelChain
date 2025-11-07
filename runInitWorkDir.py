# runInitWorkDir.py

from typing import Union
import os
import pathlib
import configparser
import logging

from in1Utils.cfgUtils import readConfig

log = logging.getLogger(__name__)


def initWorkDir(config_or_path: Union[str, pathlib.Path, configparser.ConfigParser]):
    """
    Create the CAIROS workflow directory structure based on config.

    Returns:
      dict of absolute paths for all workflow step directories.
    """
    # ----------------------------------------------------------------------
    # Load config
    # ----------------------------------------------------------------------
    if isinstance(config_or_path, (str, pathlib.Path)):
        cfg = readConfig(config_or_path)
    elif isinstance(config_or_path, configparser.ConfigParser):
        cfg = config_or_path
    else:
        raise TypeError("initWorkDir expects a path or a ConfigParser object")

    if "MAIN" not in cfg:
        raise KeyError("Config missing [MAIN] section")

    if not cfg["MAIN"].getboolean("initWorkDir", fallback=False):
        log.info("initWorkDir=False → no directories created")
        return {}

    # ----------------------------------------------------------------------
    # Core project identifiers
    # ----------------------------------------------------------------------
    workDir = (cfg["MAIN"].get("workDir", "") or "").strip()
    project = (cfg["MAIN"].get("project", "") or "").strip()
    ID      = (cfg["MAIN"].get("ID", "") or "").strip()

    if not workDir or not project or not ID:
        raise ValueError(f"MAIN fields must be set: workDir, project, ID.")

    # Base directory
    cairosDir = pathlib.Path(workDir) / project.strip("/") / ID.strip("/")
    cairosDir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------------
    # Define workflow subfolders (Steps 00–15 + support)
    # ----------------------------------------------------------------------
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

        # AvaDirectory chain
        ("avaDir",           "11_avaDirectoryData"),  # Step 13
        ("avaDirType",       "12_avaDirectory"),      # Step 14
        ("avaDirResults",    "12_avaDirectory"),      # Step 15
        ("avaDirIndex",      "12_avaDirectory"),      # cache/index file (.pkl)

        # Map/preview steps
        ("avaScenMaps",      "13_avaScenMaps"),
        ("avaScenPreview",   "14_avaScenPreview"),

        # Post-processing / support
        ("plots",            "91_plots"),
        ("gis",              "92_GIS"),
    ]

    # ----------------------------------------------------------------------
    # Create directories and build dictionary
    # ----------------------------------------------------------------------
    workFlowDir = {"cairosDir": str(cairosDir)}
    for flag, folder in steps:
        varName = f"{flag}Dir"
        dirPath = cairosDir / folder
        dirPath.mkdir(parents=True, exist_ok=True)
        workFlowDir[varName] = str(dirPath)

    # ----------------------------------------------------------------------
    # Logging summary
    # ----------------------------------------------------------------------
    log.info("cairosDir: %s", workFlowDir["cairosDir"])
    for key, path in workFlowDir.items():
        rel = os.path.relpath(path, start=workFlowDir["cairosDir"])
        log.info("...%s: ./%s", key, rel)

    return workFlowDir
