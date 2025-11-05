# cairos/runCairos.py

import os
import sys
import time
import logging
import configparser
import pathlib
from logging.handlers import MemoryHandler

# ------------------ CAIROS core imports ------------------ #
import runInitWorkDir as initWorkDir
import in1Utils.cfgUtils as cfgUtils
import in1Utils.dataUtils as dataUtils
import in1Utils.workflowUtils as workflowUtils
import in2Parameter.compParams as compParams

# ------------------ Component imports ------------------ #
import com1PRA.praDelineation as praDelineation
import com1PRA.praSelection as praSelection
import com1PRA.praSubCatchments as subCatchments
import com1PRA.praProcessing as praProcessing
import com1PRA.praSegmentation as praSegmentation
import com1PRA.praAssignElevSize as praAssignElevSize
import com1PRA.praPrepForFlowPy as praPrepForFlowPy
import com1PRA.praMakeBigDataStructure as praMakeBigDataStructure

import com2AvaDirectory.avaDirectoryBuildFromFlowPy as avaDirectoryBuildFromFlowPy
import com2AvaDirectory.avaDirectoryType as avaDirectoryType

# ------------------ AvaFrame interface ------------------ #
from avaframe import runCom4FlowPy

# ------------------ Environment setup ------------------ #
from in1Utils.cfgUtils import setupGdalEnv
setupGdalEnv(verbose=True)

log = logging.getLogger(__name__)


# ------------------ Helper: close early MemoryHandler ------------------ #
def _closeEarlyBuffer(buf: MemoryHandler, root_logger: logging.Logger) -> None:
    try:
        root_logger.removeHandler(buf)
        buf.close()
    except Exception:
        pass


def _filterSingleTestDirs(cfg, dirs: list[pathlib.Path], stepLabel: str) -> list[pathlib.Path]:
    """Restrict FlowPy leaves to a single directory if makeSingleTestRun=True."""
    if not dirs:
        return dirs

    wf = cfg["WORKFLOW"]
    if not wf.getboolean("makeSingleTestRun", fallback=False):
        return dirs

    singleDir = wf.get("singleTestDir", "").strip()
    if not singleDir:
        log.warning("%s: makeSingleTestRun=True but no singleTestDir specified.", stepLabel)
        return dirs

    filtered = [d for d in dirs if singleDir in {d.name, d.parent.name, d.parent.parent.name}]
    if not filtered:
        log.warning("%s: singleTestDir '%s' not found among discovered leaves.", stepLabel, singleDir)
        return []

    log.info("%s: Single test mode → %s (%d leaves)", stepLabel, singleDir, len(filtered))
    return filtered


# ─────────────────────────────────────────────────────────────────────────────
# Step 00 - 15: MAIN DRIVER FOR CAIROS MODEL CHAIN
# ─────────────────────────────────────────────────────────────────────────────

def runCairos(workDir: str = "") -> bool:
    # choose config file
    modPath = os.getcwd()
    localFile = os.path.join(modPath, "local_cairosCfg.ini")
    defaultFile = os.path.join(modPath, "cairosCfg.ini")
    configPath = localFile if os.path.isfile(localFile) else defaultFile

    # ---- buffer ALL early logs; flush into file once workDir is known ----
    root_logger = logging.getLogger()
    early_buf = MemoryHandler(capacity=10000, flushLevel=logging.CRITICAL)
    root_logger.addHandler(early_buf)

    start_human = time.strftime("%Y-%m-%d %H:%M:%S")
    log.info(
        "\n\n       ============================================================================\n"
        f"          ... Start main driver for CAIROS model chain ({start_human}) ...\n"
        "       ============================================================================\n"
    )
    log.info("Config file: %s", os.path.abspath(configPath))

    # ------------------ Step 00: Initializing project ------------------ #
    log.info("Step 00: Initializing project...")
    t0 = time.perf_counter()

    # update workDir in config before folder creation (if provided)
    if workDir:
        cfgTmp = configparser.ConfigParser()
        cfgTmp.read(configPath)
        cfgTmp.setdefault("MAIN", {})
        cfgTmp["MAIN"]["workDir"] = workDir
        with open(configPath, "w") as f:
            cfgTmp.write(f)

    # sanity check MAIN
    cfgPreview = configparser.ConfigParser()
    cfgPreview.read(configPath)
    if "MAIN" not in cfgPreview:
        log.error("Step 00: Config %s missing [MAIN] section.", configPath)
        _closeEarlyBuffer(early_buf, root_logger)
        return False

    main = cfgPreview["MAIN"]
    init_flag = main.getboolean("initWorkDir", fallback=False)
    work_dir = (main.get("workDir", "") or "").strip()
    project = (main.get("project", "") or "").strip()
    run_id = (main.get("ID", "") or "").strip()

    if not init_flag:
        log.info("Step 00: initWorkDir=False → no directories created.")
        _closeEarlyBuffer(early_buf, root_logger)
        return False
    if not work_dir or not project or not run_id:
        log.error(
            "Step 00: MAIN fields must be set (workDir, project, ID). "
            "Got workDir=%r project=%r ID=%r",
            work_dir,
            project,
            run_id,
        )
        _closeEarlyBuffer(early_buf, root_logger)
        return False

    # create folder structure
    workFlowDir = initWorkDir.initWorkDir(configPath)
    log.info("Step 00: Project initialized in %.2fs", time.perf_counter() - t0)

    # ---- attach a file logger in the workDir root and FLUSH early buffer ----
    try:
        log_dir = workFlowDir["cairosDir"]
        log_path = os.path.join(
            log_dir,
            f"runCairos_{time.strftime('%Y%m%d_%H%M%S')}.log",
        )

        fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

        root_logger.addHandler(fh)
        early_buf.setTarget(fh)
        early_buf.flush()
        _closeEarlyBuffer(early_buf, root_logger)

        log.info("Step 00: Log file: %s", os.path.relpath(log_path, start=log_dir))
    except Exception:
        _closeEarlyBuffer(early_buf, root_logger)
        log.exception("Step 00: Could not create/attach file logger in workDir.")
        return False

    # reload config for further steps
    cfg = cfgUtils.getConfig()
    if "WORKFLOW" not in cfg:
        log.error("Step 00: Config missing [WORKFLOW] section.")
        return False
    workflowFlags = cfg["WORKFLOW"]

    # -------- Step 00b: Input presence check --------
    inputDir = pathlib.Path(workFlowDir["inputDir"])
    demName = cfg["MAIN"].get("DEM", "").strip()
    forestName = cfg["MAIN"].get("FOREST", "").strip()
    boundaryName = cfg["MAIN"].get("BOUNDARY", "").strip()

    missing = []
    for label, fname in (
        ("DEM", demName),
        ("FOREST", forestName),
        ("BOUNDARY", boundaryName),
    ):
        if not fname:
            missing.append(f"{label}=<empty in INI>")
        else:
            fpath = inputDir / fname
            if not fpath.exists():
                missing.append(f"{label}={fname}")

    if missing:
        log.error(
            "Step 00: Required input files are missing in ./%s:",
            os.path.relpath(str(inputDir), start=workFlowDir["cairosDir"]),
        )
        for m in missing:
            log.error("  - %s", m)
        log.error(
            "\n\n          ... Please provide the required input files and run again ...\n"
        )
        return False

    # --- Extra: enforce numeric nodata & CRS normalization for raster inputs ---
    for label, fname in (("DEM", demName), ("FOREST", forestName)):
        if not fname:
            continue
        fpath = inputDir / fname
        try:
            dataUtils.enforceNumericNoData(
                fpath,
                fallback=-9999.0,
                force_epsg=25832,
            )
            log.info("Step 00: Input %s validated: nodata + CRS check done.", label)
        except Exception:
            log.exception("Step 00: Failed to normalize %s: %s", label, fpath)
            return False

    log.info(
        "Step 00: All raster inputs validated: DEM + FOREST nodata/CRS checked and safe."
    )

    # Master flags
    masterPra = workflowFlags.getboolean("runAllPRASteps", fallback=False)
    masterFlowPy = workflowFlags.getboolean("runAllFlowPySteps", fallback=False)
    masterAvaDir = workflowFlags.getboolean("runAllAvaDirectorySteps", fallback=False)

    log.info(
        "All inputs complete: %s/00_input\n\n"
        "       ============================================================================\n"
        "               ... LET'S KICK IT - AVALANCHE SCENARIOS in 3... 2... 1...\n"
        "       ============================================================================\n",
        workFlowDir["cairosDir"],
    )

    # Collect per-step run times
    stepStats: dict[str, float] = {}

    # ─────────────────────────────────────────────────────────────────────────
    # Step 01 - 08: PRA PROCESSING
    # ─────────────────────────────────────────────────────────────────────────

    # Step 01: PRA delineation
    t1 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praDelineation", masterPra, default=False):
        log.info("Step 01: Start PRA delineation...")
        try:
            praDelineation.runPraDelineation(cfg, workFlowDir)
            stepStats["Step 01"] = time.perf_counter() - t1
            log.info(
                "Step 01: Finish PRA delineation in %.2fs", stepStats["Step 01"]
            )
        except Exception:
            log.exception("Step 01: PRA delineation failed.")
            return False
    else:
        log.info("Step 01: ...PRA delineation skipped (flag is False)")

    # Step 02: PRA selection
    t2 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praSelection", masterPra, default=False):
        log.info("Step 02: Start PRA selection...")
        try:
            praDir = pathlib.Path(workFlowDir["praDelineationDir"])
            if not (praDir / "pra.tif").exists() or not (praDir / "aspect.tif").exists():
                log.error(
                    "Step 02: Requires pra.tif and aspect.tif in 01_praDelineation; missing inputs."
                )
            else:
                maskComm = False
                if "praSELECTION" in cfg:
                    selCfg = cfg["praSELECTION"]
                    maskComm = selCfg.getboolean("maskCommRegion", fallback=False)

                if maskComm:
                    commFile = cfg["MAIN"].get("COMMISSIONREGION", "").strip()
                    log.info(
                        "Step 02: ...maskCommRegion=True → using COMMISSIONREGION=%s",
                        commFile or "<missing>",
                    )

                praSelection.runPraSelection(cfg, workFlowDir)
                stepStats["Step 02"] = time.perf_counter() - t2
                log.info(
                    "Step 02: Finish PRA selection in %.2fs", stepStats["Step 02"]
                )
        except Exception:
            log.exception("Step 02: PRA selection failed.")
            return False
    else:
        log.info("Step 02: ...PRA selection skipped (flag is False)")

    # Step 03: Subcatchments
    t3 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praSubCatchments", masterPra, default=False):
        log.info("Step 03: Start subcatchments...")
        try:
            subCatchments.runSubcatchments(cfg, workFlowDir)
            stepStats["Step 03"] = time.perf_counter() - t3
            log.info(
                "Step 03: Finish subcatchments in %.2fs", stepStats["Step 03"]
            )
        except Exception:
            log.exception("Step 03: Subcatchments failed.")
            return False
    else:
        log.info("Step 03: ...Subcatchments skipped (flag is False)")

    # Step 04: PRA processing
    t4 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praProcessing", masterPra, default=False):
        log.info("Step 04: Start PRA processing...")
        try:
            praProcessing.runPraProcessing(cfg, workFlowDir)
            stepStats["Step 04"] = time.perf_counter() - t4
            log.info(
                "Step 04: Finish PRA processing in %.2fs", stepStats["Step 04"]
            )
        except Exception:
            log.exception("Step 04: PRA processing failed.")
            return False
    else:
        log.info("Step 04: ...PRA processing skipped (flag is False)")

    # Step 05: PRA segmentation
    t5 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praSegmentation", masterPra, default=False):
        log.info("Step 05: Start PRA segmentation...")
        try:
            praSegmentation.runPraSegmentation(cfg, workFlowDir)
            stepStats["Step 05"] = time.perf_counter() - t5
            log.info(
                "Step 05: Finish PRA segmentation in %.2fs", stepStats["Step 05"]
            )
        except Exception:
            log.exception("Step 05: PRA segmentation failed.")
            return False
    else:
        log.info("Step 05: ...PRA segmentation skipped (flag is False)")

    # Step 06: PRA Assign Elev/Size
    t6 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praAssignElevSize", masterPra, default=False):
        log.info("Step 06: Start PRA assign elev/size...")
        try:
            praAssignElevSize.runPraAssignElevSize(cfg, workFlowDir)
            stepStats["Step 06"] = time.perf_counter() - t6
            log.info(
                "Step 06: Finish PRA assign elev/size in %.2fs", stepStats["Step 06"]
            )
        except Exception:
            log.exception("Step 06: PRA assign elev/size failed.")
            return False
    else:
        log.info("Step 06: ...PRA assign elev/size skipped (flag is False)")

    # Step 07: PRA → FlowPy prep
    t7 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praPrepForFlowPy", masterPra, default=False):
        log.info("Step 07: Start PRA → FlowPy preparation...")
        try:
            praPrepForFlowPy.runPraPrepForFlowPy(cfg, workFlowDir)
            stepStats["Step 07"] = time.perf_counter() - t7
            log.info(
                "Step 07: Finish PRA → FlowPy preparation in %.2fs",
                stepStats["Step 07"],
            )
        except Exception:
            log.exception("Step 07: PRA → FlowPy preparation failed.")
            return False
    else:
        log.info("Step 07: ...PRA → FlowPy prep skipped (flag is False)")

    # Step 08: Make Big Data Structure
    t8 = time.perf_counter()
    if workflowUtils.stepEnabled(
        workflowFlags, "praMakeBigDataStructure", masterPra, default=False
    ):
        log.info("Step 08: Start MakeBigDataStructure...")
        try:
            praMakeBigDataStructure.runPraMakeBigDataStructure(cfg, workFlowDir)
            stepStats["Step 08"] = time.perf_counter() - t8
            log.info(
                "Step 08: Finish MakeBigDataStructure in %.2fs", stepStats["Step 08"]
            )
        except Exception:
            log.exception("Step 08: MakeBigDataStructure failed.")
            return False
    else:
        log.info("Step 08: ...MakeBigDataStructure skipped (flag is False)")

    # ─────────────────────────────────────────────────────────────────────────
    # Step 09 - 12: FlowPy Processing (+ optional parameterization)
    # ─────────────────────────────────────────────────────────────────────────

    avaDirs: list[pathlib.Path] = []

    # Step 09: Size dependent parameterization for FlowPy inputs
    t9 = time.perf_counter()
    log.info("Step 09: Start size dependent parameterization for FlowPy inputs...")
    if workflowUtils.stepEnabled(workflowFlags, "flowPyInputToSize", masterFlowPy, default=False):
        # Discover leaves
        avaDirs = workflowUtils.discoverAvaDirs(cfg, workFlowDir)
        if not avaDirs:
            log.error("Step 09: No valid AvaFrame leaves found; cannot continue.")
            return False

        # Optional single-test filtering
        avaDirs = _filterSingleTestDirs(cfg, avaDirs, "Step 09")
        if not avaDirs:
            return False

        # Resolve DEM path from INI once (global DEM, not per-leaf)
        demName = cfg["MAIN"].get("DEM", "").strip()
        demPath = pathlib.Path(workFlowDir["inputDir"]) / demName

        if not demPath.exists():
            log.error("Step 09: DEM not found at expected path: %s", demPath)
            return False

        for avaDir in avaDirs:
            relLeaf = os.path.relpath(str(avaDir), start=workFlowDir["cairosDir"])
            scen = avaDir.name.lower()

            # --- Make a fresh ConfigParser with a copy of avaSIZE ---
            cfgSize = configparser.ConfigParser()
            cfgSize["avaSIZE"] = dict(cfg["avaSIZE"])
            cfgSize = cfgSize["avaSIZE"]  # SectionProxy with .get(), .getfloat(), etc.

            size_parent = avaDir.parent.name.lower()
            if size_parent.startswith("size"):
                try:
                    sizeN = int(size_parent[4:])
                    cfgSize["sizeMax"] = str(sizeN)
                except ValueError:
                    pass

            if scen == "dry":
                cfgSize["constantTemperature"] = "True"
                cfgSize["Tcons"] = cfgSize.get("TCold", cfgSize.get("Tcons", "0"))
            elif scen == "wet":
                cfgSize["constantTemperature"] = "True"
                cfgSize["Tcons"] = cfgSize.get("TWarm", cfgSize.get("Tcons", "0"))

            log.info(
                "Step 09: ...REL → ALPHA/UMAX/EXP for ./%s (scenario=%s)",
                relLeaf,
                scen,
            )
            try:
                compParams.computeAndSaveParameters(
                    avaDir,
                    cfg["avaPARAMETER"],
                    cfgSize,
                    demOverride=demPath,
                    compressFiles=False,
                )
            except Exception:
                log.exception(
                    "Step 09: Parameterization failed for leaf: %s", relLeaf
                )
                return False

        stepStats["Step 09"] = time.perf_counter() - t9
        log.info(
            "Step 09: Finish size dependent parameterization for FlowPy inputs in %.2fs",
            stepStats["Step 09"],
        )
    else:
        log.info(
            "Step 09: ...Size dependent parameterization for FlowPy inputs skipped (flag is False)"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Step 10: Run FlowPy (per enumerated leaf, with size + cleanup)
    # ─────────────────────────────────────────────────────────────────────────
    t10 = time.perf_counter()
    log.info("Step 10: Start FlowPy runs...")

    if workflowUtils.stepEnabled(workflowFlags, "flowPyRun", masterFlowPy, default=False):
        if not avaDirs:
            # rediscover if Step 09 was skipped
            avaDirs = workflowUtils.discoverAvaDirs(cfg, workFlowDir)

        if not avaDirs:
            log.error(
                "Step 10: No avaDirs available (nothing discovered). Cannot run FlowPy."
            )
            return False

        # Optional single-test filtering
        avaDirs = _filterSingleTestDirs(cfg, avaDirs, "Step 10")
        if not avaDirs:
            return False

        doSize = workflowUtils.stepEnabled(
            workflowFlags, "flowPyOutputToSize", masterFlowPy, default=False
        )
        doCompress = workflowUtils.stepEnabled(
            workflowFlags, "flowPyOutputCompress", masterFlowPy, default=False
        )
        delOG = workflowUtils.stepEnabled(
            workflowFlags, "flowPyDOutputDeleteOGFiles", masterFlowPy, default=False
        )
        delTemp = workflowUtils.stepEnabled(
            workflowFlags, "flowPyDeleteTempFolder", masterFlowPy, default=False
        )

        for avaDir in avaDirs:
            relLeaf = os.path.relpath(str(avaDir), start=workFlowDir["cairosDir"])
            log.info("Step 10: ...Start running FlowPy for: avaDir=./%s", relLeaf)
            t_leaf = time.perf_counter()

            try:
                # --- Backup handlers ---
                root_logger = logging.getLogger()
                handlers_backup = list(root_logger.handlers)

                # --- Attach extra file handler so FlowPy logs also go to CAIROS logfile ---
                file_handlers = [
                    h for h in handlers_backup if isinstance(h, logging.FileHandler)
                ]
                flowpy_handler = None
                if file_handlers:
                    fh = file_handlers[0]
                    flowpy_handler = logging.FileHandler(
                        fh.baseFilename, mode="a", encoding="utf-8"
                    )
                    flowpy_handler.setLevel(logging.INFO)
                    flowpy_handler.setFormatter(
                        logging.Formatter(
                            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                        )
                    )
                    root_logger.addHandler(flowpy_handler)

                # --- Run FlowPy (its logs will appear in console + logfile) ---
                runCom4FlowPy.main(avalancheDir=str(avaDir))

                # --- Remove temp FlowPy handler ---
                if flowpy_handler:
                    root_logger.removeHandler(flowpy_handler)
                    flowpy_handler.close()

                # --- Restore original handlers ---
                root_logger.handlers = handlers_backup

                log.info(
                    "Step 10: ...Finish FlowPy run for ./%s in %.2fs",
                    relLeaf,
                    time.perf_counter() - t_leaf,
                )

                # --- Optional Step 11: Output → Size ---
                if doSize:
                    try:
                        log.info("Step 11 [for leaf]: Start results → size...")
                        compParams.computeAndSaveSize(pathlib.Path(avaDir), cfg["avaSIZE"])
                        log.info("Step 11 [for leaf]: Finish results → size.")
                    except Exception:
                        log.exception(
                            "Step 11 [for leaf]: Results → size failed for leaf: %s",
                            relLeaf,
                        )
                        return False

                # --- Optional Step 12: Compress & clean ---
                if doCompress:
                    try:
                        outDir = pathlib.Path(avaDir) / "Outputs"
                        log.info(
                            "Step 12 [for leaf]: Compress outputs/ for: ./%s",
                            relLeaf,
                        )
                        dataUtils.tifCompress(outDir, delete_original=delOG)
                    except Exception:
                        log.exception(
                            "Step 12 [for leaf]: Compression failed for leaf: %s",
                            relLeaf,
                        )
                        return False

                if delTemp:
                    try:
                        log.info(
                            "Step 12 [for leaf]: Delete temp data for: ./%s", relLeaf
                        )
                        dataUtils.deleteTempFolder(pathlib.Path(avaDir))
                    except Exception:
                        log.exception(
                            "Step 12 [for leaf]: Deleting temp data failed for leaf: %s",
                            relLeaf,
                        )
                        return False

            except Exception:
                log.exception("Step 10: FlowPy failed for leaf: %s", relLeaf)
                return False

        stepStats["Step 10"] = time.perf_counter() - t10
        log.info(
            "Step 10: Finish FlowPy runs in %.2fs",
            stepStats["Step 10"],
        )
    else:
        log.info("Step 10: ...FlowPy runs skipped (flag is False)")

    # ─────────────────────────────────────────────────────────────────────────
    # Step 13–15: Avalanche Directory Builder (ADB full chain)
    # ─────────────────────────────────────────────────────────────────────────


    # Step 13: Avalanche Directory Build from FlowPy
    t13 = time.perf_counter()
    log.info("Step 13: Start Avalanche Directory Build from FlowPy...")

    # Awareness of global single-test mode
    if workflowFlags.getboolean("makeSingleTestRun", fallback=False):
        singleDir = workflowFlags.get("singleTestDir", "").strip()
        log.info(
            "Step 13: Single-test mode is ON (singleTestDir=%s). "
            "AvaDirectory build will restrict processing to this PRA directory.",
            singleDir or "<not set>",
        )
    else:
        log.info("Step 13: Global single-test mode is OFF → processing all PRA directories.")

    try:
        # Execute step if enabled individually or via runAllAvaDirectorySteps
        if (
            workflowUtils.stepEnabled(
                workflowFlags, "avaDirectoryBuildFromFlowPy", masterAvaDir, default=False
            )
            or workflowFlags.getboolean("runAllAvaDirectorySteps", fallback=False)
        ):
            import com2AvaDirectory.avaDirectoryBuildFromFlowPy as avaDirectoryBuildFromFlowPy

            avaDirectoryBuildFromFlowPy.runAvaDirectoryBuildFromFlowPy(cfg, workFlowDir)

            stepStats["Step 13"] = time.perf_counter() - t13
            log.info(
                "Step 13: Avalanche Directory Build from FlowPy finished successfully in %.2fs",
                stepStats["Step 13"],
            )
        else:
            log.info(
                "Step 13: Avalanche Directory Build from FlowPy skipped (flag set to False)."
            )

    except Exception:
        log.exception("Step 13: Avalanche Directory Build from FlowPy failed.")
        return False


    # Step 14: Avalanche Directory Type
    t14 = time.perf_counter()
    log.info("Step 14: Start Avalanche Directory Type Build...")

    if workflowFlags.getboolean("makeSingleTestRun", fallback=False):
        singleDir = workflowFlags.get("singleTestDir", "").strip()
        log.info(
            "Step 14: Single test mode is ON (singleTestDir=%s). "
            "Avalanche Directory Type is still computed for the full AvaDirectory.",
            singleDir or "<not set>",
        )

    try:
        if workflowUtils.stepEnabled(
            workflowFlags, "avaDirectoryType", masterAvaDir, default=False
        ):
            avaDirectoryType.runAvaDirectoryType(cfg, workFlowDir)
            stepStats["Step 14"] = time.perf_counter() - t14
            log.info(
                "Step 14: Avalanche Directory Type Build finished successfully in %.2fs",
                stepStats["Step 14"],
            )
        else:
            log.info(
                "Step 14: ...Avalanche Directory Type Build skipped (flag set to False)"
            )
    except Exception:
        log.exception("Step 14: Avalanche Directory Type Build failed.")
        return False

    # Step 15: AvaDirectory Results
    t15 = time.perf_counter()
    log.info("Step 15: Start AvaDirectory Results Build...")

    try:
        if workflowUtils.stepEnabled(workflowFlags, "avaDirectoryResults", masterAvaDir, default=False):
            import com2AvaDirectory.avaDirectoryResults as avaDirectoryResults
            avaDirectoryResults.runAvaDirectoryResults(cfg, workFlowDir)
            stepStats["Step 15"] = time.perf_counter() - t15
            log.info(
                "Step 15: AvaDirectory Results Build finished successfully in %.2fs",
                stepStats["Step 15"],
            )
        else:
            log.info("Step 15: ...AvaDirectory Results Build skipped (flag set to False)")
    except Exception:
        log.exception("Step 15: AvaDirectory Results Build failed.")
        return False


    # ------------------ FINAL SUMMARY ------------------ #
    total = sum(stepStats.values())
    log.info("\n\nCAIROS Workflow Summary\n")
    for s, dur in stepStats.items():
        log.info("%-12s ✅ %.2fs", s, dur)
    log.info(f"Total runtime:  {total:.2f}s")

    return True


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s:%(name)s: %(message)s",
    )

    # Explicitly enable INFO logging for CAIROS main pieces
    for name in [
        "__main__",
        "runCairos",
        "runInitWorkDir",
        "com2AvaDirectory.avaDirectoryBuildFromFlowPy",
        "in2Parameter",
        "in2Parameter.compParams",
    ]:
        logging.getLogger(name).setLevel(logging.INFO)

    # Silence noisy AvaFrame / utils internals
    for name in [
        "in2Parameter.sizeParameters",
        "avaframe.com4FlowPy.splitAndMerge",
        "in1Utils.cfgUtils",
        "avaframe.in3Utils.cfgUtils",
        "avaframe.com4FlowPy.cfgUtils",
    ]:
        logging.getLogger(name).setLevel(logging.WARNING)

    t_all = time.perf_counter()
    success = runCairos()
    if success:
        log.info(
            "\n\n       ============================================================================\n"
            "                 ... CAIROS WORKFLOW DONE - completed in %.2fs ...\n"
            "       ============================================================================\n",
            time.perf_counter() - t_all,
        )
