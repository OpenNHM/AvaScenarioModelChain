# cairos/runCairos.py

import logging
from logging.handlers import MemoryHandler
import pathlib
import os
import configparser
import time

import runInitWorkDir as initWorkDir
import in1Utils.cfgUtils as cfgUtils
import in1Utils.dataUtils as dataUtils
import in1Utils.workflowUtils as workflowUtils
import in2Parameter.compParams as compParams

import com1PRA.praDelineation as praDelineation
import com1PRA.praSelection as praSelection
import com1PRA.praSubCatchments as subCatchments
import com1PRA.praProcessing as praProcessing
import com1PRA.praSegmentation as praSegmentation
import com1PRA.praAssignElevSize as praAssignElevSize
import com1PRA.praPrepForFlowPy as praPrepForFlowPy
import com1PRA.praMakeBigDataStructure as praMakeBigDataStructure

# AvaFrame
from avaframe import runCom4FlowPy

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Step 00 - 12: MAIN DRIVER FOR CAIROS MODEL CHAIN
# ─────────────────────────────────────────────────────────────────────────────

def runCairos(workDir: str = ""):
    # choose config file
    modPath = os.getcwd()
    localFile = os.path.join(modPath, "local_cairosCfg.ini")
    defaultFile = os.path.join(modPath, "cairosCfg.ini")
    configPath = localFile if os.path.isfile(localFile) else defaultFile

    # ---- buffer ALL early logs; flush into file once workDir is known ----
    root_logger = logging.getLogger()
    _early_buf = MemoryHandler(capacity=10000, flushLevel=logging.CRITICAL)
    root_logger.addHandler(_early_buf)

    start_human = time.strftime("%Y-%m-%d %H:%M:%S")
    log.info(
        "\n\n       ============================================================================\n"
        f"          ... Start main driver for CAIROS model chain ({start_human}) ...\n"
        "       ============================================================================\n"
    )
    log.info("config file in command: %s", os.path.abspath(configPath))

    # -------- Step 00: Initializing project --------
    log.info("Step 00: Initializing project...")
    t0 = time.perf_counter()

    # update workDir in config before folder creation (if provided)
    if workDir:
        cfgTmp = configparser.ConfigParser()
        cfgTmp.read(configPath)
        if "MAIN" not in cfgTmp:
            cfgTmp["MAIN"] = {}
        cfgTmp["MAIN"]["workDir"] = workDir
        with open(configPath, "w") as f:
            cfgTmp.write(f)

    # sanity check MAIN
    cfgPreview = configparser.ConfigParser()
    cfgPreview.read(configPath)
    if "MAIN" not in cfgPreview:
        log.error("Config %s missing [MAIN] section.", configPath)
        try:
            root_logger.removeHandler(_early_buf); _early_buf.close()
        except Exception:
            pass
        return False

    main = cfgPreview["MAIN"]
    init_flag = main.getboolean("initWorkDir", fallback=False)
    work_dir  = (main.get("workDir", "") or "").strip()
    project   = (main.get("project", "") or "").strip()
    run_id    = (main.get("ID", "") or "").strip()

    if not init_flag:
        log.info("initWorkDir=False -> no directories created")
        try:
            root_logger.removeHandler(_early_buf); _early_buf.close()
        except Exception:
            pass
        return False
    if not work_dir or not project or not run_id:
        log.error("MAIN fields must be set (workDir, project, ID). Got workDir=%r project=%r ID=%r",
                  work_dir, project, run_id)
        try:
            root_logger.removeHandler(_early_buf); _early_buf.close()
        except Exception:
            pass
        return False

    # create folder structure
    workFlowDir = initWorkDir.initWorkDir(configPath)
    log.info("Step 00: Project initialized in %.2fs", time.perf_counter() - t0)

    # ---- attach a file logger in the workDir root and FLUSH early buffer ----
    try:
        log_dir = workFlowDir["cairosDir"]
        log_path = os.path.join(log_dir, f"runCairos_{time.strftime('%Y%m%d_%H%M%S')}.log")

        fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

        root_logger.addHandler(fh)
        _early_buf.setTarget(fh)
        _early_buf.flush()
        root_logger.removeHandler(_early_buf)
        _early_buf.close()

        log.info("Log file: %s", os.path.relpath(log_path, start=log_dir))
    except Exception:
        try:
            root_logger.removeHandler(_early_buf); _early_buf.close()
        except Exception:
            pass
        log.exception("Could not create/attach file logger in workDir.")

    # reload config for further steps
    cfg = cfgUtils.getConfig()
    if "WORKFLOW" not in cfg:
        log.error("Config missing [WORKFLOW] section.")
        return False
    workflowFlags = cfg["WORKFLOW"]

   # -------- Input presence check (after Step 00) --------
    inputDir = pathlib.Path(workFlowDir["inputDir"])
    demName = cfg["MAIN"].get("DEM", "").strip()
    forestName = cfg["MAIN"].get("FOREST", "").strip()
    boundaryName = cfg["MAIN"].get("BOUNDARY", "").strip()

    missing = []
    for label, fname in (("DEM", demName), ("FOREST", forestName), ("BOUNDARY", boundaryName)):
        if not fname:
            missing.append(f"{label}=<empty in INI>")
        else:
            fpath = inputDir / fname
            if not fpath.exists():
                missing.append(f"{label}={fname}")

    if missing:
        log.error("...Required input files are missing in ./%s:",
                  os.path.relpath(str(inputDir), start=workFlowDir["cairosDir"]))
        for m in missing:
            log.error("  - %s", m)
        log.error("\n\n          ... Please provide the required input files and run again ...\n")
        return False

    # --- Extra: enforce numeric nodata & CRS normalization for raster inputs ---
    for label, fname in (("DEM", demName), ("FOREST", forestName)):
        if not fname:
            continue
        fpath = inputDir / fname
        try:
            dataUtils.enforceNumericNoData(fpath, fallback=-9999.0, force_epsg=25832)
            log.info("Input %s validated: nodata + CRS check done", label)
        except Exception:
            log.exception("Failed to normalize %s: %s", label, fpath)
            return False

    log.info("All raster inputs validated: DEM + FOREST nodata/CRS checked and safe.")

    # Master flag: run all PRA steps if True
    masterPra = workflowFlags.getboolean("runAllPRASteps", fallback=False)

    # Master flag: run all FlowPy steps if True
    masterFlowPy = workflowFlags.getboolean("runAllFlowPySteps", fallback=False)




    log.info(
        "All inputs complete: %s/00_input\n\n"
        "       ============================================================================\n"
        "               ... LET'S KICK IT - AVALANCHE SCENARIOS in 3... 2... 1...\n"
        "       ============================================================================\n",
        workFlowDir["cairosDir"]
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Step 01 - 08: PRA PROCESSING
    # ─────────────────────────────────────────────────────────────────────────

    # Step 01: PRA delineation
    log.info("Step 01: Start PRA delineation...")
    t1 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praDelineation", masterPra, default=False):
        try:
            praDelineation.runPraDelineation(cfg, workFlowDir)
            log.info("Step 01: Finish PRA delineation in %.2fs", time.perf_counter() - t1)
        except Exception:
            log.exception("Step 01: PRA delineation failed")
            return False
    else:
        log.info("Step 01: ...PRA delineation skipped (flag is False)")

    # Step 02: PRA selection
    log.info("Step 02: Start PRA selection...")
    t2 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praSelection", masterPra, default=False):
        try:
            praDir = pathlib.Path(workFlowDir["praDelineationDir"])
            if not (praDir / "pra.tif").exists() or not (praDir / "aspect.tif").exists():
                log.error("Step 02 requires pra.tif and aspect.tif in 01_praDelineation; missing inputs.")
            else:
                praSelection.runPraSelection(cfg, workFlowDir)
                log.info("Step 02: Finish PRA selection in %.2fs", time.perf_counter() - t2)
        except Exception:
            log.exception("Step 02: PRA selection failed")
            return False
    else:
        log.info("Step 02: ...PRA selection skipped (flag is False)")

    # Step 03: Subcatchments
    log.info("Step 03: Start subcatchments...")
    t3 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praSubCatchments", masterPra, default=False):
        try:
            subCatchments.runSubcatchments(cfg, workFlowDir)
            log.info("Step 03: Finish subcatchments in %.2fs", time.perf_counter() - t3)
        except Exception:
            log.exception("Step 03: Subcatchments failed")
            return False
    else:
        log.info("Step 03: ...Subcatchments skipped (flag is False)")

    # Step 04: PRA processing
    log.info("Step 04: Start PRA processing...")
    t4 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praProcessing", masterPra, default=False):
        try:
            praProcessing.runPraProcessing(cfg, workFlowDir)
            log.info("Step 04: Finish PRA processing in %.2fs", time.perf_counter() - t4)
        except Exception:
            log.exception("Step 04: PRA processing failed")
            return False
    else:
        log.info("Step 04: ...PRA processing skipped (flag is False)")

    # Step 05: PRA segmentation
    log.info("Step 05: Start PRA segmentation...")
    t5 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praSegmentation", masterPra, default=False):
        try:
            praSegmentation.runPraSegmentation(cfg, workFlowDir)
            log.info("Step 05: Finish PRA segmentation in %.2fs", time.perf_counter() - t5)
        except Exception:
            log.exception("Step 05: PRA segmentation failed")
            return False
    else:
        log.info("Step 05: ...PRA segmentation skipped (flag is False)")

    # Step 06: PRA Assign Elev/Size
    log.info("Step 06: Start PRA assign elev/size...")
    t6 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praAssignElevSize", masterPra, default=False):
        try:
            praAssignElevSize.runPraAssignElevSize(cfg, workFlowDir)
            log.info("Step 06: Finish PRA assign elev/size in %.2fs", time.perf_counter() - t6)
        except Exception:
            log.exception("Step 06: PRA assign elev/size failed")
            return False
    else:
        log.info("Step 06: ...PRA assign elev/size skipped (flag is False)")

    # Step 07: PRA → FlowPy prep
    log.info("Step 07: Start PRA → FlowPy prep...")
    t7 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praPrepForFlowPy", masterPra, default=False):
        try:
            praPrepForFlowPy.runPraPrepForFlowPy(cfg, workFlowDir)
            log.info("Step 07: Finish PRA → FlowPy prep in %.2fs", time.perf_counter() - t7)
        except Exception:
            log.exception("Step 07: PRA → FlowPy prep failed")
            return False
    else:
        log.info("Step 07: ...PRA → FlowPy prep skipped (flag is False)")

    # Step 08: Make Big Data Structure
    log.info("Step 08: Start MakeBigDataStructure...")
    t8 = time.perf_counter()
    if workflowUtils.stepEnabled(workflowFlags, "praMakeBigDataStructure", masterPra, default=False):
        try:
            praMakeBigDataStructure.runPraMakeBigDataStructure(cfg, workFlowDir)
            log.info("Step 08: Finish MakeBigDataStructure in %.2fs", time.perf_counter() - t8)
        except Exception:
            log.exception("Step 08: MakeBigDataStructure failed")
            return False
    else:
        log.info("Step 08: ...MakeBigDataStructure skipped (flag is False)")

    # ─────────────────────────────────────────────────────────────────────────
    # Step 09 - 12: FlowPy Processing (+ optional parameterization)
    # ─────────────────────────────────────────────────────────────────────────

    avaDirs: list[pathlib.Path] = []

    # Step 09: Size dependent parameterization for FlowPy inputs
    log.info("Step 09: Start size dependent parameterization for FlowPy inputs...")
    if workflowUtils.stepEnabled(workflowFlags, "flowPyInputToSize", masterFlowPy, default=False):
        t9 = time.perf_counter()
        t9p = None

        # Discover leaves
        avaDirs = workflowUtils.discoverAvaDirs(cfg, workFlowDir)
        if not avaDirs:
            log.error("Step 09: No valid AvaFrame leaves found; cannot continue.")
            return False

        # --- Parameterization pass over all leaves ---
        t9p = time.perf_counter()

        # Resolve DEM path from INI once (global DEM, not per-leaf)
        demName = cfg["MAIN"].get("DEM", "").strip()
        demPath = pathlib.Path(workFlowDir["inputDir"]) / demName

        if not demPath.exists():
            log.error("Step 09: DEM not found at expected path: %s", demPath)
            return False

        for avaDir in avaDirs:
            relLeaf = os.path.relpath(str(avaDir), start=workFlowDir["cairosDir"])
            scen = avaDir.name.lower()
            cfgSize = cfg["avaSIZE"]

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

            log.info("Step 09: ...REL → ALPHA/UMAX/EXP for ./%s (scenario=%s)", relLeaf, scen)
            try:
                compParams.computeAndSaveParameters(
                    avaDir,
                    cfg["avaPARAMETER"],
                    cfgSize,
                    demOverride=demPath,   
                    compressFiles=False,
                )
            except Exception:
                log.exception("Step 09: Parameterization failed for leaf: %s", relLeaf)
                return False

        if t9p is not None:
            log.info(
                "Step 09: Finish parameterization for FlowPy inputs in %.2fs",
                time.perf_counter() - t9p
            )

    else:
        log.info("Step 09: ...Size dependent parameterization for FlowPy inputs skipped (flag is False)")


    # ─────────────────────────────────────────────────────────────────────────
    # Step 10: Run FlowPy (per enumerated leaf, with size + cleanup)
    # ─────────────────────────────────────────────────────────────────────────
    log.info("Step 10: Start FlowPy runs...")
    t10 = time.perf_counter()

    if workflowUtils.stepEnabled(workflowFlags, "flowPyRun", masterFlowPy, default=False):
        if not avaDirs:
            # rediscover if Step 09 was skipped
            avaDirs = workflowUtils.discoverAvaDirs(cfg, workFlowDir)

        if not avaDirs:
            log.error("Step 10: No avaDirs available (nothing discovered). Cannot run FlowPy.")
            return False

        doSize     = workflowUtils.stepEnabled(workflowFlags, "flowPyOutputToSize", masterFlowPy, default=False)
        doCompress = workflowUtils.stepEnabled(workflowFlags, "flowPyOutputCompress", masterFlowPy, default=False)
        delOG      = workflowUtils.stepEnabled(workflowFlags, "flowPyDOutputDeleteOGFiles", masterFlowPy, default=False)
        delTemp    = workflowUtils.stepEnabled(workflowFlags, "flowPyDeleteTempFolder", masterFlowPy, default=False)

        for avaDir in avaDirs:
            relLeaf = os.path.relpath(str(avaDir), start=workFlowDir["cairosDir"])
            log.info("Step 10: ...Start running FlowPy for: avaDir=./%s", relLeaf)
            t_leaf = time.perf_counter()

            try:
                runCom4FlowPy.main(avalancheDir=str(avaDir))

                log.info(
                    "Step 10: ...Finish FlowPy run for ./%s in %.2fs",
                    relLeaf, time.perf_counter() - t_leaf
                )

                # --- Optional Step 11: Output → Size ---
                if doSize:
                    try:
                        log.info("Step 11 [for leaf]: Start results → size")
                        compParams.computeAndSaveSize(pathlib.Path(avaDir), cfg["avaSIZE"])
                        log.info("Step 11 [for leaf]: Finish results → size")
                    except Exception:
                        log.exception("Step 11 [for leaf]: Results → size failed for leaf: %s", relLeaf)
                        return False

                # --- Optional Step 12: Compress & clean ---
                if doCompress:
                    try:
                        outDir = pathlib.Path(avaDir) / "Outputs"
                        log.info("Step 12 [for leaf]: Compress outputs/ for: ./%s", relLeaf)
                        dataUtils.tifCompress(outDir, delete_original=delOG)
                    except Exception:
                        log.exception("Step 12 [for leaf]: Compression failed for leaf: %s", relLeaf)
                        return False

                if delTemp:
                    try:
                        log.info("Step 12 [for leaf]: Delete temp data for: ./%s", relLeaf)
                        dataUtils.deleteTempFolder(pathlib.Path(avaDir))
                    except Exception:
                        log.exception("Step 12 [for leaf]: Deleting temp data failed for leaf: %s", relLeaf)
                        return False

            except Exception:
                log.exception("Step 10: FlowPy failed for leaf: %s", relLeaf)
                return False
    else:
        log.info("Step 10: ...FlowPy runs skipped (flag is False)")

    log.info("Step 10: Finish FlowPy runs in %.2fs", time.perf_counter() - t10)

    # ─────────────────────────────────────────────────────────────────────────
    # Step 13: Prep avaDirectoryType (GEOJSON Data BASE only res rel outlines + attributes)
    # ─────────────────────────────────────────────────────────────────────────




if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,   # default: only warnings/errors show
        format="%(levelname)s:%(name)s: %(message)s"
    )

    # Explicitly allow INFO logs from CAIROS main + key drivers
    logging.getLogger("__main__").setLevel(logging.INFO)
    logging.getLogger("runInitWorkDir").setLevel(logging.INFO)
    logging.getLogger("in2Parameter").setLevel(logging.INFO)
    logging.getLogger("in2Parameter.compParams").setLevel(logging.INFO)

    # Keep details from AvaFrame + helpers quiet unless WARN/ERROR
    logging.getLogger("in2Parameter.sizeParameters").setLevel(logging.WARNING)
    logging.getLogger("avaframe.com4FlowPy.splitAndMerge").setLevel(logging.WARNING)
    logging.getLogger("in1Utils.cfgUtils").setLevel(logging.WARNING)
    logging.getLogger("avaframe.in3Utils.cfgUtils").setLevel(logging.WARNING)
    logging.getLogger("avaframe.com4FlowPy.cfgUtils").setLevel(logging.WARNING)

    t_all = time.perf_counter()
    success = runCairos()
    if success:
        log.info(
            "\n\n       ============================================================================\n"
            "                 ... CAIROS WORKFLOW DONE - completed in %.2fs ...\n"
            "       ============================================================================\n",
            time.perf_counter() - t_all
        )
