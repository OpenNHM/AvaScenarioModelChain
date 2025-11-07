#
# ───────────────────────────────────────────────────────────────────────────────────────────────
#
#   ██████╗  ██╗  ██╗ █████╗     ███████╗  ██████╗ ███████╗ ███╗   ██╗
#   ██╔══██╗ ██╗  ██║ ██╔══██╗   ██╔════╝ ██╔════╝ ██╔════╝ ████╗  ██║
#   ███████║ ██║ ██╔╝ ███████║   ███████╗ ██║      █████╗   ██╔██╗ ██║
#   ██╔══██║ ██║██╔╝  ██╔══██║   ╚════██║ ██║      ██╔══╝   ██║╚██╗██║
#   ██║  ██║ ╚███╔╝   ██║  ██║   ███████║ ╚██████╗ ███████╗ ██║ ╚████║
#   ╚═╝  ╚═╝  ╚══╝    ╚═╝  ╚═╝   ╚══════╝  ╚═════╝ ╚══════╝ ╚═╝  ╚═══╝
# ───────────────────────────────────────────────────────────────────────────────────────────────
#    A V A L A N C H E · S C E N E N A R I O · M O D E L · C H A I N
# ───────────────────────────────────────────────────────────────────────────────────────────────



# > runCairos.py <

# Purpose : Master orchestrator for the CAIROS Avalanche Model Chain (Steps 00–15)
#            Drives the end-to-end avalanche scenario workflow:
#            PRA delineation → FlowPy simulation → AvaDirectory compilation.
#
# Inputs  : - local_cairosCfg.ini / cairosCfg.ini (project configuration)
#           - 00_input/ directory containing DEM, FOREST, and BOUNDARY files
#
# Outputs : Structured scenario directories, FlowPy outputs,
#            and compiled AvaDirectory datasets ready for visualization or analysis.
#
# Config  : [MAIN] project metadata & paths
#           [WORKFLOW] step activation flags
#           [pra*]  PRA preprocessing parameters (Steps 01–08)
#           [ava*]  Avalanche simulation and directory configuration (Steps 09–15)
#
# Consumes: Step modules from:
#              com1PRA/           – PRA delineation and preprocessing (Steps 01–08)
#              com2AvaDirectory/  – FlowPy result aggregation and AvaDirectory (Steps 13–15)
#
# Depends on: 
#     in1Utils.cfgUtils           – configuration and GDAL environment setup
#     in1Utils.dataUtils          – raster/vector I/O and compression utilities
#     in1Utils.workflowUtils      – orchestration helpers:
#                                   • stepEnabled()          → unified flag control
#                                   • closeEarlyBuffer()     → safe MemoryHandler cleanup
#                                   • validateInputs()       → input presence and CRS checks
#                                   • filterSingleTestDirs() → single-leaf test mode
#                                   • runStep()              → generic step executor with logging
#                                   • stepTimer()            → context-manager timing
#
# Provides: 
#     Complete automated execution of the CAIROS Avalanche Model Chain,
#     generating PRA data, FlowPy simulations, and AvaDirectory results
#     with unified logging, timing, and resumable workflow control.
#
# Execution:
#     pixi run -e dev cairos
#     or python runCairos.py
#
# ───────────────────────────────────────────────────────────────────────────────────────────────

import os
import time
import logging
import configparser
import pathlib
from logging.handlers import MemoryHandler

# ------------------ CAIROS core imports ------------------ #
import runInitWorkDir as initWorkDir
import in1Utils.cfgUtils as cfgUtils
import in1Utils.workflowUtils as workflowUtils
import in1Utils.dataUtils as dataUtils
import in2Parameter.compParams as compParams

# ------------------ Component imports -------------------- #
import com1PRA.praDelineation as praDelineation
import com1PRA.praSelection as praSelection
import com1PRA.praSubCatchments as subCatchments
import com1PRA.praProcessing as praProcessing
import com1PRA.praSegmentation as praSegmentation
import com1PRA.praAssignElevSize as praAssignElevSize
import com1PRA.praPrepForFlowPy as praPrepForFlowPy
import com1PRA.praMakeBigDataStructure as praMakeBigDataStructure

import com2AvaDirectory.avaDirBuildFromFlowPy as avaDirBuildFromFlowPy
import com2AvaDirectory.avaDirType as avaDirType
import com2AvaDirectory.avaDirResults as avaDirResults

# ------------------ AvaFrame interface ------------------ #
from avaframe import runCom4FlowPy

# ------------------ Environment setup ------------------- #
from in1Utils.cfgUtils import setupGdalEnv
setupGdalEnv(verbose=True)

log = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────────────────────────────────────────
# MAIN DRIVER FUNCTION
# ───────────────────────────────────────────────────────────────────────────────────────────────

def runCairos(workDir: str = "") -> bool:
    # -------------------------------------------------------------------------
    # Step 00: Initialization -------------------------------------------------
    # -------------------------------------------------------------------------
    modPath = os.getcwd()
    localFile = os.path.join(modPath, "local_cairosCfg.ini")
    configPath = localFile if os.path.isfile(localFile) else os.path.join(modPath, "cairosCfg.ini")

    root_logger = logging.getLogger()
    early_buf = MemoryHandler(capacity=10000, flushLevel=logging.CRITICAL)
    root_logger.addHandler(early_buf)

    # Log header (as before, single INFO entry)
    log.info(
        "\n\n"
        "       ============================================================================\n"
        f"          ... Start main driver for CAIROS model chain ({time.strftime('%Y-%m-%d %H:%M:%S')}) ...\n"
        "       ============================================================================\n"
    )
    log.info("Config file: %s", os.path.abspath(configPath))

    # --- Update config if workDir provided ---
    if workDir:
        cfgTmp = configparser.ConfigParser()
        cfgTmp.read(configPath)
        cfgTmp.setdefault("MAIN", {})
        cfgTmp["MAIN"]["workDir"] = workDir
        with open(configPath, "w") as f:
            cfgTmp.write(f)

    # --- Initialize work directory ---
    cfgPreview = configparser.ConfigParser()
    cfgPreview.read(configPath)
    if "MAIN" not in cfgPreview:
        log.error("Step 00: Config missing [MAIN] section.")
        workflowUtils.closeEarlyBuffer(early_buf, root_logger)
        return False

    main = cfgPreview["MAIN"]
    if not main.getboolean("initWorkDir", fallback=False):
        log.info("Step 00: initWorkDir=False → no directories created.")
        workflowUtils.closeEarlyBuffer(early_buf, root_logger)
        return False

    workFlowDir = initWorkDir.initWorkDir(configPath)
    log.info("Step 00: Project initialized in %.2fs", time.perf_counter())

    # --- Attach log file ---
    log_dir = workFlowDir["cairosDir"]
    log_path = os.path.join(log_dir, f"runCairos_{time.strftime('%Y%m%d_%H%M%S')}.log")
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root_logger.addHandler(fh)
    early_buf.setTarget(fh)
    early_buf.flush()
    workflowUtils.closeEarlyBuffer(early_buf, root_logger)
    log.info("Step 00: Log file created at %s", os.path.relpath(log_path, start=log_dir))

    # --- Load full config ---
    cfg = cfgUtils.getConfig()
    if "WORKFLOW" not in cfg:
        log.error("Step 00: Missing [WORKFLOW] section in config.")
        return False
    workflowFlags = cfg["WORKFLOW"]

    # --- Validate inputs ---
    if not workflowUtils.validateInputs(cfg, workFlowDir):
        return False

    # --- Master flags ---
    masterPra = workflowFlags.getboolean("runAllPRASteps", fallback=False)
    masterFlowPy = workflowFlags.getboolean("runAllFlowPySteps", fallback=False)
    masterAvaDir = workflowFlags.getboolean("runAllAvaDirSteps", fallback=False)

    stepStats: dict[str, float] = {}

    # --- Kickoff banner ---
    log.info(
        "All inputs complete: %s/00_input\n\n"
        "       ============================================================================\n"
        "               ... LET'S KICK IT - AVALANCHE SCENARIOS in 3... 2... 1...\n"
        "       ============================================================================\n",
        workFlowDir["cairosDir"],
    )




    # ───────────────────────────────────────────────────────────────────────────────────────────
    # Step 01–08: PRA Processing
    # ───────────────────────────────────────────────────────────────────────────────────────────

    praSteps = [
        ("01", "PRA delineation", praDelineation.runPraDelineation),
        ("02", "PRA selection", praSelection.runPraSelection),
        ("03", "Subcatchments", subCatchments.runSubcatchments),
        ("04", "PRA processing", praProcessing.runPraProcessing),
        ("05", "PRA segmentation", praSegmentation.runPraSegmentation),
        ("06", "PRA assign elevation & size", praAssignElevSize.runPraAssignElevSize),
        ("07", "PRA → FlowPy preparation", praPrepForFlowPy.runPraPrepForFlowPy),
        ("08", "Make Big Data Structure", praMakeBigDataStructure.runPraMakeBigDataStructure),
    ]
    for stepKey, label, func in praSteps:
        if not workflowUtils.runStep(stepKey, label, func, cfg, workFlowDir, stepStats, workflowFlags, masterPra):
            return False

    # ───────────────────────────────────────────────────────────────────────────────────────────
    # Step 09–12: Avalanche intensity and runout modelling
    # ───────────────────────────────────────────────────────────────────────────────────────────

    # -------------------------------------------------------------------------
    # Step 09: Size dependent parametrization
    # -------------------------------------------------------------------------

    avaDirs: list[pathlib.Path] = []
    if workflowUtils.stepEnabled(workflowFlags, "flowPyInputToSize", masterFlowPy):
        t9 = time.perf_counter()
        log.info("Step 09: Start size-dependent FlowPy parameterization...")
        try:
            avaDirs = workflowUtils.discoverAvaDirs(cfg, workFlowDir)
            avaDirs = workflowUtils.filterSingleTestDirs(cfg, avaDirs, "Step 09")

            demName = cfg["MAIN"].get("DEM", "").strip()
            demPath = pathlib.Path(workFlowDir["inputDir"]) / demName
            if not demPath.exists():
                log.error("Step 09: DEM missing at %s", demPath)
                return False

            for avaDir in avaDirs:
                relLeaf = os.path.relpath(avaDir, workFlowDir["cairosDir"])
                scen = avaDir.name.lower()
                cfgSize = configparser.ConfigParser()
                cfgSize["avaSIZE"] = dict(cfg["avaSIZE"])
                sect = cfgSize["avaSIZE"]

                size_parent = avaDir.parent.name.lower()
                if size_parent.startswith("size"):
                    try:
                        sect["sizeMax"] = str(int(size_parent[4:]))
                    except ValueError:
                        pass

                if scen in ("dry", "wet"):
                    sect["constantTemperature"] = "True"
                    sect["Tcons"] = sect.get("TCold" if scen == "dry" else "TWarm", sect.get("Tcons", "0"))

                compParams.computeAndSaveParameters(
                    avaDir, cfg["avaPARAMETER"], sect, demOverride=demPath, compressFiles=False
                )
                log.info("Step 09: Parameterized ./%s (%s)", relLeaf, scen)

            stepStats["Step 09"] = time.perf_counter() - t9
            log.info("Step 09: Finished parameterization in %.2fs", stepStats["Step 09"])
        except Exception:
            log.exception("Step 09: Parameterization failed.")
            return False
    else:
        log.info("Step 09: Skipped (flag=False)")


    # -------------------------------------------------------------------------
    # Step 10–12: FlowPy run & postprocessing
    # -------------------------------------------------------------------------
    if workflowUtils.stepEnabled(workflowFlags, "flowPyRun", masterFlowPy):
        t10 = time.perf_counter()
        log.info("Step 10: FlowPy run -----------------------------------------------------")
        try:
            # Discover and filter FlowPy leaves
            avaDirs = workflowUtils.discoverAndFilterAvaDirs(cfg, workFlowDir, "Step 10")
            if not avaDirs:
                log.error("Step 10: No FlowPy directories available; cannot continue.")
                return False

            # Optional post-processing flags
            doSize     = workflowUtils.stepEnabled(workflowFlags, "flowPyOutputToSize",   masterFlowPy)
            doCompress = workflowUtils.stepEnabled(workflowFlags, "flowPyOutputCompress", masterFlowPy)
            delOG      = workflowUtils.stepEnabled(workflowFlags, "flowPyDOutputDeleteOGFiles", masterFlowPy)
            delTemp    = workflowUtils.stepEnabled(workflowFlags, "flowPyDeleteTempFolder",     masterFlowPy)

            # -----------------------------------------------------------------
            # Loop over each FlowPy directory
            # -----------------------------------------------------------------
            for avaDir in avaDirs:
                relLeaf = os.path.relpath(avaDir, workFlowDir["cairosDir"])
                log.info("Step 10: Running FlowPy for ./%s...", relLeaf)
                t_leaf = time.perf_counter()

                # --- Run FlowPy safely while preserving CAIROS logging setup ---
                with workflowUtils.preserveLoggingForFlowPy():
                    runCom4FlowPy.main(avalancheDir=str(avaDir))

                log.info(
                    "Step 10: FlowPy run finished for ./%s in %.2fs",
                    relLeaf, time.perf_counter() - t_leaf,
                )

                # --- Step 11: Optional back-map FlowPy results to size ---
                if doSize:
                    try:
                        log.info("Step 11: Back-map FlowPy output to size for ./%s", relLeaf)
                        compParams.computeAndSaveSize(pathlib.Path(avaDir), cfg["avaSIZE"])
                    except Exception:
                        log.exception("Step 11: Results → size failed for ./%s", relLeaf)
                        return False

                # --- Step 12: Optional compression and cleanup ---
                if doCompress:
                    try:
                        outDir = pathlib.Path(avaDir) / "Outputs"
                        log.info("Step 12: Compress outputs for ./%s", relLeaf)
                        dataUtils.tifCompress(outDir, delete_original=delOG)
                    except Exception:
                        log.exception("Step 12: Compression failed for ./%s", relLeaf)
                        return False

                if delTemp:
                    try:
                        log.info("Step 12: Delete temporary data for ./%s", relLeaf)
                        dataUtils.deleteTempFolder(pathlib.Path(avaDir))
                    except Exception:
                        log.exception("Step 12: Delete temp data failed for ./%s", relLeaf)
                        return False

            # -----------------------------------------------------------------
            # Step summary
            # -----------------------------------------------------------------
            stepStats["Step 10"] = time.perf_counter() - t10
            log.info("Step 10–12: FlowPy + postprocessing completed in %.2fs",
                     stepStats["Step 10"])

        except Exception:
            log.exception("Step 10–12: FlowPy processing failed.")
            return False

    else:
        log.info("Step 10: FlowPy run skipped (flag=False)")



    # ─────────────────────────────────────────────────────────────────────────
    # Step 13–15: Avalanche Directory Builder
    # ─────────────────────────────────────────────────────────────────────────

    # -------------------------------------------------------------------------
    # Step 13: Avalanche Directory Build from FlowPy
    # -------------------------------------------------------------------------
    t13 = time.perf_counter()
    log.info("Step 13: Avalanche Directory Build from FlowPy -----------------------------")

    if not workflowUtils.stepEnabled(workflowFlags, "avaDirBuildFromFlowPy", masterAvaDir):
        log.info("Step 13: ...AvaDirectory Build from FlowPy skipped (flag=False)")
    else:
        try:
            avaDirBuildFromFlowPy.runAvaDirBuildFromFlowPy(cfg, workFlowDir)
            stepStats["Step 13"] = time.perf_counter() - t13
            log.info(
                "Step 13: AvaDirectory Build from FlowPy finished successfully in %.2fs",
                stepStats["Step 13"],
            )
        except Exception:
            log.exception("Step 13: AvaDirectory Build from FlowPy failed.")
            return False

    # -------------------------------------------------------------------------
    # Step 14: Avalanche Directory Type
    # -------------------------------------------------------------------------
    t14 = time.perf_counter()
    log.info("Step 14: Avalanche Directory Type ------------------------------------------")

    if not workflowUtils.stepEnabled(workflowFlags, "avaDirType", masterAvaDir):
        log.info("Step 14: ...AvaDirectory Type skipped (flag=False)")
    else:
        try:
            avaDirType.runAvaDirType(cfg, workFlowDir)
            stepStats["Step 14"] = time.perf_counter() - t14
            log.info(
                "Step 14: AvaDirectory Type finished successfully in %.2fs",
                stepStats["Step 14"],
            )
        except Exception:
            log.exception("Step 14: Avalanche Directory Type failed.")
            return False

    # -------------------------------------------------------------------------
    # Step 15: Avalanche Directory Results
    # -------------------------------------------------------------------------
    t15 = time.perf_counter()
    log.info("Step 15: Avalanche Directory Results ---------------------------------------")

    if not workflowUtils.stepEnabled(workflowFlags, "avaDirResults", masterAvaDir):
        log.info("Step 15: ...AvaDirectory Results skipped (flag=False)")
    else:
        try:
            avaDirResults.runAvaDirResults(cfg, workFlowDir)
            stepStats["Step 15"] = time.perf_counter() - t15
            log.info(
                "Step 15: Avalanche Directory Results Build finished successfully in %.2fs",
                stepStats["Step 15"],
            )
        except Exception:
            log.exception("Step 15: AvaDirectory Results failed.")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Step 00–15: FINAL SUMMARY
    # ─────────────────────────────────────────────────────────────────────────
    total = sum(stepStats.values())
    log.info("\n\nCAIROS Workflow Summary...\n")
    for s, dur in stepStats.items():
        log.info("%-12s ✅ %.2fs", s, dur)
    log.info("Total runtime: %.2fs", total)
    return True



# ─────────────────────────────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(name)s: %(message)s")

    # Enable informative logging for key CAIROS modules
    for name in [
        "__main__",
        "runCairos",
        "runInitWorkDir",
        "in1Utils.workflowUtils", 
        "com2AvaDirectory.avaDirBuildFromFlowPy",
        "in2Parameter",
        "in2Parameter.compParams",
    ]:
        logging.getLogger(name).setLevel(logging.INFO)

    # Silence noisy AvaFrame internals
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
