"""Build a current AvaDirectory from existing FlowPy results."""

import logging
import pathlib
import time

import avaframe.in3Utils.cfgUtils as cfgUtils

from ati.mod0Helper import cfgUtils as atiCfgUtils
from ati.mod0Helper.avaDirectory import avaDirectoryWorkflow
import workflows.runAvaDirectory as runAvaDirectory


log = logging.getLogger(__name__)


def _resolveDirectories(cfg):
    """Resolve and validate the project-root directory structure."""
    projectRootValue = cfg["PATHS"].get("projectRoot", "").strip()
    if not projectRootValue:
        raise ValueError("[PATHS] projectRoot is empty.")

    projectRoot = pathlib.Path(projectRootValue).expanduser().resolve()
    flowPySourceDir = projectRoot / "09_flowPyBigDataStructure"
    if not flowPySourceDir.is_dir():
        raise FileNotFoundError(
            f"FlowPy source directory not found: {flowPySourceDir}"
        )

    flowPyResults = sorted(
        path
        for path in flowPySourceDir.rglob("Outputs/com4FlowPy")
        if path.is_dir() and any(path.glob("peakFiles/res_*"))
    )
    if not flowPyResults:
        raise FileNotFoundError(
            "No FlowPy results found below "
            f"{flowPySourceDir}; expected */Outputs/com4FlowPy/peakFiles/res_*"
        )

    avaDirData = projectRoot / "11_avaDirectoryData"
    avaDirOutput = projectRoot / "12_avaDirectory"
    avaDirData.mkdir(parents=True, exist_ok=True)
    avaDirOutput.mkdir(parents=True, exist_ok=True)

    workFlowDir = {
        "cairosDir": str(projectRoot),
        "flowPySourceDir": str(flowPySourceDir),
        "flowPyRunDir": str(flowPySourceDir),
        "avaDirDir": str(avaDirData),
        "avaDirTypeDir": str(avaDirOutput),
        "avaDirResultsDir": str(avaDirOutput),
        "avaDirIndexDir": str(avaDirOutput),
    }
    return workFlowDir, flowPyResults


def runAvaDirectoryMain():
    """Run AvaDirectory Steps 13–15 for one model-chain project root."""
    cfg = cfgUtils.getModuleConfig(runAvaDirectory, toPrint=False)
    logLevel = cfg["WORKFLOW"].get("logLevel", "INFO").upper()
    logging.getLogger().setLevel(getattr(logging, logLevel, logging.INFO))

    try:
        workFlowDir, flowPyResults = _resolveDirectories(cfg)
    except Exception:
        log.exception("AvaDirectory initialization failed.")
        return False

    avaDirOutput = pathlib.Path(workFlowDir["avaDirTypeDir"])
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    runName = f"runAvaDirectory_{timestamp}"
    logPath = avaDirOutput / f"{runName}.log"
    fileHandler = logging.FileHandler(logPath, mode="w", encoding="utf-8")
    fileHandler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logging.getLogger().addHandler(fileHandler)

    log.info("AvaDirectory project root: %s", workFlowDir["cairosDir"])
    log.info("FlowPy search root: %s", workFlowDir["flowPySourceDir"])
    log.info("Discovered %d FlowPy result directories.", len(flowPyResults))
    log.info("AvaDirectory data: %s", workFlowDir["avaDirDir"])
    log.info("AvaDirectory outputs: %s", workFlowDir["avaDirTypeDir"])

    configPath = atiCfgUtils.writeEffectiveConfigJson(
        cfg, avaDirOutput, f"{runName}.json"
    )
    log.info("Effective configuration saved at %s", configPath)

    stepStats = {}
    started = time.perf_counter()
    success = avaDirectoryWorkflow.runAvaDirectorySteps(
        cfg, workFlowDir, stepStats=stepStats
    )
    if success:
        log.info("AvaDirectory workflow completed in %.2fs", time.perf_counter() - started)
        for step, elapsed in stepStats.items():
            log.info("%s: %.2fs", step, elapsed)
    return success


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
    raise SystemExit(0 if runAvaDirectoryMain() else 1)
