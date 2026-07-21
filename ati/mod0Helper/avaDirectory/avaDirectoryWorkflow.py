"""Shared orchestration for AvaDirectory Steps 13–15."""

import logging
import pathlib
import time

from ati.mod0Helper import workflowUtils
from ati.mod0Helper.avaDirectory import avaDirBuildFromFlowPy
from ati.mod0Helper.avaDirectory import avaDirResults
from ati.mod0Helper.avaDirectory import avaDirType


log = logging.getLogger(__name__)


def _hasConfiguredOutput(directory, stem, cfg, prefix):
    """Return whether at least one enabled output format exists."""
    formats = {
        "Csv": ".csv",
        "GeoJSON": ".geojson",
        "Parquet": ".parquet",
    }
    for optionSuffix, extension in formats.items():
        option = f"write{prefix}{optionSuffix}"
        if cfg.getboolean(option, fallback=True):
            if (directory / f"{stem}{extension}").is_file():
                return True
    return False


def _validateStepOutput(stepKey, cfg, workFlowDir):
    """Validate the main artifact produced by an AvaDirectory step."""
    avaCfg = cfg["avaDIRECTORY"]
    avaDirData = pathlib.Path(workFlowDir["avaDirDir"])
    avaDirOutput = pathlib.Path(workFlowDir["avaDirTypeDir"])
    flowPySource = pathlib.Path(workFlowDir["flowPySourceDir"])

    if stepKey == "13":
        if avaCfg.getboolean("doCollectSingleAva", fallback=True):
            return any(avaDirData.glob("com4_*"))
        if avaCfg.getboolean("writeScenarioParquet", fallback=False):
            return any(flowPySource.rglob("avaScenLeaf_com4_*.parquet"))
        return True
    if stepKey == "14":
        return _hasConfiguredOutput(
            avaDirOutput, "avaDirectoryType", avaCfg, "Type"
        )
    if stepKey == "15":
        return _hasConfiguredOutput(
            avaDirOutput, "avaDirectoryResults", avaCfg, "Results"
        )
    return True


def runAvaDirectorySteps(cfg, workFlowDir, stepStats=None):
    """Run enabled AvaDirectory steps and verify their main outputs.

    Parameters
    ----------
    cfg : configparser.ConfigParser
        Effective runner or model-chain configuration.
    workFlowDir : dict
        Resolved FlowPy source and AvaDirectory output directories.
    stepStats : dict, optional
        Dictionary updated with elapsed time per step.

    Returns
    -------
    bool
        ``True`` when every enabled step completed and produced output.
    """
    if stepStats is None:
        stepStats = {}

    workflowFlags = cfg["WORKFLOW"]
    runAll = workflowFlags.getboolean("runAllAvaDirSteps", fallback=False)
    steps = (
        (
            "13",
            "avaDirBuildFromFlowPy",
            "Avalanche Directory Build from FlowPy",
            avaDirBuildFromFlowPy.runAvaDirBuildFromFlowPy,
        ),
        ("14", "avaDirType", "Avalanche Directory Type", avaDirType.runAvaDirType),
        (
            "15",
            "avaDirResults",
            "Avalanche Directory Results",
            avaDirResults.runAvaDirResults,
        ),
    )

    for stepKey, flag, label, function in steps:
        if not workflowUtils.stepEnabled(workflowFlags, flag, runAll):
            log.info("Step %s: ...%s skipped (flag is False)", stepKey, label)
            continue

        started = time.perf_counter()
        log.info("Step %s: Start %s...", stepKey, label)
        try:
            function(cfg, workFlowDir)
        except Exception:
            log.exception("Step %s: %s failed.", stepKey, label)
            return False

        elapsed = time.perf_counter() - started
        stepStats[f"Step {stepKey}"] = elapsed
        if not _validateStepOutput(stepKey, cfg, workFlowDir):
            log.error(
                "Step %s: %s produced no expected output; stopping.",
                stepKey,
                label,
            )
            return False
        log.info("Step %s: %s finished in %.2fs", stepKey, label, elapsed)

    return True
