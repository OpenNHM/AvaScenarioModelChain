import pathlib
import logging
import shutil

from avaframe import runCom4FlowPy as runFlowPy
import avaframe.com4FlowPy.com4FlowPy as com4FlowPy
import avaframe.in3Utils.cfgUtils as cfgUtils
from avaframe.in3Utils import cfgHandling

import ati
import ati.mod0Helper.dataUtils as dataUtils
import ati.mod1Release.praDelineationVeitinger as praDelineationVeitinger
import ati.mod1Release.praProcessing as praProcessing
import ati.mod1Release.praSubCatchments as praSubCatchments
import ati.mod1Release.praSegmentation as praSegmentation
import ati.mod1Release.praPrepForFlowPy as praPrepForFlowPy
import ati.mod2Mobility.compParams as compParams
import ati.mod3Map.autoATESClassifier as autoATESClassifier
import workflows.runAutoAtesModelChain as runAutoAtesModelChain

log = logging.getLogger("workflows.runDynamicParameterisation")


def autoAtesModelChainMain(avaDir=None, cfgAutoAtes=None):
    """
    Workflow to compute ATES maps from DEM including:
        - PRA Delineation
        - PRA segmentation
        - dynamic parameterization
        - AvaFrame::com4FlowPy execution
        - autoATES classifier
    Parameters
    -----------------
    avaDir: str
        directory to processed avalanche
    cfgAutoAtes: configparser object
        setup
    """
    modPath = pathlib.Path(ati.__file__).resolve().parent
    cfgNameFile = modPath / "atiCfg.ini"
    cfgMain = cfgUtils.getGeneralConfig(nameFile=cfgNameFile)

    if avaDir is None:
        avaDir = cfgMain["MAIN"]["avalancheDirectory"]
    else:
        cfgMain["MAIN"]["avalancheDirectory"] = avaDir

    avaDir = pathlib.Path(avaDir)

    if cfgAutoAtes is None:
        cfgAutoAtes = cfgUtils.getModuleConfig(runAutoAtesModelChain)

    # override parameters for pra delineation
    praDelineationCfg = cfgUtils.getModuleConfig(
        praDelineationVeitinger,
        fileOverride="",
        modInfo=False,
        toPrint=False,
        onlyDefault=cfgAutoAtes["mod1Release_praDelineationVeitinger_override"].getboolean("defaultConfig"),
    )
    praDelineationCfg, _ = cfgHandling.applyCfgOverride(
        praDelineationCfg, cfgAutoAtes, praDelineationVeitinger, addModValues=False
    )

    #  run pra delineation
    log.info("Run PRA delineation")
    praDelineationVeitinger.runPraDelineation(praDelineationCfg, avaDir=avaDir)

    # copy binary release file from Outputs folder to Inputs/REL
    praDelineationDir = avaDir / "Outputs" / "PraDelineation"
    inRelDir = avaDir / "Inputs" / "REL"
    inRelIDDir = avaDir / "Inputs" / "RELID"
    inRelAreaDir = avaDir / "Inputs" / "RELArea"

    inRelDir.mkdir(parents=True, exist_ok=True)

    # override parameters for pra processing (polygonizing)
    mod1ReleaseCfg = cfgUtils.getModuleConfig(
        modPath / "mod1Release" / "mod1Release",
        fileOverride="",
        modInfo=False,
        toPrint=False,
        onlyDefault=cfgAutoAtes["mod1Release_mod1Release_override"].getboolean("defaultConfig"),
    )
    mod1ReleaseCfg, _ = cfgHandling.applyCfgOverride(
        mod1ReleaseCfg,
        cfgAutoAtes,
        modPath / "mod1Release" / "mod1Release",
        addModValues=False,
    )

    #  run pra processing (polygonizing)
    log.info("Run PRA processing (polygonizing)")
    praProcessing.runPraProcessing(mod1ReleaseCfg, avaDir=avaDir)

    # generate subcatchments
    log.info("Run generating subcatchments")
    praSubCatchments.runSubcatchments(mod1ReleaseCfg, avaDir=avaDir)

    log.info("Run PRA segmentation")
    praSegmentation.runPraSegmentation(mod1ReleaseCfg, avaDir=avaDir)

    log.info("Run PRA rasterization")
    praPrepForFlowPy.runPraPrepForFlowPy(mod1ReleaseCfg, avaDir=avaDir)

    inRelIDDir.mkdir(parents=True, exist_ok=True)
    inRelAreaDir.mkdir(exist_ok=True)
    praPrepDir = avaDir / "Work" / "praPrepForFlowPy"

    if len(list(inRelIDDir.glob("*.tif"))) > 0:
        message = "In Inputs/RELID folder is a PRA file, the generated file is not copied!"
        log.info(message)
    else:
        praIDFile = list(praPrepDir.glob("*-5-praID.tif"))
        if len(praIDFile) == 0:
            message = f"No REL ID file is in {praPrepDir} to copy into Inputs/RELID!"
            log.error(message)
            raise FileNotFoundError(message)
        elif len(praIDFile) > 1:
            message = f"Too many REL ID files in {praPrepDir} to copy into Inputs/RELID!"
            log.error(message)
            raise FileNotFoundError(message)
        shutil.copy2(praIDFile[0], inRelIDDir)

    if len(list(inRelAreaDir.glob("*.tif"))) > 0:
        message = "In Inputs/RELArea folder is a PRA file, the generated file is not copied!"
        log.info(message)
    else:
        praVolFile = list(praPrepDir.glob("*-5-praAreaM.tif"))
        if len(praVolFile) == 0:
            message = f"No REL Vol file is in {praPrepDir} to copy into Inputs/RELArea!"
            log.error(message)
            raise FileNotFoundError(message)
        elif len(praVolFile) > 1:
            message = f"Too many REL Vol files in {praPrepDir} to copy into Inputs/RELArea!"
            log.error(message)
            raise FileNotFoundError(message)

        shutil.copy2(praVolFile[0], inRelAreaDir)

    if len(list(inRelDir.glob("*.tif"))) > 0:
        message = "In Inputs/REL folder is a PRA file, the generated file is not copied!"
        log.info(message)
    else:
        # save binary PRA raster
        praArea, header = dataUtils.readRaster(pathlib.Path(praVolFile[0]))
        praArea[praArea > 0] = 1
        dataUtils.saveRaster(praVolFile[0], inRelDir / "pra_binary.tif", praArea)

    # override parameters for pra delineation
    compParamsCfg = cfgUtils.getModuleConfig(
        compParams,
        fileOverride="",
        modInfo=False,
        toPrint=False,
        onlyDefault=cfgAutoAtes["mod2Mobility_compParams_override"].getboolean("defaultConfig"),
    )
    compParamsCfg, _ = cfgHandling.applyCfgOverride(
        compParamsCfg, cfgAutoAtes, compParams, addModValues=False
    )

    compParams.computeAndSaveParameters(avaDir, compParamsCfg, demOverride=None, compressFiles=False)

    # execute AvaFrame::com4FlowPy
    # override parameters for pra delineation
    FlowPyCfg = cfgUtils.getModuleConfig(
        com4FlowPy,
        fileOverride="",
        modInfo=False,
        toPrint=False,
        onlyDefault=cfgAutoAtes["com4FlowPy_com4FlowPy_override"].getboolean("defaultConfig"),
    )
    FlowPyCfg, _ = cfgHandling.applyCfgOverride(FlowPyCfg, cfgAutoAtes, com4FlowPy, addModValues=False)
    flowPyResDict = runFlowPy.main(avalancheDir=str(avaDir), cfg=FlowPyCfg)
    FlowpyHash = flowPyResDict["uid"]

    # override parameters for pra delineation
    atesClassifierCfg = cfgUtils.getModuleConfig(
        autoATESClassifier,
        fileOverride="",
        modInfo=False,
        toPrint=False,
        onlyDefault=cfgAutoAtes["mod3Map_autoATESClassifier_override"].getboolean("defaultConfig"),
    )
    atesClassifierCfg, _ = cfgHandling.applyCfgOverride(
        atesClassifierCfg, cfgAutoAtes, autoATESClassifier, addModValues=False
    )

    autoATESClassifier.autoATESClassifierMain(cfg=atesClassifierCfg, avaDir=avaDir, flowpyHash=FlowpyHash)


if __name__ == "__main__":
    autoAtesModelChainMain()
