import pathlib
import logging
import shutil
import avaframe.in3Utils.cfgUtils as cfgUtils
from avaframe.in3Utils import cfgHandling

import ati
import ati.mod0Helper.dataUtils as dataUtils
import workflows.runDynamicParameterisation as runDynamicParameterisation
import ati.mod1Release.praDelineationVeitinger as praDelineationVeitinger
import ati.mod1Release.praProcessing as praProcessing
import ati.mod1Release.praSubCatchments as praSubCatchments
import ati.mod1Release.praSegmentation as praSegmentation
import ati.mod1Release.praPrepForFlowPy as praPrepForFlowPy
import ati.mod2Mobility.compParams as compParams

log = logging.getLogger("ati.workflows.runDynamicParameterisation")


def dynamicParameterisationMain(avaDir=None, cfgDynParamWorkflow=None):
    """
    Workflow to compute dynamic parameters alpha angle and max velocity limit:
        - PRA Delineation
        - PRA segmentation
        - dynamic parameterization
    Parameters
    -----------------
    avaDir: str
        directory to processed avalanche
    cfgDynParamWorkflow: configparser object
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

    if cfgDynParamWorkflow is None:
        cfgDynParamWorkflow = cfgUtils.getModuleConfig(runDynamicParameterisation)

    # override parameters for pra delineation (after Veitinger et al 2016)
    praDelineationCfg = cfgUtils.getModuleConfig(
        praDelineationVeitinger,
        fileOverride="",
        modInfo=False,
        toPrint=False,
        onlyDefault=cfgDynParamWorkflow["mod1Release_praDelineationVeitinger_override"].getboolean(
            "defaultConfig"
        ),
    )
    praDelineationCfg, _ = cfgHandling.applyCfgOverride(
        praDelineationCfg,
        cfgDynParamWorkflow,
        praDelineationVeitinger,
        addModValues=False,
    )

    #  run pra delineation
    log.info("Run PRA delineation")
    praDelineationVeitinger.runPraDelineation(praDelineationCfg, avaDir=avaDir)

    # copy binary release file from Outputs folder to Inputs/REL
    praDelineationDir = avaDir / "Outputs" / "PraDelineation"
    inRelDir = avaDir / "Inputs" / "REL"
    inRelIDDir = avaDir / "Inputs" / "RELID"
    inRelAreaDir = avaDir / "Inputs" / "RELArea"

    inRelAreaDir.mkdir(exist_ok=True)
    inRelDir.mkdir(parents=True, exist_ok=True)
    inRelIDDir.mkdir(parents=True, exist_ok=True)

    # override parameters for pra processing (polygonizing)
    mod1ReleaseCfg = cfgUtils.getModuleConfig(
        modPath / "mod1Release" / "mod1Release",
        fileOverride="",
        modInfo=False,
        toPrint=False,
        onlyDefault=cfgDynParamWorkflow["mod1Release_mod1Release_override"].getboolean("defaultConfig"),
    )
    mod1ReleaseCfg, _ = cfgHandling.applyCfgOverride(
        mod1ReleaseCfg,
        cfgDynParamWorkflow,
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
        onlyDefault=cfgDynParamWorkflow["mod2Mobility_compParams_override"].getboolean("defaultConfig"),
    )
    compParamsCfg, _ = cfgHandling.applyCfgOverride(
        compParamsCfg, cfgDynParamWorkflow, compParams, addModValues=False
    )

    compParams.computeAndSaveParameters(avaDir, compParamsCfg, demOverride=None, compressFiles=False)


if __name__ == "__main__":
    dynamicParameterisationMain()
