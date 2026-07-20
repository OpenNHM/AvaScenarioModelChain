"""
runscript to run the plots with the actual configuration
"""

import numpy as np
import os

import avaframe.in3Utils.cfgUtils as cfgUtils
import ati.out1Plots.out1SizeParameter as sizePlots
import ati.mobilityUtils.compParams as compParams


def runAndSavePlots(savePlotPath=""):
    """
    run and save plots to control config parameters
    """

    cfg = cfgUtils.getModuleConfig(compParams)
    cfgSize = cfg["avaSIZE"]
    cfgPlot = cfg["PLOT"]

    if cfgPlot.getfloat("elevationMin") != cfgPlot.getfloat("elevationMax"):
        elevation = np.linspace(
            cfgPlot.getfloat("elevationMin"), cfgPlot.getfloat("elevationMax"), 20
        )
    else:
        elevation = cfgPlot.getfloat("elevationMin")

    if cfgPlot.getfloat("ARelMin") != cfgPlot.getfloat("ARelMax"):
        ARel = np.linspace(cfgPlot.getfloat("ARelMin"), cfgPlot.getfloat("ARelMax"), 20)
        elevation = cfgPlot.getfloat("elevationMin")
    else:
        ARel = cfgPlot.getfloat("ARelMin")

    if savePlotPath == "":
        plotPath = "data/plots"
        if os.path.isdir(plotPath) == False:
            os.makedirs(plotPath)
    else:
        plotPath = savePlotPath

    if ARel is not None:
        crossplot = sizePlots.plotCrossCheck(cfgSize, ARel=ARel, elevation=elevation)
        for xVariable in ["size", "Vrel", "elevation"]:
            if type(elevation) == float and xVariable == "elevation":
                continue
            summarizeplot = sizePlots.plotSizeToPArameters(
                cfgSize,
                ARel=ARel,
                elevation=elevation,
                expBool=cfgPlot.getboolean("plotExponent"),
                xAxis=xVariable,
            )
            summarizeplot.savefig(
                f"{plotPath}/parameters_{xVariable}.png", bbox_inches="tight"
            )
    else:
        crossplot = sizePlots.plotCrossCheck(cfgSize, elevation=elevation)
        for xVariable in ["size", "Vrel", "elevation"]:
            if type(elevation) == float and xVariable == "elevation":
                continue
            summarizeplot = sizePlots.plotSizeToPArameters(
                cfgSize,
                elevation=elevation,
                expBool=cfgPlot.getboolean("plotExponent"),
                xAxis=xVariable,
            )
            summarizeplot.savefig(
                f"{plotPath}/parameters_{xVariable}.png", bbox_inches="tight"
            )

    muxi = sizePlots.plotMuXi(cfgSize, cfgPlot)
    muxi.savefig(f"{plotPath}/muxi.png")
    crossplot.savefig(f"{plotPath}/sizeCrossCheck.png")

    # fig = sizePlots.plotDataExample()
    # fig.savefig(f'{plotPath}/test.png')


if __name__ == "__main__":
    runAndSavePlots()
