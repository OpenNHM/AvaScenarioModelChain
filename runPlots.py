"""
runscript to run the plots with the actual configuration
"""
import numpy as np
import os

import outPlots.out1SizeParameter as sizePlots

import in1Utils.cfgUtils as cfgUtils


cfg = cfgUtils.getConfig()

def runAndSavePlots(savePlotPath=''):
    """
    run and save plots to control config parameters
    """

    cfg = cfgUtils.getConfig()
    cfgSize = cfg['SIZEPARAMETER']
    cfgPlot = cfg['PLOTS']

    elevation = np.linspace(cfgPlot.getfloat('elevationMin'), cfgPlot.getfloat('elevationMax'), 20)

    if savePlotPath == '':
        plotPath = 'data/plots'
        if os.path.isdir(plotPath) == False:
            os.makedirs(plotPath)
    else:
        plotPath = savePlotPath


    try:
        ARel = cfgPlot.getfloat('ARel')
    except:
        ARel = None
        
    if ARel is not None:
        crossplot = sizePlots.plotCrossCheck(cfgSize, ARel=ARel, elevation=elevation)
        for xVariable in ['size', 'Vrel', 'elevation']:
            summarizeplot = sizePlots.plotSizeToPArameters(cfgSize, ARel=ARel, elevation=elevation, expBool=cfgPlot.getboolean('plotExponent'), xAxis=xVariable)
            summarizeplot.savefig(f'{plotPath}/parameters_{xVariable}.png',bbox_inches='tight')
    else:
        crossplot = sizePlots.plotCrossCheck(cfgSize, elevation=elevation)
        for xVariable in ['size', 'Vrel', 'elevation']:
            summarizeplot = sizePlots.plotSizeToPArameters(cfgSize, elevation=elevation, expBool=cfgPlot.getboolean('plotExponent'), xAxis=xVariable)
            summarizeplot.savefig(f'{plotPath}/parameters_{xVariable}.png',bbox_inches='tight')

    muxi = sizePlots.plotMuXi(cfgSize, cfgPlot)
    muxi.savefig(f'{plotPath}/muxi.png')
    crossplot.savefig(f'{plotPath}/sizeCrossCheck.png')

    #fig = sizePlots.plotDataExample()
    #fig.savefig(f'{plotPath}/test.png')
 
        

if __name__ == '__main__':
    runAndSavePlots()