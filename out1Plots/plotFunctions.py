# AvaScenarioModelChain/out1Plots/plotFunctions.py
# Author: Paula Spannring (BFW)

import numpy as np

import in1Utils.dataUtils as dataUtils

def getInputParameters(path, parameter):
    '''
    Get value of input parameter (alpha, umax, exponent) of simulation
    only works if all release cells have the same parameter
    
    Parameters:
    -----------
    path: str 
        Path to the data (avaframe structure)
        
    Returns:
    -----------
    value: float
        value of input parameter used for this simulated path
    '''
    
    inputsPath = dataUtils.getInputPath(path)
    
    if parameter == 'alpha':
        paramPath = f'{inputsPath}/ALPHA'
    elif parameter == 'exp':
        paramPath = f'{inputsPath}/EXP'
    elif parameter == 'umax':
        paramPath = f'{inputsPath}/UMAX'
    elif parameter == 'rel':
        paramPath = f'{inputsPath}/REL'
    raster = dataUtils.readRaster(paramPath)
    print(raster)
    value = np.nanmax(raster)
    
    return value