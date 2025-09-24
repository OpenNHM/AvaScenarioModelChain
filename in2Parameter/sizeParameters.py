import numpy as np
import math
import logging

log = logging.getLogger(__name__)



def praToVrel(ARel, dem, cfgSize):
    '''
    calculate release Volume dependend on release area and elevation
    
    Parameters:
    -----------
    ARel: 2-dim numpy array or float
        area of PRA
    dem: 2-dim numpy array
        elevation values of PRAs
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    VRel: numpy array or float
        Volume of release area
    '''
    d = snowclimateToThickness(dem, cfgSize)
    VRel = ARel * d # m³
    return VRel, d


def snowclimateToThickness(dem, cfgSize):
    '''
    calculate snow thickness dependend on elevation and snow climate
    
    Parameters:
    -----------
    dem: 2-dim numpy array
        elevation values of PRAs
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    d: numpy array or float
        snow thickness
    '''
    if cfgSize.getboolean('constantPraThickness'):
        d = cfgSize.getfloat('praThickness')
    else:
        D0 = cfgSize.getfloat('D0')
        deltaD = cfgSize.getfloat('deltaD')
        d = D0 + deltaD * dem # m
    return d
    

def praToVRelSize(ARel, dem, cfgSize):
    '''
    calculate avalanche size dependend on release area and dem
    
    Parameters:
    -----------
    ARel: 2-dim numpy array or float
        area of PRA
    dem: 2-dim numpy array
        elevation values of PRAs
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    size: numpy array or float
        avalanche size of PRA
    '''
    
    ARel = np.array(ARel)
    
    if cfgSize.getboolean('constantPraThickness') == False:
        dem = np.array(dem)
        size = np.zeros(dem.shape)

        if len(dem.shape) > 1 and len(ARel.shape) > 1:
            # dem and pra are 2 dim
            for i,(z,pra) in enumerate(zip(dem, ARel)):
                for j, (z2, pra2) in enumerate(zip(z,pra)):
                    try:
                        d = snowclimateToThickness(z2, cfgSize)
                        size[i,j] = 2 + math.log(d * pra2 * 1e-3 , 5)                   
                    except:
                        size[i,j] = 0

        elif len(dem.shape) > 1 and len(ARel.shape) == 0:
            # dem is 2 dimensional, ARel is float
            for i, z in enumerate(dem):
                for j, z2 in enumerate(z):
                    try:
                        d = snowclimateToThickness(z2, cfgSize)
                        size[i,j] = 2 + math.log(d * ARel * 1e-3 , 5)                   
                    except:
                        size[i,j] = 0

        elif len(dem.shape) == 1 and len(ARel.shape) == 0:
            # dem is 1 dimensional, ARel is float
            for i, z2 in enumerate(dem):
                try:
                    d = snowclimateToThickness(z2, cfgSize)
                    size[i] = 2 + math.log(d * ARel * 1e-3 , 5)                   
                except:
                    size[i] = 0

    else:
        praThickness = cfgSize.getfloat('praThickness')
        size = np.zeros(ARel.shape)

        vRel = praThickness * ARel
        if len(ARel.shape) == 1:
            for i,v in enumerate(vRel):
                try:
                    size[i] = (2 + math.log(v * 1e-3, 5))
                except:
                    size[i] = 0
                    
        elif len(ARel.shape) > 1:
            for i,v in enumerate(vRel):
                for j, v2 in enumerate(v):
                    try:
                        size[i,j] = (2 + math.log(v2 * 1e-3, 5))
                    except:
                        size[i,j] = 0
        
        elif len(ARel.shape) == 0:
            size = (2 + math.log(vRel * 1e-3, 5))

    size = np.array(size)

    if cfgSize['sizeMax'] != '':
        sizeMax = cfgSize.getfloat('sizeMax')
        size[size > sizeMax] = sizeMax

    return size


def sizeToAlpha(size, dem, cfgSize):
    '''
    calculate FlowPy input parameter alpha angle dependend on avalanche size 
    the alpha angle decreases linearly with the avalanche size
    
    Parameters:
    -----------
    size: numpy array or float
        avalanche size of PRA cell
    dem: numpy array
        DEM, elevation
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    alphaPRA: numpy array or float
        alpha angle of PRA
    '''
    if cfgSize.getboolean("alphaDependendTemperature"):
        sizeTemp = sizeForParameterisation(size, dem, cfgSize, cfgSize.getfloat('sizeShiftAlpha'))
        log.info(f'The average of the change in size in the alpha parameterisation is: {np.nanmean(sizeTemp - size)}')
    else:
        sizeTemp = size


    alphaSize2 = cfgSize.getfloat('alphaSize2')
    deltaAlpha = cfgSize.getfloat('deltaAlpha')

    alphaPRA = alphaSize2 - (sizeTemp - 2) * deltaAlpha
    return alphaPRA


def sizeToUmax(size, dem, cfgSize):
    '''
    calculate FlowPy input parameter limit of maximal velocity dependend on avalanche size 
    the uMax limit increases linearly with the avalanche size

    Parameters:
    -----------
    size: numpy array or float
        avalanche size of PRA cell
    dem: numpy array
        DEM, elevation
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    umaxPRA: numpy array or float
        uMax limit of PRA
    '''

    sizeTemp = sizeForParameterisation(size, dem, cfgSize, cfgSize.getfloat('sizeShiftUmax'))
    log.info(f'The average of the change in size in the uMax parameterisation is: {np.nanmean(sizeTemp - size)}')

    uMaxSize2 = cfgSize.getfloat('uMaxSize2')
    deltaUMax = cfgSize.getfloat('deltaUMax')
    
    umaxPRA = uMaxSize2 + (sizeTemp - 2) * deltaUMax
    umaxPRA[umaxPRA < 5] = 5
    return umaxPRA


def sizeToExp(size, dem, cfgSize):
    '''
    calculate FlowPy input parameter exponent dependend on avalanche size
    
    Parameters:
    -----------
    size: numpy array or float
        avalanche size of PRA cell
    dem: numpy array
        DEM, elevation
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    exp: numpy array or float
        exponent of PRA
    '''
    sizeTemp = sizeForParameterisation(size, dem, cfgSize, cfgSize.getfloat('sizeShiftExp'))

    exp = 75 * (0.64)**sizeTemp
    return exp


def sizeForParameterisation(sizeRef, dem, cfgSize, wetSizeShift):
    """
    compute the shifted size as input for parameterisation - functions
    as function of temperature, with a cold and a warm limit

    Parameters:
    -----------
    sizeRef: numpy array or float
        avalanche size of PRA cell (for cold avalanches)
    dem: numpy array
        DEM
    cfgSize: congig Parser
        contains parameters for size parameterisation
    wetSizeShift: float
        maximal shift of size (for wet avalanches)
        
    Returns:
    -----------
    sizeTemp: numpy array 
        shifted size inlcuding temperature
    """

    temp = zToTemp(cfgSize, dem)
    TCold = cfgSize.getfloat('TCold')
    TWarm = cfgSize.getfloat('TWarm')

    # compute the size as input for parameterisation as function of temperature
    slope = wetSizeShift / (TWarm - TCold)
    sizeTemp = sizeRef + (temp - TCold) * slope
    return sizeTemp


def zToTemp(cfgSize, dem):
    """
    compute temperature profile dependend on snow climate

    Parameters:
    -----------
    dem: numpy array
        DEM
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    temp: numpy array 
        temperature dependend on snow climate and dem
    """
    TCold = cfgSize.getfloat('TCold')
    TWarm = cfgSize.getfloat('TWarm')

    if cfgSize.getboolean('constantTemperature'):
        temp = cfgSize.getfloat('Tcons')
        temp = np.array(temp)
    else:
        T0 = cfgSize.getfloat('T0')
        deltaT = cfgSize.getfloat('deltaT')
        temp = T0 + dem * deltaT
        
    temp[temp<TCold] = TCold
    temp[temp>TWarm] = TWarm
    return temp


def alphaToSize(alphaSim, cfgSize):
    """
    Inverse of sizeToAlpha():
    Computes avalanche size from runout or travel angle
    
    Parameters:
    -----------
    alphaSim: numpy array or float
        simulated runout or travel angle
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    sizeSim: numpy array or float
        avalanche size
    """

    alphaSize2 = cfgSize.getfloat('alphaSize2')
    deltaAlpha = cfgSize.getfloat('deltaAlpha')

    sizeSim = - (alphaSim - alphaSize2) / deltaAlpha + 2
    return sizeSim


def zDeltaToSize(zDeltaSim, cfgSize):
    """
    Inverse of sizeToAlpha():
    Computes avalanche size from runout or travel angle
    
    Parameters:
    -----------
    zDeltaSim: numpy array or float
        simulated zDelta
    cfgSize: congig Parser
        contains parameters for size parameterisation
        
    Returns:
    -----------
    sizeSim: numpy array or float
        avalanche size
    """
    uMaxSim = np.sqrt(2 * 9.81 * zDeltaSim)
    uMaxSize2 = cfgSize.getfloat('uMaxSize2')
    deltaUMax = cfgSize.getfloat('deltaUMax')
    
    sizeSim = (uMaxSim - uMaxSize2) / deltaUMax + 2
    return sizeSim


def travelLengthToSize(travelLength):
    """
    compute avalanche size from travel length

    Parameters:
    -----------
    travelLength: numpy array or float
        simulated runout or travel length    

    Returns:
    -----------
    sizeSim: numpy array or float
        avalanche size
    """

    sizeSim = (travelLength / 4.5 )**(1/4)

    return sizeSim


