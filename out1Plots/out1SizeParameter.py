# AvaScenarioModelChain/out1Plots/out1SizeParameter.py
# Author: Paula Spannring (BFW)

import numpy as np
import matplotlib.pyplot as plt

import in2Parameter.sizeParameters as sizePar
import in2Parameter.muxi as muxi
import outPlots.plotFunctions as pF
from in1Utils import dataUtils 

def plotRasterResult(path, axs, axs_idx, param, flowPyUid=''):
    '''
    
    
    Parameters:
    -----------
    path: str 
        Path to the data (avaframe structure)
    resultName: str
        Name of Result folder
    axs: plt.axes
        axes of figure that is plotted
    axs_idx:
        index of figure panel
    size_params: dict
        information about avalanche size and area of PRAs (used for parameterisation)
    param: dict
        information about the parameter that is respresented (containing the y-axis label, represented maximal value
    flowPyUid: str
        result uid, in which folder is search, if '' (default) all folders are searched
        
    Returns:
    -----------
    axs: plt.axis
        axis for one figure subplot
    '''
    
    # get data (input and output data)
    alpha = pF.getInputParameters(path, 'alpha')
    uMax = pF.getInputParameters(path, 'umax')
    exp = pF.getInputParameters(path, 'exp')
    ARel = pF.getInputParameters(path, 'rel')
    #size = size_params['size']
    variable = param['name']
    inputsPath = dataUtils.getInputPath(path)
    dem = dataUtils.readRaster(inputsPath)
    dem[dem<0]=np.nan

    dataPath = dataUtils.getFlowPyOutputPath(path, variable, flowPyUid=flowPyUid)
    data, _ = dataUtils.readRaster(dataPath)
    data[data<=0] = np.nan
    if variable == 'zdelta':
        v = (data * 2 * 9.81)**0.5 # convert zDelta to velocity
    else:
        v = data
    
    # plot background-DEM, parameter
    axs[axs_idx].imshow(dem, alpha = 0.5, cmap = 'Greys')
    CS = axs[axs_idx].contour(dem, levels = np.arange(0,1200,100), colors ='k',linewidths=0.5)
    axs[axs_idx].clabel(CS, CS.levels[::2], inline=True,  fontsize=10) 
    cmap = axs[axs_idx].imshow(v)#, vmax = param['vmax'])
    
    axs[axs_idx].set_title(f'ARel = {ARel} m², (alpha = {alpha:.0f}°, uMax = {uMax:.0f} m/s, exp = {exp:.0f})')
    
    # tick labels in m (be aware! cellsize is hardcoded!)
    cellsize = 10
    axs[axs_idx].set_xticks([])
    axs[axs_idx].set_yticks([])
    x_ticks = np.linspace(0, 500, 6)
    x_ticks = np.append(x_ticks, 50)
    x_tick_labels = [str(label * cellsize) for label in x_ticks] 
    axs[axs_idx].set_xticks(x_ticks)
    axs[axs_idx].set_xticklabels(x_tick_labels)   
    y_ticks = np.array([0,50,100])
    y_tick_labels = [str(label * cellsize) for label in y_ticks] 
    axs[axs_idx].set_yticks(y_ticks)
    axs[axs_idx].set_yticklabels(y_tick_labels) 
    
    axs[axs_idx].set_ylabel('y in [m]', fontsize = 13)
    if axs_idx == 3:
        axs[axs_idx].set_xlabel('x in [m]', fontsize = 13)
    
    # colorbar
    if axs_idx == 0:
        cax = axs[axs_idx].inset_axes([400, 20, 15, 100], transform=axs[axs_idx].transData)
        plt.colorbar(cmap, cax=cax, label = param['label'])
    
    return axs[axs_idx]


def plotDataExample():
    fig, axs = plt.subplots(4, figsize = (15,15), tight_layout = True)
    for size in np.arange(2,6):
        avaframeName = f'parabChannel_topoSize{size}'
        resultName = 'res_20240813'
    
        globals()[f'path{size}'] = f'data/dataExamples/{avaframeName}/size{size}/dry'
        
        #####################
        size_params2 = {'A_rel' : 1000,
                        'size' : 2,
                        }

        size_params3 = {'A_rel' : 5000,
                        'size' : 3,
                        }

        size_params4 = {'A_rel' : 25000,
                        'size' : 4,
                        }
        size_params5 = {'A_rel' : 125000,
                        'size' : 5,
                        }
        paramZd = {'name': 'zdelta',
                'label': 'v [m/s]',
                'vmax': 65,
                }
    ###################
        
    axs[0] = plotRasterResult(path2, axs, 0, paramZd)
    axs[1] = plotRasterResult(path3, axs, 1, paramZd)
    axs[2] = plotRasterResult(path4, axs, 2, paramZd)
    axs[3] = plotRasterResult(path5, axs, 3, paramZd)
    ###################
    fig.suptitle(f'avalanche', fontsize = 15)
    return fig


def plotCrossCheck(cfgSize, ARel=5000, elevation=np.arange(100,3500,100)):
    '''
    plot various parameters dependent on the avalanche size and elevation
    
    Parameters:
    -----------
    cfgSize: config Parser
        contains parameters for size parameterisation
    ARel: numpy array or float
        area of PRA (default: 5000 m²)
    elevation: numpy array
        elevation values of PRAs (default: 100-3500 m)

    Returns:
    -----------
    fig: matplotlib figure
        contains the different parameters for teh size parameterisation
    '''
    D0 = cfgSize.getfloat('D0')
    deltaD = cfgSize.getfloat('deltaD')

    VRel = sizePar.praToVrel(ARel, elevation, cfgSize)[0]
    dRelease = sizePar.snowclimateToThickness(elevation, cfgSize)
    size = sizePar.praToVRelSize(ARel, elevation, cfgSize)

    if len(np.array(VRel).shape) == 0:
        VRel = np.ones_like(elevation) * VRel
    if len(np.array(dRelease).shape) == 0:
        dRelease = np.ones_like(elevation) * dRelease
    if len(np.array(size).shape) == 0:
        size = np.ones_like(elevation) * size

    alpha = sizePar.sizeToAlpha(size, elevation, cfgSize)
    umax = sizePar.sizeToUmax(size, elevation, cfgSize)
    exp = sizePar.sizeToExp(size, elevation, cfgSize)

    fig, axs = plt.subplots(6,2, figsize = (15,10), tight_layout =True)
    axs[0,0].set_title(f'Release area: {ARel} $m^2$, snowclimate: $\Delta$d = {deltaD*10000} cm / 100 m, $d_{{0m}}$ = {D0} m');

    axs[0,0].plot(elevation, dRelease)
    axs[0,0].set_xlabel('elevation [m]')
    axs[0,0].set_ylabel('snow height [m]')

    axs[1,0].plot(elevation, VRel)
    axs[1,0].set_xlabel('elevation [m]')
    axs[1,0].set_ylabel('Volume release [m³]');

    
    axs[2,0].plot(VRel, size)
    axs[2,0].set_xlabel('release Volume [m³]')
    axs[2,0].set_ylabel('avalanche size')

    axs[3,0].plot(VRel, alpha)
    axs[3,0].set_xlabel('release Volume [m³]')
    axs[3,0].set_ylabel('alpha [°]');

    axs[4,0].plot(VRel, umax)
    axs[4,0].set_ylabel('u max [m/s]')
    axs[4,0].set_xlabel('release Volume [m³]');

    axs[5,0].plot(VRel, exp)
    axs[5,0].set_xlabel('release Volume [m³]')
    axs[5,0].set_ylabel('exponent');

    axs[0,1].plot(size, dRelease)
    axs[0,1].set_xlabel('avalanche size')
    axs[0,1].set_ylabel('snow height m]')

    axs[1,1].plot(size, VRel)
    axs[1,1].set_xlabel('avalanche size')
    axs[1,1].set_ylabel('Volume release [m³]');

    axs[2,1].plot(size, elevation)
    axs[2,1].set_ylabel('elevation [m]')
    axs[2,1].set_xlabel('avalanche size')

    axs[3,1].plot(size, alpha)
    axs[3,1].set_xlabel('avalanche size')
    axs[3,1].set_ylabel('alpha [°]');

    axs[4,1].plot(size, umax)
    axs[4,1].set_xlabel('avalanche size')
    axs[4,1].set_ylabel('umax [m/s]');

    axs[5,1].plot(size, exp)
    axs[5,1].set_xlabel('avalanche size')
    axs[5,1].set_ylabel('exponent');

    return fig


def plotSizeToPArameters(cfgSize, ARel=5000, elevation = np.arange(100,3500,100), expBool = False, xAxis='size'):
    '''
    plot FlowPy input parameters alpha angle, uMaxLim and exponent dependent on the elevation
    
    Parameters:
    -----------
    cfgSize: config Parser
        contains parameters for size parameterisation
    ARel: numpy array or float
        area of PRA (default: 5000 m²)
    elevation: numpy array
        elevation values of PRAs (default: 100-3500 m)
    expBool: bool
        if True, the exponent is plotted
    xAxis: str
        choose variable on x axis (size, elevation or VRel)
    '''
    
    VRel = sizePar.praToVrel(ARel, elevation, cfgSize)[0]
    size = sizePar.praToVRelSize(ARel, elevation, cfgSize)
    if len(np.array(VRel).shape) == 0:
        VRel = np.ones_like(elevation) * VRel
    if len(np.array(size).shape) == 0:
        size = np.ones_like(elevation) * size

    alpha = sizePar.sizeToAlpha(size, elevation, cfgSize)
    umax = sizePar.sizeToUmax(size, elevation, cfgSize)
    exp = sizePar.sizeToExp(size, elevation, cfgSize)

    D0 = cfgSize.getfloat('D0')
    deltaD = cfgSize.getfloat('deltaD')

    if xAxis.lower() == 'elevation':
        variable = elevation
        label = 'elevation [m]'
    elif xAxis.lower() == 'vrel':
        variable = VRel
        label = 'release Volume [m³]'
    else:
        variable = size
        label = 'avalanche size'


    fig, ax1 = plt.subplots(figsize=(8, 8))
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("axes", 1.13))
    
    if expBool:
        ax4 = ax1.twinx()
        ax4.spines["right"].set_position(("axes", 1.3))

    ax3.plot(variable, size, color = 'g')
    ax1.plot(variable, alpha, color = 'r', label = 'alpha')
    ax2.plot(variable, umax, 'b--', linewidth=3, label = 'u_max')
    ax1.set_ylabel('alpha [°]')
    ax3.set_ylabel('avalanche size')
    ax2.set_ylabel('u_max [m/s]')

    ax1.set_xlabel(label)

    ax1.spines['left'].set_edgecolor('red')
    ax3.spines["right"].set_edgecolor('g')
    ax2.spines['right'].set_edgecolor('b')
    ax3.spines['bottom'].set_edgecolor('k')
    ax3.spines['top'].set_edgecolor('k')


    ax2.yaxis.label.set_color('b')
    ax1.yaxis.label.set_color('red')
    ax3.yaxis.label.set_color('g')


    ax2.tick_params(axis='y', colors='b')
    ax3.tick_params(axis='y', colors='g')
    ax1.tick_params(axis='y', colors='red')
    
    if expBool:
        ax4.plot(variable, exp, marker = '', color = 'm')
        ax4.set_ylabel('exponent')
        ax4.spines["right"].set_edgecolor('m')
        ax4.spines['bottom'].set_edgecolor('k')
        ax4.spines['top'].set_edgecolor('k')
        ax4.yaxis.label.set_color('m')
        ax4.tick_params(axis='y', colors='m')


    if cfgSize.getboolean('constantPraThickness'):
        labelD = 'PRA thickness: constant'
    else:
        labelD = 'PRA thickness: linear with elevation'
    if cfgSize.getboolean('constantTemperature'):
        labelT = 'temperature: constant'
    else:
        labelT = 'temperature: linear with elevation'
    if xAxis.lower() == 'elevation':
        plt.title(f'Release area: {ARel} $m^2$, snowclimate: {labelD}, {labelT}');

    return fig


def plotMuXi(cfgSize, cfgPlot, size=np.linspace(2,5,7), elevation = np.linspace(100,3500,7)):
    '''
    calculate and plot the friction parameters mu and xi and the FlowPy input parameters
    uMaxLim and alpha dependent on the avalanche size
    
    Parameters:
    -----------
    cfgSize: config Parser
        contains parameters for size parameterisation
    cfgPlot config Parser
        contains parameters for plots
    size: numpy array or float
        avalanche size of PRA cell
    elevation: numpy array
        elevation values of PRAs (default: 100-3500 m)

    '''
    
    alpha = sizePar.sizeToAlpha(size, elevation, cfgSize)
    umax = sizePar.sizeToUmax(size, elevation, cfgSize)

    fig, (ax1,ax3) = plt.subplots(1,2, figsize=(10,5), tight_layout=True)
    ax2 = ax1.twinx()

    ax1.plot(size, alpha, marker = 'o', color = 'r', label = 'alpha')
    ax2.plot(size, umax, marker = '*', color = 'b', label = 'u_max')
    ax1.set_ylabel('alpha [°]')
    ax2.set_ylabel('u_max [m/s]')
    ax1.set_xlabel('Avalanche size')
    plt.title(f"alpha(size=2) = {cfgSize['alphaSize2']}°, $\Delta$ alpha = {cfgSize['deltaAlpha']}°, \n uMax(size=2) = {cfgSize['uMaxSize2']} m/s, $\Delta$ uMax = {cfgSize['deltaUMax']} m/s")


    #ax2.spines["right"].set_edgecolor('b')
    ax1.spines['left'].set_edgecolor('red')
    ax2.spines['top'].set_edgecolor('k')
    ax2.spines['bottom'].set_edgecolor('k')
    ax2.spines['right'].set_edgecolor('b')

    ax2.yaxis.label.set_color('b')
    ax1.yaxis.label.set_color('red')

    ax2.tick_params(axis='y', colors='b')
    ax1.tick_params(axis='y', colors='red')

    d = cfgPlot.getfloat('d')
    mu, xi = muxi.testXiMu(alpha, umax, h=d, theta = cfgPlot.getfloat('theta'))

    ax5 = ax3.twinx()

    ax3.plot(size, mu, marker = 'o', color = 'r', label = 'mu')
    ax5.plot(size, xi, marker = '*', color = 'b', label = 'xi')
    ax3.set_ylabel('mu')
    ax5.set_ylabel('xi in [m/$s^2$]')
    ax3.set_xlabel('Avalanche size')

    #ax5.spines["right"].set_edgecolor('b')
    ax3.spines['left'].set_edgecolor('red')
    ax5.spines['top'].set_edgecolor('k')
    ax5.spines['bottom'].set_edgecolor('k')
    ax5.spines['right'].set_edgecolor('b')

    ax5.yaxis.label.set_color('b')
    ax3.yaxis.label.set_color('red')

    ax5.tick_params(axis='y', colors='b')
    ax3.tick_params(axis='y', colors='red')
    plt.suptitle('Check correlation of alpha, umax and corresponding mu and xi')

    return fig
