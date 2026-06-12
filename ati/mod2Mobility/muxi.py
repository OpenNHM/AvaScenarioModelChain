# AvaScenarioModelChain/in2Parameter/muxi.py
# Author: Paula Spannring (BFW)

import numpy as np

def testXiMu(alpha, umax, h = 2., theta = 35.):
    '''
    calculate mu and xi from alpha and umax
    (assume: h = 2 m, theta = 35°)
    
    Parameters:
    -----------
    alpha: numpy array or float
        alpha angle
    umax: numpy array or float
        maximum velocity limit
    h: float
        snow thickness (default: 2)
    theta: float
        slope angle

    Returns:
    -----------
    mu: numpy array
    xi: numpy array
    '''
    
    mu = np.tan(np.deg2rad(alpha))
    xi = umax**2 / h / (np.sin(np.deg2rad(theta)) - mu * np.cos(np.deg2rad(theta)))
    
    return mu, xi