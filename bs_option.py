# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 14:12:33 2019

@author: admin
"""

from math import log,sqrt,exp
from scipy import stats
from random import gauss,seed
from time import time

def bs_call_value(S0,K,T,r,sigma):
    s0 = float(S0)
    d1 = (log(s0/K) + (r + sigma*sigma*0.5)*T)/(sigma*sqrt(T))
    d2 = (log(s0/K) + (r - sigma*sigma*0.5)*T)/(sigma*sqrt(T))
    value = s0 * stats.norm.cdf(d1,0.0,1.0) - K * exp(-r*T)*stats.norm.cdf(d2,0.0,1)
    return value

def bs_vega(S0,K,T,r,sigma):
    s0 = float(S0)
    d1 = ((log(s0/K) + (r + 0.5*sigma*sigma)*T))/(sigma*sqrt(T))
    vega = s0 * stats.norm.cdf(d1,0.0,1.0)*sqrt(T)
    return vega

def bs_call_imp_vol(S0,K,T,r,C0,sigma_est,it=100):
    '''Implied volatility of European call option in BS model.
    Parameters
    =============
    S0 : float
        initaial stock/index/ level
    K : float
        Strike price
    T : float
        maturity date( in year fractions)
    r : float 
        constant risk-free short rate
    sigma_est : float
        estimate of impl . volatility
    it : integer
        number of iterations
        
    Returns
    ==============
    simga_est : float
        numerically estimated implied volatility
    '''
    
    for i in range(it):
        sigma_est -= ((bs_call_value(S0,K,T,r,sigma_est) - C0) / bs_vega(S0,K,T,r,sigma_est))
    return sigma_est

def monte_carlo_valuation():
    seed(20000)
    t0 = time()
    
    S0 = 2.982
    K = 2.7
    T = 0.08333
    r = 0.03
    sigma = 0.1374
    M = 50
    dt = T/M
    I = 250000
    S = []
    
    for i in range(I):
        path = []
        for t in range(M + 1):
            if t == 0:
                path.append(S0)
            else:
                z = gauss(0.0,1.0)
                St = path[t - 1] * exp((r - 0.5*sigma*sigma) * dt + sigma *sqrt(dt) *z)
                path.append(St)
        S.append(path)
    
    C0 = exp(-r*T)*sum([max(path[-1] - K,0) for path in S]) / I
    tpy = time() - t0
    print( "European Option Value %7.3f" % C0)
    print("Duration in Seconds %7.3f" % tpy)
    

if __name__ == '__main__':
    print("bs option function : " + str(bs_call_value(2.982,2.7,0.083333,0.03,0.1374)))
    monte_carlo_valuation()