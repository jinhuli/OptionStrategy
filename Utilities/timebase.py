#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-03-20 13:45:02
# @Author  : Xiaohui (${xiaohui.li@orientfutures.com})

'''
几种价格“趋势线”的方法
'''
# import talib
import pandas as pd
import numpy as np

from pykalman import KalmanFilter

# =============================================================================
# 1. 局域线性核回归
# =============================================================================
def LLKSR(ts, d, f='gau', t='F'):
    # 局域线性核回归:f为核函数，t为平滑(S)或滤波(F)
    x = np.arange(ts.shape[0])
    y_est = pd.Series(index=ts.index)

    X_fit = x[:].copy()
    Y_fit = ts.values.copy()
    Y_fit[np.isnan(Y_fit)] = 0

    if t=='S':
        start = 0
    else:
        start = d - 1 + ts.isnull().sum()
    for i in np.arange(start, len(x)):
        x0 = x[i]
        xx = X_fit - x0
        # ksr 核心算法，权重分布和核函数、数据位置有关
        s0 = np.sum(KernelFun(xx/d, f, t))
        s1 = np.sum(xx * KernelFun(xx/d, f, t))
        s2 = np.sum(xx**2 * KernelFun(xx/d, f, t))
        w = (s2 - s1*xx) * KernelFun(xx/d, f, t) / (s2*s0 - s1**2)
        y_est[i] = np.sum(w * Y_fit)
    return y_est

def KernelFun(x, f, t):
    '''核函数公式，f为核函数类型，t为平滑S或滤波F（影响权重分布的范围）'''

    if f == 'gau': # 高斯核
        K = lambda u: np.exp(- u * u / 2) / np.sqrt(2 * np.pi)
        return K(x) * Indicator(x, np.inf, t)
    elif f == 'log': # logistic
        K = lambda u: 1 / (np.exp(u) + 2 + np.exp(-u))
        return K(x) * Indicator(x, np.inf, t)
    elif f == 'ep': # epanechnikov
        K = lambda u: 3/4 * (1-u**2)
        return K(x) * Indicator(x, 1, t)
    elif f == 'mean': # mena
        K = lambda u: (0.5 + u - u)
        return K(x) * Indicator(x, 1, t)
    elif f == 'quar': # quartic
        K = lambda u: 15/16 * (1 - u**2)**2
        return K(x) * Indicator(x, 1, t)
    elif f == 'sig': # sigmoid
        K = lambda u: 2/np.pi * 1/(np.exp(u) + np.exp(-u))
        return K(x) * Indicator(x, np.inf, t)
    else:
        raise ValueError('公式类型错误')

def Indicator(x, limit, t):
    # 示性函数，上界、下界（含）之内返回1，其他返回0
    indicator = np.zeros(x.shape)
    if t == 'F':# 平滑的话包括t0时候之后的数据，滤波则只有t0和t0之前的数据
        indicator[(- limit < x) & (x <= 0)] = 1
    elif t == 'S':
        indicator[(- limit < x) & (x <= limit)] = 1

    return indicator

# =============================================================================
# 2. 简单均线
# =============================================================================
def MA(ts, d):
    ''' 移动平均线 '''
    tmp = ts.rolling(d).mean()
    return tmp

# =============================================================================
# 3. 卡尔曼滤波
# =============================================================================
def KALMAN(ts, d):
    '''卡尔曼滤波'''
    sigma = (2/(d+1))**2
    kf = KalmanFilter(transition_covariance=sigma,
                      transition_matrices=[1],
                      observation_covariance=1,
                      observation_matrices=[1],
                      initial_state_covariance=1,
                      initial_state_mean=0)
    tmp = kf.filter(ts.values)[0]
    tmp = pd.Series(tmp[:, 0], index=ts.index)
    tmp[:2*d] = np.nan
    return tmp

# =============================================================================
# 4. MACD
# =============================================================================
# def MACD(ts, d):
#     '''MACD, 计算dif所用的快慢均线参数固定为12日、26日，d为计算dea时的均线参数'''
#     dif, dea, macd = talib.MACD(ts.values, fastperiod=12, slowperiod=26, signalperiod=d)
#     dif = pd.Series(dif, index=ts.index)
#     dea = pd.Series(dea, index=ts.index)
#     macd = pd.Series(macd, index=ts.index)
#     return dif, dea, macd

# =============================================================================
# 5. 奇异谱分析Singular Spectrum Analysis
# =============================================================================
def SSA(ts, d, t='F'):
    '''奇异谱分析'''
    if t == 'S':  # 如果只是对曲线拟合、平滑
        # 构建轨迹矩阵
        n = ts.shape[0]
        k = n - d + 1
        A = np.zeros((d, k))
        for i in np.arange(d):
            A[i, :] = ts.values[i:i+k]

        u, s, vh = np.linalg.svd(A, full_matrices=False)
        Ar = s[0]* np.matrix(u[:, 0]).T * np.matrix( vh[0,:])
        tmp = pd.DataFrame(index=np.arange(d), columns=np.arange(n), dtype=float).values
        for i in np.arange(d):
            tmp[i, i:n-d+1+i] = Ar[i, :]
        tmp = pd.Series(np.nanmean(tmp, axis=0), index=ts.index)
    else:
        n = 2*d
        k = n - d + 1
        tmp = pd.Series(index=ts.index)
        for i in np.arange(n, ts.shape[0]):
            tmp_tmp = ts.values[i-n:i]
            A = np.zeros((d, k))
            for j in np.arange(d):
                A[j, :] = tmp_tmp[j:j+k]

            u, s, vh = np.linalg.svd(A, full_matrices=False)
            Ar = s[0]* np.matrix(u[:, 0]).T * np.matrix( vh[0,:])
            tmp.iloc[i] = Ar[-1, -1]
    return tmp

# =============================================================================
# 6. 低延迟趋势线LLT: Low Lag Trend
# =============================================================================
def LLT(ts, d):
    '''低延迟趋势线LLT'''
    llt = ts.copy()
    a = 2/(d+1)
    for i in np.arange(ts.isnull().sum()+2, llt.shape[0]):
        if not np.isnan(ts[i-2]):
            llt[i] = (a-a*a/4)*ts[i] + a*a*ts[i-1]/2 - (a-3*a*a/4)*ts[i-2] + 2*(1-a)*llt[i-1] - (1-a)**2*llt[i-2]
    llt[:(ts.isnull().sum()+d)] = np.nan
    return llt

# =============================================================================
# 7. 线性多项式拟合趋势线: Linear Fitting Trend
# =============================================================================
def LFT(ts, d):
    '''线性多项式拟合趋势线'''
    x = np.arange(d)
    tmp = pd.Series(index=ts.index)
    for i in tmp.index[d:]:
        y = ts.loc[:i][-d:]
        ma = y.mean()
        y = y - ma
        p = np.polyfit(x, y, 4)
        tmp.loc[i] = np.polyval(p, x)[-1]
    return tmp

# =============================================================================
# 8. 停损指标SAR: Stop And Reverse
# =============================================================================
# def SAR(high, low):
#     '''停损指标'''
#     sar = talib.SAR(high.values, low.values, acceleration=0.02, maximum=0.2)
#     sar = pd.Series(sar, index=high.index)
#     return sar

