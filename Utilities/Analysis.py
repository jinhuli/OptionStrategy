# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 17:29:49 2018
@author: xhlidzqh
"""
import numpy as np
import pandas as pd


def get_netvalue_analysis(netvalue, freq='D'):
    '''由净值序列进行指标统计,netvalue应为Series'''
    if freq == 'D':
        oneyear = 252
    elif freq == 'W':
        oneyear = 50
    elif freq == 'M':
        oneyear = 12
    else:
        print('Not Right freq')
    # 交易次数
    tradeslen = netvalue.shape[0]
    # 收益率序列
    tmp = netvalue.shift()
    tmp[0] = 1
    returns = netvalue / tmp - 1
    # 累计收益率
    totalreturn = netvalue.iloc[-1] - 1
    # 年化收益率
    return_yr = (1 + totalreturn) ** (oneyear / tradeslen) - 1
    # 年化波动率
    volatility_yr = np.std(returns, ddof=0) * np.sqrt(oneyear)
    # 夏普比率
    sharpe = (return_yr - 0.024) / volatility_yr
    # 回撤
    drawdowns = get_maxdrawdown(netvalue)
    # 最大回撤
    maxdrawdown = min(drawdowns)
    # 收益风险比
    profit_risk_ratio = return_yr / np.abs(maxdrawdown)
    # 盈利次数
    win_count = (returns >= 0).sum()
    # 亏损次数
    lose_count = (returns < 0).sum()
    # 胜率
    win_rate = win_count / (win_count + lose_count)
    # 盈亏比
    p_over_l = returns[returns > 0].mean() / np.abs(returns[returns < 0].mean())
    r = pd.Series()
    r['累计收益率'] = totalreturn
    r['年化收益率'] = return_yr
    r['年化波动率'] = volatility_yr
    r['最大回撤率'] = maxdrawdown
    r['胜率(' + freq + ')'] = win_rate
    r['盈亏比'] = p_over_l
    r['夏普比率'] = sharpe
    r['Calmar比'] = profit_risk_ratio

    return r


def get_maxdrawdown(netvalue):
    '''
    最大回撤率计算
    '''
    maxdrawdowns = pd.Series(index=netvalue.index)
    for i in np.arange(len(netvalue.index)):
        highpoint = netvalue.iloc[0:(i + 1)].max()
        if highpoint == netvalue.iloc[i]:
            maxdrawdowns.iloc[i] = 0
        else:
            maxdrawdowns.iloc[i] = netvalue.iloc[i] / highpoint - 1


    return maxdrawdowns