from abc import ABCMeta, abstractmethod
import QuantLib as ql
import numpy as np
import pandas as pd
import datetime
from PricingLibrary.Options import OptionPlainEuropean
from PricingLibrary.OptionMetrics import OptionMetrics
from PricingLibrary.Evaluation import Evaluation
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
from OptionStrategyLib.OptionPricing import OptionPricingUtil as util
import math


pu = PlotUtil()
eval_date = datetime.date(2018, 3, 1)
mdt = datetime.date(2018, 3, 28)
# spot = 2.8840  # 2018-3-1 50etf close price
spot = 2.9
strike = 2.9
strike2 = 3.0
strike3 = 2.8
vol = 0.3
rf = 0.03
maturitydt = ql.Date(mdt.day, mdt.month, mdt.year)
engineType = 'AnalyticEuropeanEngine'
mkt_call = {3.1: 0.0148, 3.0: 0.0317, 2.95: 0.0461, 2.9: 0.0641, 2.85: 0.095, 2.8: 0.127, 2.75: 0.1622}
mkt_put = {3.1: 0.2247, 3.0: 0.1424, 2.95: 0.1076, 2.9: 0.0751, 2.85: 0.0533, 2.8: 0.037, 2.75: 0.0249}
calendar = ql.China()
daycounter = ql.ActualActual()
evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
evaluation = Evaluation(evalDate, daycounter, calendar)

# calendar = ql.China()
# daycounter = ql.ActualActual()
# evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
# maturitydt = ql.Date(mdt.day, mdt.month, mdt.year)
# evaluation = Evaluation(evalDate, daycounter, calendar)
#
# option_call = OptionPlainEuropean(strike, maturitydt, ql.Option.Call)
#
# option_put = OptionPlainEuropean(strike, maturitydt, ql.Option.Put)
# option_call2 = OptionPlainEuropean(strike2, maturitydt, ql.Option.Call)
# metrics = OptionMetrics(option_call2, rf, engineType).set_evaluation(evaluation)
# iv = metrics.implied_vol(spot, option_price)
# vega = metrics.vega(spot, vol)
# vega2 = metrics.vega_effective(spot,vol)
# rho = metrics.rho(spot,vol)
# print(vega,vega2,rho)
# theta = metrics.theta(spot, iv)
#
# yield_curve = ql.FlatForward(evalDate,
#                              rf,
#                              daycounter)
# T = yield_curve.dayCounter().yearFraction(evalDate,
#                                           maturitydt)
# discount = yield_curve.discount(T)
# strikepayoff = ql.PlainVanillaPayoff(ql.Option.Call, strike)
# stddev = vol * math.sqrt(T)
# black = ql.BlackCalculator(strikepayoff, spot, stddev, discount)
# theta2 = black.theta(spot,T)/252
# print(iv, vega, theta,theta2)

yield_curve = ql.FlatForward(evalDate,
                             rf,
                             daycounter)

option_call = OptionPlainEuropean(strike, maturitydt, ql.Option.Call, mkt_call[strike])
option_call2 = OptionPlainEuropean(strike2, maturitydt, ql.Option.Call, mkt_call[strike2]) #虚值
option_call3 = OptionPlainEuropean(strike3,maturitydt,ql.Option.Call,mkt_call[strike3]) #实值

metricscall = OptionMetrics(option_call, rf, engineType).set_evaluation(evaluation)
metricscall2 = OptionMetrics(option_call2, rf, engineType).set_evaluation(evaluation)
metricscall3 = OptionMetrics(option_call3, rf, engineType).set_evaluation(evaluation)
iv_call = metricscall.implied_vol(spot, mkt_call[strike])
iv_call2 = metricscall2.implied_vol(spot, mkt_call[strike2])
iv_call3 = metricscall3.implied_vol(spot, mkt_call[strike3])

prices = []
prices2 = []
prices3 = []
x = []
# t = 0
while evalDate < maturitydt:
    p = metricscall.theta(spot,iv_call)
    p2 = metricscall2.theta(spot,iv_call2)
    p3 = metricscall3.theta(spot,iv_call3)
    prices.append(p)
    prices2.append(p2)
    prices3.append(p3)
    T = yield_curve.dayCounter().yearFraction(evalDate,maturitydt)*365.0
    x.append(T)
    evalDate = calendar.advance(evalDate, ql.Period(1, ql.Days))
    evaluation = Evaluation(evalDate, daycounter, calendar)
    metricscall.set_evaluation(evaluation)
    metricscall2.set_evaluation(evaluation)
    metricscall3.set_evaluation(evaluation)
    # t += 1
df_thetas = pd.DataFrame({'t':x,'theta-atm':prices,'theta-otm':prices2,'theta-itm':prices3}).sort_values(by='t')
df_thetas.to_csv('../save_results/df_thetas.csv')

