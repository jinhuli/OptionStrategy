import math
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime
import os
import pickle
import QuantLib as ql
from WindPy import w
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from data_access.db_tables import DataBaseTables as dbt
from OptionStrategyLib.calibration import SVICalibration
from OptionStrategyLib.Util import PricingUtil


def calculate_implied_vol(ql_optiontype, ql_mdt, strike, spot, option_price):
    exercise = ql.EuropeanExercise(ql_mdt)
    payoff = ql.PlainVanillaPayoff(ql_optiontype, strike)
    option = ql.EuropeanOption(payoff, exercise)
    flat_vol_ts = ql.BlackVolTermStructureHandle(
        ql.BlackConstantVol(ql_evalDate, calendar, 0.0, daycounter))
    dividend_ts = ql.YieldTermStructureHandle(ql.FlatForward(ql_evalDate, 0.0, daycounter))
    yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(ql_evalDate, rf, daycounter))
    process = ql.BlackScholesMertonProcess(ql.QuoteHandle(ql.SimpleQuote(spot)), dividend_ts, yield_ts,
                                           flat_vol_ts)
    option.setPricingEngine(ql.AnalyticEuropeanEngine(process))

    try:
        implied_vol = option.impliedVolatility(option_price, process, 1.0e-4, 300, 0.0, 10.0)
    except RuntimeError:
        implied_vol = 0.0
    return implied_vol


##################################################################################################
evalDate = datetime.date(2017, 12, 8)
ql_evalDate = ql.Date(evalDate.day, evalDate.month, evalDate.year)
rf = 0.03
calendar = ql.China()
daycounter = ql.ActualActual()
ql.Settings.instance().evaluationDate = ql_evalDate

##################################################################################################
# ql_mdt = calendar.advance(ql_evalDate,ql.Period(1,ql.Months))
#
# spot_300 = 3510.9845
# spot_50 = 2479.9115
# spot_500 = 5217.7643
# strike_300 = spot_300
# strike_50 = spot_50
# strike_500 = spot_500
# option_300_call = spot_300*5.46/100.0
# option_300_put = spot_300*2/100.0
# option_50_call = spot_50*2/100.0
# option_50_put = spot_50*2/100.0
# option_500_call = spot_500*5.13/100.0
# option_500_put = spot_500*7.48/100.0
# iv_300_call = calculate_implied_vol(ql.Option.Call,ql_mdt,strike_300,spot_300,option_300_call)
# iv_300_put = calculate_implied_vol(ql.Option.Put,ql_mdt,strike_300,spot_300,option_300_put)
# iv_50_call = calculate_implied_vol(ql.Option.Call,ql_mdt,strike_50,spot_50,option_50_call)
# iv_50_put = calculate_implied_vol(ql.Option.Put,ql_mdt,strike_50,spot_50,option_50_put)
# iv_500_call = calculate_implied_vol(ql.Option.Call,ql_mdt,strike_500,spot_500,option_500_call)
# iv_500_put = calculate_implied_vol(ql.Option.Put,ql_mdt,strike_500,spot_500,option_500_put)
# print('iv_300_call : ',iv_300_call)
# print('iv_300_put : ',iv_300_put)
# print('iv_50_call : ',iv_50_call)
# print('iv_50_put : ',iv_50_put)
# print('iv_500_call : ',iv_500_call)
# print('iv_500_put : ',iv_500_put)
# print(iv_300_call)
# print(iv_300_put)
# print(iv_50_call)
# print(iv_50_put)
# print(iv_500_call)
# print(iv_500_put)
##################################################################################################
ql_mdt = calendar.advance(ql_evalDate, ql.Period(1, ql.Months))
spot = 3000
k1 = 0.9 * spot
k2 = 0.95 * spot
k3 = 1.0 * spot
k4 = 1.05 * spot
k5 = 1.1 * spot
# p1 = spot * 0.1 / 100.0
# p2 = spot * 0.4 / 100.0
# p3 = spot * 2.0 / 100.0
# p4 = spot * 8.3 / 100.0
# p5 = spot * 12.5 / 100.0
# iv1 = calculate_implied_vol(ql.Option.Put, ql_mdt, k1, spot, p1) * 100
# iv2 = calculate_implied_vol(ql.Option.Put, ql_mdt, k2, spot, p2) * 100
# iv3 = calculate_implied_vol(ql.Option.Put, ql_mdt, k3, spot, p3) * 100
# iv4 = calculate_implied_vol(ql.Option.Put, ql_mdt, k4, spot, p4) * 100
# iv5 = calculate_implied_vol(ql.Option.Put, ql_mdt, k5, spot, p5) * 100
# print('-'*100)
# print(iv1)
# print(iv2)
# print(iv3)
# print(iv4)
# print(iv5)
# print('-'*100)

k = k5
p1 = spot * 10.5 / 100.0
p2 = spot * 10.7 / 100.0
p3 = spot * 10.2 / 100.0
p4 = spot * 12.5 / 100.0
p5 = spot * 9.5 / 100.0
iv1 = calculate_implied_vol(ql.Option.Put, ql_mdt, k, spot, p1) * 100
iv2 = calculate_implied_vol(ql.Option.Put, ql_mdt, k, spot, p2) * 100
iv3 = calculate_implied_vol(ql.Option.Put, ql_mdt, k, spot, p3) * 100
iv4 = calculate_implied_vol(ql.Option.Put, ql_mdt, k, spot, p4) * 100
iv5 = calculate_implied_vol(ql.Option.Put, ql_mdt, k, spot, p5) * 100
print('-' * 100)
print(iv1)
print(iv2)
print(iv3)
print(iv4)
print(iv5)
print('-' * 100)
