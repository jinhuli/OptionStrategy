from data_access.get_data import get_future_mktdata, get_index_mktdata
from back_test.BktUtil import BktUtil
from OptionStrategyLib.OptionPricing import Options
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
import datetime
import pandas as pd
import numpy as np
import QuantLib as ql

utl = BktUtil()
name_code = 'IF'
id_index = 'index_300sh'
start_date = datetime.date(2018, 1, 1)
end_date = datetime.date.today()
df = get_future_mktdata(start_date, end_date, name_code)
df_future1 = utl.get_futures_c1(df)  # 基于成交量的主力连续
df_index = get_index_mktdata(start_date, end_date, id_index)

""" Option Basics """
issuedt = datetime.date(2018, 3, 1)
maturitydt = datetime.date(2018, 4, 27)
calendar = ql.China()
daycounter = ql.ActualActual()
engineType = 'AnalyticEuropeanEngine'
vol = 0.2
rf = 0.03
fee = 5.0 / 10000.0
spot = strike = df_index[df_index[utl.dt_date] == issuedt][utl.col_close].values[0]  # ATM Strike
option = Options.OptionPlainEuropean(strike, utl.to_ql_date(maturitydt), ql.Option.Put)
pricing = OptionMetrics(option, rf, engineType)
beg = utl.to_ql_date(issuedt)
end = utl.to_ql_date(maturitydt)

""" Underlying Holdings """
multiplier = 100
unit = 0.0  # 合约乘数100
portfolio = 100 * spot
dt_list = list(df_future1[(df_future1[utl.dt_date] <= maturitydt) &
                          (df_future1[utl.dt_date] >= issuedt)][utl.dt_date])
option_init = 0.0
delta = delta0 = 0.0
cashflowA = 0.0
transaction_fee = 0.0
replicate_value = 0.0
""" Replicate """
print('-' * 150)
print("%10s %20s %20s %20s %20s %20s %20s %20s" %
      ('日期', 'Spot', 'Delta', 'Cashflow', 'Unit', 'replicate value', 'option value', 'transaction fee'))
print('-' * 150)
for (i, dt) in enumerate(dt_list):
    eval_date = utl.to_ql_date(dt)
    evaluation = Evaluation(eval_date, daycounter, calendar)
    pricing.set_evaluation(evaluation)
    spot = df_index[df_index[utl.dt_date] == dt][utl.col_close].values[0]
    if dt == maturitydt:
        if strike > spot: delta = -1.0
        elif strike < spot: delta = 1.0
        else: delta = 0.5
        option_price = max(strike - spot, 0) * multiplier
    else:
        delta = pricing.delta(spot, vol)
        option_price = pricing.option_price(spot, vol) * multiplier
    if i == 0: option_init = option_price
    unit = delta * multiplier
    unit_chg = (delta - delta0) * multiplier
    cashflow = - unit_chg * spot
    cashflowA += cashflow
    delta0 = delta
    transaction_fee += fee * cashflow
    replicate_value = cashflowA + delta * spot * multiplier - transaction_fee
    print("%10s %20s %20s %20s %20s %20s %20s %20s" %
          (dt, spot, round(delta * 100, 2), round(cashflowA, 1), round(unit, 0), round(replicate_value, 0),
           round(option_price, 0), round(transaction_fee, 0)))

option_value = max((strike - spot) * multiplier, 0)
# replicate_value = cashflowA + delta * spot * multiplier - transaction_fee
print('-' * 150)
print('strike : ', strike * 100)
print('option init value : ', option_init)
print('option terminal value : ', option_value)
print('replicate terminal value : ', replicate_value)
print('replication cost : ', replicate_value - option_value)
print('-' * 150)




