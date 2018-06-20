import datetime
import pandas as pd
import numpy as np
import QuantLib as ql
from back_test.BktUtil import BktUtil
from OptionStrategyLib.OptionPricing import Options
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from data_access.get_data import get_future_mktdata, get_index_mktdata
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil

class ReplicateOption():
    def __init__(self, strike, dt_issue, dt_maturity, vol=0.2, rf=0.03, multiplier=100):
        self.utl = BktUtil()
        self.calendar = ql.China()
        self.daycounter = ql.ActualActual()
        self.engineType = 'AnalyticEuropeanEngine'
        self.strike = strike
        self.dt_issue = dt_issue
        self.dt_maturity = dt_maturity
        self.issuedt = self.utl.to_ql_date(dt_issue)
        self.maturitydt = self.utl.to_ql_date(dt_maturity)
        self.vol = vol
        self.rf = rf
        self.multiplier = multiplier
        self.pricing = None

    def replicate_put(self, dt_date,df_data):
        option = Options.OptionPlainEuropean(self.strike, self.maturitydt, ql.Option.Put)
        pricing = OptionMetrics(option, self.rf, self.engineType)
        spot = df_index[df_index[utl.dt_date] == dt_date][utl.col_close].values[0]
        delta_init, option_init = self.get_delta(dt_date, spot, option, pricing)
        replication_init = - delta_init * spot
        dt_list = list(df_data[(df_data[self.utl.dt_date] <= self.dt_maturity) &
                                  (df_data[self.utl.dt_date] >= self.dt_issue)][self.utl.dt_date])
        delta0 = 0.0
        cashflowA = 0.0
        transaction_fee = 0.0
        replicate_value = 0.0
        print('init option : ', option_init)
        print('init replication : ', replication_init)
        print('-' * 150)
        print("%10s %20s %20s %20s %20s %20s %20s %20s" %
              ('日期', 'Spot', 'Delta', 'Cashflow', 'Unit', 'replicate pnl', 'option pnl', 'transaction fee'))
        print('-' * 150)
        for (i, dt) in enumerate(dt_list):
            spot = df_index[df_index[utl.dt_date] == dt][utl.col_close].values[0]
            delta,option_price = self.get_delta(dt,spot,option,pricing)
            unit = delta * self.multiplier
            unit_chg = (delta - delta0) * self.multiplier
            cashflow = - unit_chg * spot
            cashflowA += cashflow
            delta0 = delta
            transaction_fee += fee * cashflow
            replicate_value = cashflowA + delta * spot * self.multiplier - transaction_fee
            replicate_pnl = cashflowA-replication_init
            option_pnl = option_price - option_init
            print("%10s %20s %20s %20s %20s %20s %20s %20s" %
                  (dt, spot, round(delta * 100, 2), round(cashflowA, 1), round(unit, 0), round(replicate_pnl, 0),
                   round(option_pnl, 0), round(transaction_fee, 0)))


    def get_delta(self, dt_date, spot, option, pricing):
        eval_date = self.utl.to_ql_date(dt_date)
        evaluation = Evaluation(eval_date, self.daycounter, self.calendar)
        pricing.set_evaluation(evaluation)
        if dt_date == self.dt_maturity:
            if strike > spot:
                delta = -1.0
            elif strike < spot:
                delta = 1.0
            else:
                delta = 0.5
            option_price = max(strike - spot, 0) * self.multiplier
        else:
            delta = pricing.delta(spot, vol)
            option_price = pricing.option_price(spot, vol) * self.multiplier
        delta = delta * self.multiplier
        # if option.optionType == ql.Option.Put:
        #     if spot >= self.strike:
        #         delta = 0.0
        #     else:
        #         eval_date = self.utl.to_ql_date(dt_date)
        #         evaluation = Evaluation(eval_date, self.daycounter, self.calendar)
        #         pricing.set_evaluation(evaluation)
        #         delta = pricing.delta(spot, self.vol)
        # else:
        #     return
        return delta,option_price

plot_utl = PlotUtil()
utl = BktUtil()
name_code = 'IF'
id_index = 'index_300sh'
start_date = datetime.date(2018, 1, 1)
end_date = datetime.date.today()
df_index = get_index_mktdata(start_date, end_date, id_index)
dt_issue = datetime.date(2018, 3, 1)
dt_maturity = datetime.date(2018, 4, 27)
vol = 0.2
rf = 0.03
fee = 5.0 / 10000.0
strike = df_index[df_index[utl.dt_date] == dt_issue][utl.col_close].values[0]  # ATM Strike
replication = ReplicateOption(strike, dt_issue, dt_maturity)
replication.replicate_put(dt_issue,df_index)

# plot_utl.plot_line_chart(underlyings,[replicate_pnl],['replicate_values'])
# plot_utl.plot_line_chart(underlyings,[deltas],['delta'])
#
# plt.show()
