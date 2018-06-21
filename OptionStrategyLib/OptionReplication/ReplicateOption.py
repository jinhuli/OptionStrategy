import datetime
import pandas as pd
import numpy as np
import QuantLib as ql
from back_test.BktUtil import BktUtil
from OptionStrategyLib.OptionPricing import Options
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from data_access.get_data import get_future_mktdata, get_index_mktdata, get_index_intraday
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.Util import PricingUtil
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator


class ReplicateOption():
    def __init__(self, strike, dt_issue, dt_maturity, vol=0.2, rf=0.03, multiplier=1, fee=0.0):
        self.utl = BktUtil()
        self.pricing_utl = PricingUtil()
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
        self.fee = 0.0
        self.multiplier = multiplier
        self.pricing = None
        self.dt_eval = None

    def get_blackcalculator(self, dt_date, spot):
        stdDev = self.pricing_utl.get_blackcalculator_std(dt_date, self.dt_maturity, self.vol)
        discount = self.pricing_utl.get_discount(dt_date, self.dt_maturity, self.rf)
        black = BlackCalculator(self.strike, spot, stdDev, discount, False)
        # alpha is component shares of stock, N(d1) for call / -N(-d1) for put
        # beta id component shares of borrowing/lending -N(d2) for call / N(-d2) for put
        return black

    def replicate_put(self, dt_date, df_data):
        option = Options.OptionPlainEuropean(self.strike, self.maturitydt, ql.Option.Put)
        pricing = OptionMetrics(option, self.rf, self.engineType)
        discount = self.pricing_utl.get_discount(dt_date, self.dt_maturity, self.rf)
        spot = self.strike
        black = self.get_blackcalculator(dt_date, spot)
        replicate0 = black.Beta() * self.strike * discount + black.Alpha() * spot
        delta0 = black.delta(spot)
        option0 = black.NPV()
        cash = black.Beta() * self.strike * discount
        # delta_init, option_init = self.get_delta(dt_date, spot, option, pricing)
        # replication_init = - delta_init * spot
        tmp = self.dt_issue + datetime.timedelta(days=5)
        dttime_list = list(df_data[(df_data[self.utl.col_datetime] <= tmp) &
                                   (df_data[self.utl.col_datetime] > self.dt_issue)][self.utl.col_datetime])
        unit = -delta0
        cashflowA = unit * spot
        transaction_fee = 0.0
        replicate_value = 0.0
        option = 0.0
        replicate = 0.0
        replicate_pnl = 0.0
        option_pnl = 0.0
        print('-' * 150)
        print("%10s %20s %20s %20s %20s %20s %20s %20s" %
              ('日期', 'Spot', 'Delta', 'Cashflow A', 'Unit', 'replicate', 'option', 'total fee'))
        print('-' * 150)
        print("%10s %20s %20s %20s %20s %20s %20s %20s" %
              (dt_date, spot, round(delta0, 2), round(cash, 1), round(unit, 2), round(replicate0, 0),
               round(option0, 0), round(transaction_fee, 0)))
        for (i, dt_time) in enumerate(dttime_list):
            if i % 5 != 0: continue
            dt_time = pd.to_datetime(dt_time)
            dt = dt_time.date()
            spot = df_data[df_data[utl.col_datetime] == dt_time][utl.col_close].values[0]
            black = self.get_blackcalculator(dt_date, spot)

            replicate = black.Beta() * self.strike * discount + black.Alpha() * spot
            cash = black.Beta() * self.strike * discount
            stock = black.Alpha() * spot
            replicate_pnl = replicate - replicate0
            delta = black.delta(spot)
            option = black.NPV()
            option_pnl = option - option0
            unit = -delta
            # delta1, option_value = self.get_delta(dt, spot, option, pricing)
            # unit_chg = (delta - delta0)
            # cashflow = - unit_chg * spot
            # cashflowA += cashflow
            # delta0 = delta
            # transaction_fee += self.fee * abs(cashflow)
            # replicate_value = cashflowA + delta * spot - transaction_fee

            # replicate_pnl = unit * spot - replication_init
            print("%10s %20s %20s %20s %20s %20s %20s %20s" %
                  (dt_time, spot, round(delta, 2), round(cash, 1), round(unit, 2), round(replicate, 0),
                   round(option, 0), round(transaction_fee, 0)))
        print('-' * 150)
        print('init option : ', option0)
        print('init replication : ', replicate0)
        print('terminal option value : ', option)
        print('terminal replicate value : ', replicate)
        print('replication cost : ', (replicate - option) * 100 / option, '%')
        print('-' * 150)

    def replicate_put_daily(self, dt_date, df_data):
        option = Options.OptionPlainEuropean(self.strike, self.maturitydt, ql.Option.Put)
        pricing = OptionMetrics(option, self.rf, self.engineType)
        spot = df_data[df_data[utl.dt_date] == dt_date][utl.col_close].values[0]
        delta_init, option_init = self.get_delta(dt_date, spot, option, pricing)
        replication_init = - delta_init * spot
        dt_list = list(df_data[(df_data[self.utl.dt_date] <= self.dt_maturity) &
                               (df_data[self.utl.dt_date] >= self.dt_issue)][self.utl.dt_date])
        delta0 = 0.0
        cashflowA = 0.0
        transaction_fee = 0.0
        replicate_value = 0.0
        option_value = 0.0

        print('-' * 150)
        print("%10s %20s %20s %20s %20s %20s %20s %20s" %
              ('日期', 'Spot', 'Delta', 'Cashflow A', 'Unit', 'replicate', 'option', 'transaction fee A'))
        print('-' * 150)
        for (i, dt) in enumerate(dt_list):
            spot = df_index[df_index[utl.dt_date] == dt][utl.col_close].values[0]
            delta, option_value = self.get_delta(dt, spot, option, pricing)
            unit = -delta
            unit_chg = (delta - delta0)
            cashflow = - unit_chg * spot
            cashflowA += cashflow
            delta0 = delta
            transaction_fee += fee * cashflow
            replicate_value = cashflowA + delta * spot - transaction_fee
            replicate_pnl = unit * spot - replication_init
            option_pnl = option_value - option_init
            print("%10s %20s %20s %20s %20s %20s %20s %20s" %
                  (dt, spot, round(delta * 100, 2), round(cashflowA, 1), round(cashflowA, 0), round(replicate_value, 0),
                   round(option_value, 0), round(transaction_fee, 0)))
        print('-' * 150)
        print('init option : ', option_init)
        print('init replication : ', replication_init)
        print('terminal option value : ', option_value)
        print('terminal replicate value : ', replicate_value)
        print('replication cost (replicate_value - option_value) : ', replicate_value - option_value)
        print('-' * 150)

    """ Calculate delta by update evaluation """

    def get_delta(self, dt_date, spot, option, pricing):
        if self.dt_eval != dt_date:
            eval_date = self.utl.to_ql_date(dt_date)
            evaluation = Evaluation(eval_date, self.daycounter, self.calendar)
            pricing.set_evaluation(evaluation)
        self.dt_eval = dt_date
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
        return delta, option_price


plot_utl = PlotUtil()
utl = BktUtil()
name_code = 'IF'
id_index = 'index_300sh'
start_date = datetime.date(2018, 1, 1)
end_date = datetime.date.today()
df_index = get_index_mktdata(start_date, end_date, id_index)
df_intraday = get_index_intraday(start_date, end_date, id_index)
dt_issue = datetime.date(2018, 3, 1)
dt_maturity = datetime.date(2018, 4, 27)
vol = 0.2
rf = 0.03
fee = 5.0 / 10000.0
strike = df_index[df_index[utl.dt_date] == dt_issue][utl.col_close].values[-1]  # ATM Strike
replication = ReplicateOption(strike, dt_issue, dt_maturity)
replication.replicate_put(dt_issue, df_intraday)

# plot_utl.plot_line_chart(underlyings,[replicate_pnl],['replicate_values'])
# plot_utl.plot_line_chart(underlyings,[deltas],['delta'])
#
# plt.show()
