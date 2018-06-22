import datetime
import pandas as pd
import numpy as np
from back_test.BktUtil import BktUtil
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from data_access.get_data import get_future_mktdata, get_index_mktdata, get_index_intraday
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.Util import PricingUtil
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator, EuropeanOption
from Utilities.calculate import calculate_histvol


class ReplicateOption():
    def __init__(self, strike, dt_issue, dt_maturity, vol=0.2, rf=0.03, multiplier=1, fee=5.0 / 10000.0):
        self.utl = BktUtil()
        self.pricing_utl = PricingUtil()
        self.strike = strike
        self.dt_issue = dt_issue
        self.dt_maturity = dt_maturity
        self.vol = vol
        self.rf = rf
        self.fee = fee
        self.multiplier = multiplier
        self.pricing = None
        self.dt_eval = None
        # self.df_vix = df_vix

    def synsetic_payoff(self):

        return

    def get_vol(self, dt_date, df_data):
        df_today = df_data[df_data[self.utl.col_date] == dt_date]
        if df_today.empty:
            df_today = df_data[df_data[self.utl.col_date] <= dt_date].iloc[0]
        vix = df_today[self.utl.col_close].values[0]
        return vix

    def calculate_hist_vol(self, cd_period, df_data):
        if cd_period == '1M':
            dt_start = self.dt_issue - datetime.timedelta(days=50)
            df = df_data[df_data[self.utl.col_date] >= dt_start]
            histvol = calculate_histvol(df[self.utl.col_close], 20)
            df[self.utl.col_close] = histvol
            df = df[[self.utl.col_date, self.utl.col_close]].dropna()
        else:
            return
        return df

    def replicate_put(self, df_data, df_vol):
        df_data[self.utl.col_date] = df_data[self.utl.col_datetime].apply(lambda x: datetime.date(x.year, x.month, x.day))
        df_data = df_data[(df_data[self.utl.col_date] > self.dt_issue)&(df_data[self.utl.col_date] <= self.dt_maturity)] # cut useful data
        dt_date = self.dt_issue # Use Close price of option issue date to start replication.
        Option = EuropeanOption(self.strike, self.dt_maturity, self.utl.type_put)
        spot = self.strike
        vol = self.get_vol(dt_date, df_vol)
        black = self.pricing_utl.get_blackcalculator(dt_date, spot, Option, self.rf, vol)
        option0 = black.NPV()
        cash = black.Cash()
        delta0 = black.Delta(spot)
        replicate0 = cash + delta0 * spot
        dt_times = df_data[self.utl.col_datetime]
        unit = -delta0
        transaction_fee = abs(delta0) * self.fee
        option_price = 0.0
        replicate = 0.0
        delta = 0.0 # Unit of underlyings/futures in replicate portfolio
        cash = 0.0 # cash in replicate portfolio
        dt_last = dt_date
        # print('-' * 150)
        # print("%10s %20s %20s %20s %20s %20s %20s %20s" %
        #       ('日期', 'Spot', 'Delta', 'Cashflow A', 'Unit', 'replicate', 'option', 'total fee'))
        # print('-' * 150)
        # print("%10s %20s %20s %20s %20s %20s %20s %20s" %
        #       (dt_date, spot, round(delta0, 2), round(cash, 1), round(unit, 2), round(replicate0, 0),
        #        round(option0, 0), round(transaction_fee, 0)))
        for (i, dt_time) in enumerate(dt_times):
            dt_time = pd.to_datetime(dt_time)
            if dt_time.time() < datetime.time(9, 30, 0) or dt_time.time() > datetime.time(15, 0, 0): continue
            if dt_time.minute % 5 != 0: continue  # 5min调整一次delta
            spot = df_data[df_data[self.utl.col_datetime] == dt_time][self.utl.col_close].values[0]
            if dt_time.date() == self.dt_maturity:
                replicate = cash + delta * spot
                delta, option_price = self.pricing_utl.get_maturity_metrics(dt_date, spot, Option)
                break
            else:
                vol = self.get_vol(dt_time.date(), df_vol)
                black = self.pricing_utl.get_blackcalculator(dt_time.date(), spot, Option, self.rf, vol)
                cash = black.Cash()
                delta = black.Delta(spot)
                replicate = cash + delta * spot
                option_price = black.NPV()
                unit = -delta
            transaction_fee += abs(delta - delta0) * self.fee
            delta0 = delta0
            replicate_pnl = replicate - replicate0
            option_pnl = option_price - option0
            if dt_time.date() != dt_last:
                cash = cash*(1+self.rf *self.pricing_utl.get_ttm(dt_last,dt_time.date()))
                dt_last = dt_time.date()
            # print("%10s %20s %20s %20s %20s %20s %20s %20s" %
            #       (dt_time, spot, round(delta, 2), round(cash, 1), round(unit, 2), round(replicate, 0),
            #        round(option_price, 0), round(transaction_fee, 2)))
        pct_replication_cost = (option_price - replicate + transaction_fee) / option0
        # print('-' * 150)
        # print('init option : ', option0)
        # print('init replication : ', replicate0)
        # print('terminal option value : ', option_price)
        # print('terminal replicate value : ', replicate)
        # print('replication cost : ', pct_replication_cost * 100, '%')
        # print('-' * 150)
        return pct_replication_cost

        # def replicate_put_daily(self, dt_date, df_data):
        #     option = Options.OptionPlainEuropean(self.strike, self.maturitydt, ql.Option.Put)
        #     pricing = OptionMetrics(option, self.rf, self.engineType)
        #     spot = df_data[df_data[utl.dt_date] == dt_date][utl.col_close].values[0]
        #     delta_init, option_init = self.get_delta(dt_date, spot, option, pricing)
        #     replication_init = - delta_init * spot
        #     dt_list = list(df_data[(df_data[self.utl.dt_date] <= self.dt_maturity) &
        #                            (df_data[self.utl.dt_date] >= self.dt_issue)][self.utl.dt_date])
        #     delta0 = 0.0
        #     cashflowA = 0.0
        #     transaction_fee = 0.0
        #     replicate_value = 0.0
        #     option_value = 0.0
        #
        #     print('-' * 150)
        #     print("%10s %20s %20s %20s %20s %20s %20s %20s" %
        #           ('日期', 'Spot', 'Delta', 'Cashflow A', 'Unit', 'replicate', 'option', 'transaction fee A'))
        #     print('-' * 150)
        #     for (i, dt) in enumerate(dt_list):
        #         spot = df_index[df_index[utl.dt_date] == dt][utl.col_close].values[0]
        #         delta, option_value = self.get_delta(dt, spot, option, pricing)
        #         unit = -delta
        #         unit_chg = (delta - delta0)
        #         cashflow = - unit_chg * spot
        #         cashflowA += cashflow
        #         delta0 = delta
        #         transaction_fee += fee * cashflow
        #         replicate_value = cashflowA + delta * spot - transaction_fee
        #         replicate_pnl = unit * spot - replication_init
        #         option_pnl = option_value - option_init
        #         print("%10s %20s %20s %20s %20s %20s %20s %20s" %
        #               (dt, spot, round(delta * 100, 2), round(cashflowA, 1), round(cashflowA, 0), round(replicate_value, 0),
        #                round(option_value, 0), round(transaction_fee, 0)))
        #     print('-' * 150)
        #     print('init option : ', option_init)
        #     print('init replication : ', replication_init)
        #     print('terminal option value : ', option_value)
        #     print('terminal replicate value : ', replicate_value)
        #     print('replication cost (replicate_value - option_value) : ', replicate_value - option_value)
        #     print('-' * 150)


# plot_utl = PlotUtil()
# utl = BktUtil()
# name_code = 'IF'
# id_index = 'index_300sh'
# dt_issue = datetime.date(2018, 3, 1)
# dt_maturity = datetime.date(2018, 4, 13)
# dt_start = dt_issue - datetime.timedelta(days=50)
# dt_end = dt_maturity
# df_index = get_index_mktdata(dt_start, dt_end, id_index)
# df_intraday = get_index_intraday(dt_start, dt_end, id_index)
# df_vix = get_index_mktdata(dt_start, dt_end, 'index_cvix')
# df_vix[utl.col_close] = df_vix[utl.col_close] / 100.0
# vol = 0.2
# rf = 0.03
# fee = 5.0 / 10000.0
# strike = df_index[df_index[utl.dt_date] == dt_issue][utl.col_close].values[-1]  # ATM Strike
# replication = ReplicateOption(strike, dt_issue, dt_maturity, df_vix=df_vix)
# df_vol = replication.calculate_hist_vol('1M', df_index)
# replication.replicate_put(dt_issue, df_intraday, df_vol)
