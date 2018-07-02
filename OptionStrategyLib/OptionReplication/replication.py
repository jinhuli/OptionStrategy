import datetime
import pandas as pd
import numpy as np
import math
from back_test.BktUtil import BktUtil
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from data_access.get_data import get_future_mktdata, get_index_mktdata, get_index_intraday, get_dzqh_cf_daily, \
    get_dzqh_cf_minute
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.Util import PricingUtil
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator, EuropeanOption
from Utilities.calculate import calculate_histvol


class Replication():
    def __init__(self, strike, dt_issue, dt_maturity, vol=0.2, rf=0.03,
                 multiplier=1, fee=2 / 10000.0, margin_rate=0.15, slippage=0.6):
        self.utl = BktUtil()
        self.pricing_utl = PricingUtil()
        self.strike = strike
        self.dt_issue = dt_issue
        self.dt_maturity = dt_maturity
        self.vol = vol
        self.rf = rf
        self.fee = fee
        self.margin_rate = margin_rate
        self.multiplier = multiplier
        self.pricing = None
        self.dt_eval = None
        self.cash = 0
        self.dela = 0
        self.margin = 0
        # self.df_vix = df_vix

    # def synsetic_payoff(self, df_data, df_vol):
    #     strikes = np.arange(self.strike * 0.8, self.strike * 1.2, 100.0)
    #     option_payoffs = []
    #     replicate_payoffs = []
    #     for k in strikes:
    #         self.strike = k
    #         c, option, replicate = self.replicate_put(df_data, df_vol)
    #         option_payoffs.append(option)
    #         replicate_payoffs.append(replicate)
    #     return strikes, option_payoffs, replicate_payoffs

    def get_vol(self, dt_date, df_data):
        if isinstance(df_data, float):
            return df_data
        vix = df_data[df_data[self.utl.col_date] <= dt_date][self.utl.col_close].values[-1]
        return vix

    # def replicate_continuous(self, df_data, df_vol,df_underlying=None):
    #     self.cash = cash = 0
    #     self.dela = 0
    #     self.margin = 0
    #     total_change_pnl = 0.0
    #     res = []
    #     df_data[self.utl.col_date] = df_data[self.utl.col_datetime].apply(
    #         lambda x: datetime.date(x.year, x.month, x.day))
    #     dt_date = self.dt_issue  # Use Close price of option issue date to start replication.
    #     Option = EuropeanOption(self.strike, self.dt_maturity, self.utl.type_put)
    #     # tmp = df_data[df_data[self.utl.col_date] == dt_date][[self.utl.col_datetime, self.utl.col_close]]
    #     spot = df_data[df_data[self.utl.col_date] == dt_date][self.utl.col_close].values[-1]
    #     vol = self.get_vol(dt_date, df_vol)
    #     # print(dt_date, vol)
    #     black = self.pricing_utl.get_blackcalculator(dt_date, spot, Option, self.rf, vol)
    #     option0 = black.NPV()
    #     # cash = black.Cash()
    #     delta0 = black.Delta(spot)
    #     asset = delta0 * spot
    #     replicate0 = cash + asset
    #     margin = abs(delta0) * spot * self.margin_rate
    #     unit = -delta0
    #     transaction_fee = abs(delta0) * spot * self.fee
    #     pct_cost = transaction_fee / option0
    #     res.append({'dt': datetime.datetime(dt_date.year, dt_date.month, dt_date.day, 15, 0, 0), 'spot': spot,
    #                 'port cash': cash, 'port asset': delta0 * spot,
    #                 'delta': delta0, 'margin': margin,
    #                 'value option': option0, 'value replicate': replicate0,
    #                 'replication cost': transaction_fee,
    #                 'transaction fee': transaction_fee, 'pnl option': 0,
    #                 'pnl replicate': 0, 'pct cost': pct_cost,'change_pnl':0,'replicate error':0
    #                 })
    #     delta = delta0  # Unit of underlyings/futures in replicate portfolio
    #     dt_last = dt_date
    #     df_data = df_data[(df_data[self.utl.col_date] > self.dt_issue) & (
    #         df_data[self.utl.col_date] <= self.dt_maturity)]  # cut useful data
    #     dt_times = df_data[self.utl.col_datetime]
    #
    #     # print('-' * 150)
    #     # print("%10s %20s %20s %20s %20s %20s %20s %20s" %
    #     #       ('日期', 'Spot', 'Delta', 'Cashflow A', 'Unit', 'replicate', 'option', 'total fee'))
    #     # print('-' * 150)
    #     # print("%10s %20s %20s %20s %20s %20s %20s %20s" %
    #     #       (dt_date, spot, round(delta0, 2), round(cash, 1), round(unit, 2), round(replicate0, 0),
    #     #        round(option0, 0), round(transaction_fee, 0)))
    #     for (i, dt_time) in enumerate(dt_times):
    #         dt_time = pd.to_datetime(dt_time)
    #         spot = df_data[df_data[self.utl.col_datetime] == dt_time][self.utl.col_close].values[0]
    #
    #         if dt_time.time() < datetime.time(9, 30, 0) or dt_time.time() > datetime.time(15, 0, 0): continue
    #         if dt_time.minute % 5 != 0: continue  # 5min调整一次delta
    #         if dt_time.date() == self.dt_maturity:
    #             # replicate = cash + delta * spot
    #             delta, option_price = self.pricing_utl.get_maturity_metrics(dt_date, spot, Option)
    #             # d_asset = (delta - delta0) * spot
    #             # asset += d_asset
    #             # transaction_fee += abs(delta - delta0) * spot * self.fee
    #             # replicate = cash + asset
    #             # replicate_pnl = delta * spot - replicate - transaction_fee
    #             # option_pnl = option_price - option0
    #             # replicate_cost = -replicate_pnl + option_pnl
    #             # margin = abs(delta) * spot * self.margin_rate
    #             # delta0 = delta
    #             # pct_cost = replicate_cost / option0
    #             # res.append({'dt': dt_time, 'spot': spot,
    #             #             'port cash': cash, 'port asset': delta * spot,
    #             #             'delta': delta, 'margin': margin,
    #             #             'value option': option_price, 'value replicate': replicate,
    #             #             'replication cost': replicate_cost,
    #             #             'transaction fee': transaction_fee, 'pnl option': option_pnl,
    #             #             'pnl replicate': replicate_pnl, 'pct cost': pct_cost
    #             #             })
    #         else:
    #             vol = self.get_vol(dt_time.date(), df_vol)
    #             black = self.pricing_utl.get_blackcalculator(dt_time.date(), spot, Option, self.rf, vol)
    #             # cash = black.Cash()
    #             delta = black.Delta(spot)
    #             option_price = black.NPV()
    #         # unit = -delta
    #         change_pnl = 0.0
    #         if i > 10 and dt_time.date() != dt_last:
    #             # interest = cash * self.rf * self.pricing_utl.get_ttm(dt_last, dt_time.date())
    #             # cash = black.Cash() + interest
    #             # cash += interest
    #             id_current = df_data[df_data[self.utl.col_date] == dt_time.date()][self.utl.id_instrument].values[0]
    #             id_last = df_data[df_data[self.utl.col_date] == dt_last][self.utl.id_instrument].values[0]
    #             spot_last = df_data[df_data[self.utl.col_date] == dt_last][self.utl.col_close].values[-1]
    #
    #             if id_current != id_last:
    #                 transaction_fee += abs(delta) * (spot_last + spot) * self.fee  # 移仓换月成本
    #                 change_pnl = delta * (spot_last - spot)
    #                 total_change_pnl += change_pnl
    #             dt_last = dt_time.date()
    #         elif dt_time.date() != dt_last:
    #             dt_last = dt_time.date()
    #
    #         d_asset = (delta - delta0) * spot
    #         asset += d_asset
    #         transaction_fee += abs(delta - delta0) * spot * self.fee
    #         replicate = cash + asset
    #         replicate_pnl = delta * spot - replicate - transaction_fee + change_pnl
    #         option_pnl = option_price - option0
    #         replicate_cost = -replicate_pnl + option_pnl
    #         replicate_error = -replicate_pnl + max(0, self.strike-spot) # Mark to option payoff at maturity
    #         margin = abs(delta) * spot * self.margin_rate
    #         delta0 = delta
    #         pct_cost = replicate_cost / option0
    #         res.append({'dt': dt_time, 'spot': spot,
    #                     'port cash': cash, 'port asset': delta * spot,
    #                     'delta': delta, 'margin': margin,
    #                     'value option': option_price, 'value replicate': replicate,
    #                     'replication cost': replicate_cost,
    #                     'transaction fee': transaction_fee, 'pnl option': option_pnl,
    #                     'pnl replicate': replicate_pnl, 'pct cost': pct_cost,
    #                     'change_pnl':total_change_pnl,'replicate error':replicate_error
    #                     })
    #         # print("%10s %20s %20s %20s %20s %20s %20s %20s" %
    #         #       (dt_time, spot, round(delta, 2), round(cash, 1), round(unit, 2), round(replicate, 0),
    #         #        round(option_price, 0), round(transaction_fee, 2)))
    #     # print('-' * 150)
    #     # print('init option : ', option0)
    #     # print('init replication : ', replicate0)
    #     # print('terminal option value : ', option_price)
    #     # print('terminal replicate value : ', replicate)
    #     # print(self.dt_issue,' replication cost : ', pct_replication_cost * 100, '%')
    #     # print('-' * 150)
    #     self.cash = cash
    #     self.delta = delta
    #     self.margin = abs(delta * spot) * self.margin_rate
    #     df_res = pd.DataFrame(res)
    #     result = df_res.iloc[-1]
    #
    #     return df_res


    def replicate_put(self, df_data, df_vol, df_underlying=None):
        self.cash = cash = 0
        self.dela = 0
        self.margin = 0
        total_change_pnl = 0.0
        res = []
        df_data[self.utl.col_date] = df_data[self.utl.col_datetime].apply(
            lambda x: datetime.date(x.year, x.month, x.day))
        dt_date = self.dt_issue  # Use Close price of option issue date to start replication.
        Option = EuropeanOption(self.strike, self.dt_maturity, self.utl.type_put)
        spot = df_data[df_data[self.utl.col_date] == dt_date][self.utl.col_close].values[-1]
        vol = self.get_vol(dt_date, df_vol)
        black = self.pricing_utl.get_blackcalculator(dt_date, spot, Option, self.rf, vol)
        option0 = black.NPV()
        delta0 = black.Delta(spot)
        asset = delta0 * spot
        replicate0 = cash + asset
        margin = abs(delta0) * spot * self.margin_rate
        transaction_fee = abs(delta0) * spot * self.fee
        pct_cost = transaction_fee / option0
        res.append({'dt': datetime.datetime(dt_date.year, dt_date.month, dt_date.day, 15, 0, 0), 'spot': spot,
                    'port cash': cash, 'port asset': delta0 * spot,
                    'delta': delta0, 'margin': margin,
                    'value option': option0, 'value replicate': replicate0,
                    'replication cost': transaction_fee,
                    'transaction fee': transaction_fee, 'pnl option': 0,
                    'pnl replicate': 0, 'pct cost': pct_cost, 'change_pnl': 0, 'replicate error': 0
                    })
        delta = delta0  # Unit of underlyings/futures in replicate portfolio
        dt_last = dt_date
        df_data = df_data[(df_data[self.utl.col_date] > self.dt_issue) & (
            df_data[self.utl.col_date] <= self.dt_maturity)]  # cut useful data
        df_data = df_data.reset_index(drop=True)
        for (i, row) in df_data.iterrows():
            dt_time = pd.to_datetime(row[self.utl.col_datetime])
            if dt_time.time() < datetime.time(9, 30, 0) or dt_time.time() > datetime.time(15, 0, 0): continue
            if dt_time.minute % 5 != 0: continue  # 5min调整一次delta
            spot = row[self.utl.col_close]
            if dt_time.date() == self.dt_maturity:
                delta, option_price = self.pricing_utl.get_maturity_metrics(dt_date, spot, Option)
            else:
                vol = self.get_vol(dt_time.date(), df_vol)
                black = self.pricing_utl.get_blackcalculator(dt_time.date(), spot, Option, self.rf, vol)
                delta = black.Delta(spot)
                option_price = black.NPV()
            change_pnl = 0.0
            date = dt_time.date()
            if i > 10 and date != dt_last:
                id_current = row[self.utl.id_instrument]
                id_last = df_data.loc[i - 1, self.utl.id_instrument]
                spot_last = df_data.loc[i - 1, self.utl.col_close]
                if id_current != id_last:
                    transaction_fee += abs(delta) * (spot_last + spot) * self.fee  # 移仓换月成本
                    change_pnl = delta * (spot_last - spot)
                    total_change_pnl += change_pnl
                dt_last = date
            elif date != dt_last:
                dt_last = date
            d_asset = (delta - delta0) * spot
            asset += d_asset
            transaction_fee += abs(delta - delta0) * spot * self.fee
            replicate = cash + asset
            replicate_pnl = delta * spot - replicate - transaction_fee + change_pnl
            option_pnl = option_price - option0
            replicate_cost = -replicate_pnl + option_pnl
            replicate_error = -replicate_pnl + max(0, self.strike - spot)  # Mark to option payoff at maturity
            margin = abs(delta) * spot * self.margin_rate
            delta0 = delta
            pct_cost = replicate_cost / option0
            res.append({'dt': dt_time, 'spot': spot,
                        'port cash': cash, 'port asset': delta * spot,
                        'delta': delta, 'margin': margin,
                        'value option': option_price, 'value replicate': replicate,
                        'replication cost': replicate_cost,
                        'transaction fee': transaction_fee, 'pnl option': option_pnl,
                        'pnl replicate': replicate_pnl, 'pct cost': pct_cost,
                        'change_pnl': total_change_pnl, 'replicate error': replicate_error
                        })
        self.cash = cash
        self.delta = delta
        self.margin = abs(delta * spot) * self.margin_rate
        df_res = pd.DataFrame(res)
        result = df_res.iloc[-1]
        return df_res

    def replicate_delta_bounds(self, df_data, df_vol):
        cash = 0
        self.dela = 0
        self.margin = 0
        total_change_pnl = 0.0
        res = []
        df_data[self.utl.col_date] = df_data[self.utl.col_datetime].apply(
            lambda x: datetime.date(x.year, x.month, x.day))
        dt_date = self.dt_issue  # Use Close price of option issue date to start replication.
        Option = EuropeanOption(self.strike, self.dt_maturity, self.utl.type_put)
        spot = df_data[df_data[self.utl.col_date] == dt_date][self.utl.col_close].values[-1]
        vol = self.get_vol(dt_date, df_vol)
        black = self.pricing_utl.get_blackcalculator(dt_date, spot, Option, self.rf, vol)
        option0 = black.NPV()
        delta0 = black.Delta(spot)
        asset = delta0 * spot
        replicate0 = cash + asset
        margin = abs(delta0) * spot * self.margin_rate
        transaction_fee = abs(delta0) * spot * self.fee
        pct_cost = transaction_fee / option0
        res.append({'dt': datetime.datetime(dt_date.year, dt_date.month, dt_date.day, 15, 0, 0), 'spot': spot,
                    'port cash': cash, 'port asset': delta0 * spot,
                    'delta': delta0, 'margin': margin,
                    'value option': option0, 'value replicate': replicate0,
                    'replication cost': transaction_fee,
                    'transaction fee': transaction_fee, 'pnl option': 0,
                    'pnl replicate': 0, 'pct cost': pct_cost, 'change_pnl': 0, 'replicate error': 0
                    })
        delta = delta0  # Unit of underlyings/futures in replicate portfolio
        dt_last = dt_date
        df_data = df_data[(df_data[self.utl.col_date] > self.dt_issue) & (
            df_data[self.utl.col_date] <= self.dt_maturity)]  # cut useful data
        df_data = df_data.reset_index(drop=True)
        for (i, row) in df_data.iterrows():
            dt_time = pd.to_datetime(row[self.utl.col_datetime])
            if dt_time.time() < datetime.time(9, 30, 0) or dt_time.time() > datetime.time(15, 0, 0): continue
            if dt_time.minute % 5 != 0: continue  # 5min调整一次delta
            spot = row[self.utl.col_close]
            if dt_time.date() == self.dt_maturity:
                delta, option_price = self.pricing_utl.get_maturity_metrics(dt_date, spot, Option)
            else:
                vol = self.get_vol(dt_time.date(), df_vol)
                black = self.pricing_utl.get_blackcalculator(dt_time.date(), spot, Option, self.rf, vol)
                delta = black.Delta(spot)
                option_price = black.NPV()
            change_pnl = 0.0
            date = dt_time.date()
            if i > 10 and date != dt_last:
                id_current = row[self.utl.id_instrument]
                id_last = df_data.loc[i - 1, self.utl.id_instrument]
                spot_last = df_data.loc[i - 1, self.utl.col_close]
                if id_current != id_last:
                    transaction_fee += abs(delta) * (spot_last + spot) * self.fee  # 移仓换月成本
                    change_pnl = delta * (spot_last - spot)
                    total_change_pnl += change_pnl
                dt_last = date
            elif date != dt_last:
                dt_last = date
            gamma = black.Gamma(spot)
            H = self.whalley_wilmott(dt_time.date(), Option, gamma, vol, spot, rho=1)
            if abs(delta - delta0) >= H:
                d_asset = (delta - delta0) * spot
                transaction_fee += abs(delta - delta0) * spot * self.fee
                delta0 = delta
            else:
                d_asset = 0.0  # delta变化在一定范围内则不选择对冲。
            asset += d_asset
            replicate = cash + asset
            replicate_pnl = delta * spot - replicate - transaction_fee + change_pnl
            option_pnl = option_price - option0
            replicate_cost = -replicate_pnl + option_pnl
            replicate_error = -replicate_pnl + max(0, self.strike - spot)  # Mark to option payoff at maturity
            margin = abs(delta) * spot * self.margin_rate
            pct_cost = replicate_cost / option0
            res.append({'dt': dt_time, 'spot': spot,
                        'port cash': cash, 'port asset': delta * spot,
                        'delta': delta, 'margin': margin,
                        'value option': option_price, 'value replicate': replicate,
                        'replication cost': replicate_cost,
                        'transaction fee': transaction_fee, 'pnl option': option_pnl,
                        'pnl replicate': replicate_pnl, 'pct cost': pct_cost,
                        'change_pnl': total_change_pnl, 'replicate error': replicate_error
                        })
        self.delta = delta
        self.margin = abs(delta * spot) * self.margin_rate
        df_res = pd.DataFrame(res)
        result = df_res.iloc[-1]
        return df_res

    """ Delta Bounds"""

    def whalley_wilmott(self, eval_date, option, gamma, vol, spot, rho=1):
        ttm = self.pricing_utl.get_ttm(eval_date, option.dt_maturity)
        H = (1.5 * math.exp(-self.rf * ttm) * self.fee * spot * (gamma ** 2) / rho) ** (1 / 3)
        return H

# plot_utl = PlotUtil()
# utl = BktUtil()
# dt_issue = datetime.date(2018, 3, 1)
# dt_maturity = datetime.date(2018, 4, 13)
# strike = 4000
# Option = EuropeanOption(strike, dt_maturity, utl.type_put)
# replication = Replication(strike, dt_issue, dt_maturity)
# vol = 0.2
# H1_list = []
# H2_list = []
# delta_list = []
# spot_list = np.arange(3500,4500,10)
# for spot in spot_list:
#     black = replication.pricing_utl.get_blackcalculator(dt_issue,spot, Option, replication.rf, vol)
#     gamma = black.Gamma(spot)
#     delta = black.Delta(spot)
#     h = replication.whalley_wilmott(dt_issue,Option,gamma,vol,spot)
#     H1 = delta + h
#     H2 = delta - h
#     H1_list.append(H1)
#     H2_list.append(H2)
#     delta_list.append(delta)
#
# plot_utl.plot_line_chart(delta_list, [H1_list, H2_list], ['对冲带上限', '对冲带下限'],'delta')
# plot_utl.plot_line_chart(spot_list, [H1_list, H2_list], ['对冲带上限', '对冲带下限'],'标的价格')
# plt.show()


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
# replication = Replication(strike, dt_issue, dt_maturity)
# # df_vol = replication.calculate_hist_vol('1M', df_index)
# # res,y,yy = replication.replicate_put(df_intraday, df_vix)
# #
# df_cf = get_dzqh_cf_daily(dt_start, dt_end, name_code.lower())
# df_cf_minute = get_dzqh_cf_minute(dt_start, dt_end, name_code.lower())
# res_fut = replication.replicate_put(df_cf_minute, df_vix)
#
# print(res_fut)
# strikes, option_payoffs, replicate_payoffs = replication.synsetic_payoff(df_intraday, df_vix)
# plot_utl.plot_line_chart(strikes, [option_payoffs, replicate_payoffs], ['option payoff', 'replicate portfolio'])
# plt.show()
