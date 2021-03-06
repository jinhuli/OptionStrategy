import datetime
import math

import pandas as pd

from PricingLibrary.BlackCalculator import BlackCalculator
from PricingLibrary.Options import EuropeanOption
from PricingLibrary.Util import PricingUtil
from back_test.deprecated.BktUtil import BktUtil


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
        self.slippage = slippage
        self.margin_rate = margin_rate
        self.multiplier = multiplier
        self.pricing = None
        self.dt_eval = None
        self.cash = 0
        self.dela = 0
        self.margin = 0
        # self.df_vix = df_vix

    def get_vol(self, dt_date, df_data):
        if isinstance(df_data, float):
            return df_data
        vix = df_data[df_data[self.utl.col_date] <= dt_date][self.utl.col_close].values[-1]
        return vix

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
        # spot = self.strike
        vol = self.get_vol(dt_date, df_vol)
        # black = self.pricing_utl.get_blackcalculator(dt_date, spot, Option, self.rf, vol)
        black = BlackCalculator(dt_date,Option.dt_maturity,Option.strike,Option.option_type,spot,vol,self.rf)
        option0 = black.NPV()
        delta0 = black.Delta()
        asset = delta0 * spot
        replicate0 = cash + asset
        margin = abs(delta0) * spot * self.margin_rate
        transaction_fee = abs(delta0) * (spot * self.fee + self.slippage)
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
                delta = black.Delta()
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
            transaction_fee += abs(delta - delta0) * (spot * self.fee + self.slippage)
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
        delta0 = black.Delta()
        asset = delta0 * spot
        replicate0 = cash + asset
        margin = abs(delta0) * spot * self.margin_rate
        transaction_fee = abs(delta0) * (spot * self.fee + self.slippage)
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
                delta = black.Delta()
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
            gamma = black.Gamma()
            H = self.whalley_wilmott(dt_time.date(), Option, gamma, vol, spot, rho=1)
            if abs(delta - delta0) >= H:
                d_asset = (delta - delta0) * spot
                transaction_fee += abs(delta - delta0) * (spot * self.fee + self.slippage)
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


    # def hedge_constant_ttm(self):

    """ Delta Bounds"""

    def whalley_wilmott(self, eval_date, option, gamma, vol, spot, rho=1, fee=5.0 / 10000.0):
        ttm = self.pricing_utl.get_ttm(eval_date, option.dt_maturity)
        H = (1.5 * math.exp(-self.rf * ttm) * fee * spot * (gamma ** 2) / rho) ** (1 / 3)
        return H


# plot_utl = PlotUtil()
# utl = BktUtil()
# dt_issue = datetime.date(2018, 3, 1)
# dt_maturity = datetime.date(2018, 4, 13)
# strike = 4000
# Option = EuropeanOption(strike, dt_maturity, utl.type_put)
# replication = Replication(strike, dt_issue, dt_maturity)
# vol = 0.2
# f = 5.0 / 10000.0
# H1_list = []
# H2_list = []
# delta_list = []
# spot_list = np.arange(3500, 4500, 10)
# for spot in spot_list:
#     black = replication.pricing_utl.get_blackcalculator(dt_issue, spot, Option, replication.rf, vol)
#     gamma = black.Gamma()
#     delta = black.Delta()
#     h = replication.whalley_wilmott(dt_issue, Option, gamma, vol, spot, 5, f)
#     H1 = delta + h
#     H2 = delta - h
#     H1_list.append(H1)
#     H2_list.append(H2)
#     delta_list.append(delta)
#
# plot_utl.plot_line_chart(delta_list, [H1_list, H2_list], ['对冲带上限', '对冲带下限'], 'delta')
# # plot_utl.plot_line_chart(spot_list, [H1_list, H2_list], ['对冲带上限', '对冲带下限'], '标的价格')
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