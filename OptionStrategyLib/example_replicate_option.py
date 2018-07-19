import datetime
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from back_test.BktUtil import BktUtil
from data_access.get_data import get_future_mktdata, get_index_mktdata, get_index_intraday, get_dzqh_cf_minute, \
    get_dzqh_cf_daily, get_vix
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.OptionReplication.replication import Replication
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator, EuropeanOption

from Utilities.calculate import *
from Utilities import Analysis
utl = BktUtil()


def get_hist_vol(cd_period, df_data):
    if cd_period == '1M':
        # dt_start = dt_issue - datetime.timedelta(days=50)
        # df = df_data[df_data[utl.col_date] >= dt_start]
        df = df_data.copy()
        histvol = calculate_histvol(df[utl.col_close], 20)
        df[utl.col_close] = histvol
        df = df[[utl.col_date, utl.col_close]].dropna()
    elif cd_period == '3M':
        df = df_data.copy()
        histvol = calculate_histvol(df[utl.col_close], 60)
        df[utl.col_close] = histvol
        df = df[[utl.col_date, utl.col_close]].dropna()
    elif cd_period == '2W':
        df = df_data.copy()
        histvol = calculate_histvol(df[utl.col_close], 10)
        df[utl.col_close] = histvol
        df = df[[utl.col_date, utl.col_close]].dropna()
    else:
        return
    return df

def fun_pct_vix(df, c):
    if df['vix'] >= 0.2 and df['histvol'] <= 0.5:
        vol = max(df['histvol'],df['vix'])
    elif df['histvol'] >= 0.5:
        vol = min(df['histvol'],df['vix'])
    else:
        vol = df['histvol']
    return vol

def histvol_ivix_signal(df_histvol,df_vix):
    df_histvol = df_histvol.rename(columns={utl.col_close:'histvol'})
    df_vix = df_vix.rename(columns={utl.col_close:'vix'})
    df_vol = df_histvol.merge(df_vix,on=[utl.col_date],how='inner')
    # df_vol['pct_vix'] = df_vol['vix'].pct_change()
    df_vol['vol'] = df_vol.apply(fun_pct_vix)
    return

def simulation_analysis(dt1, dt2, df_daily, vol):
    dt_list = sorted(df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique())
    dt = dt_list[0]
    spot = df_daily[df_daily[utl.col_date] == dt][utl.col_close].values[0]
    df_simualte = montecarlo(spot, vol, dt_list)
    df_res = analysis_strikes(dt1, dt2, df_daily, df_simualte, vol)
    return df_res


def syncetic_payoff(dt_issue, df_daily, vol, N):
    vol = float(vol)
    trading_dates = sorted(df_daily[utl.col_date].unique())
    idx = trading_dates.index(dt_issue)
    dt_maturity = trading_dates[idx + 20]

    dt_list = sorted(df_daily[(df_daily[utl.col_date] >= dt_issue) &
                              (df_daily[utl.col_date] <= dt_maturity)][utl.col_date].unique())
    dt1 = dt_list[0]
    s0 = df_daily[df_daily[utl.col_date] == dt1][utl.col_close].values[0]
    dict_replicates = {}
    dict_options = {}
    dict_options2 = {}
    for i in np.arange(N):
        df_simulate = montecarlo(s0, vol, dt_list)
        replication = Replication(s0, dt_issue, dt_maturity, rf=rf, fee=fee_rate)
        # df_vol = replication.calculate_hist_vol('1M', df_daily)
        df_res = replication.replicate_put(df_simulate, vol)
        S = df_simulate[utl.col_close].values[-1]
        R = df_res['pnl replicate'].values[-1]
        O = df_res['pnl option'].values[-1]
        O2 = df_res['value option'].values[-1]
        dict_replicates.update({S: R})
        dict_options.update({S: O})
        dict_options2.update({S: O2})
    stocks = sorted(dict_replicates)
    replicates = [value for (key, value) in sorted(dict_replicates.items())]
    options = [value for (key, value) in sorted(dict_options.items())]
    options2 = [value for (key, value) in sorted(dict_options2.items())]
    df = pd.DataFrame()
    df['stocks'] = stocks
    df['replicating pnl'] = replicates
    df['option pnl'] = options
    df['option payoff'] = options2
    return df


def analysis_strikes(dt1, dt2, df_daily, df_intraday, df_vix, df_underlying, cd_vol='1M'):
    res1 = []
    res2 = []
    trading_dates = sorted(df_daily[utl.col_date].unique())
    dt_list = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique()
    df_vol = get_hist_vol(cd_vol, df_daily)
    for dt in dt_list:
        print(dt, ' - ', datetime.datetime.now())
        dt_issue = dt
        idx = trading_dates.index(dt_issue)
        dt_maturity = trading_dates[idx + 20]
        if dt not in df_underlying[utl.col_date].values: continue
        spot = df_underlying[df_underlying[utl.col_date] == dt][utl.col_close].values[0]
        strike_dict = creat_replication_set(spot)
        res_dic1 = {}
        res_dic2 = {}
        replication = Replication(spot, dt_issue, dt_maturity, rf=rf, fee=fee_rate)

        for m in strike_dict.keys():
            strike = strike_dict[m]
            replication.strike = strike
            df_res1 = replication.replicate_put(df_intraday, df_vol)
            r_cost = df_res1['replication cost'].values[-1]
            r_pnl = df_res1['pnl replicate'].values[-1]
            o_pnl = df_res1['pnl option'].values[-1]
            o_payoff = df_res1['value option'].values[-1]
            pct_cost = df_res1['pct cost'].values[-1]
            fee = df_res1['transaction fee'].values[-1]
            r_error = df_res1['replicate error'].values[-1]  # mark to option payoff at maturity
            m = round(m, 2)
            res_dic1.update({
                str(m) + ' pnl replicate ': r_pnl,
                str(m) + ' pnl option ': o_pnl,
                str(m) + ' payoff option ': o_payoff,
                'cost/spot ' + str(m): r_cost / spot,
                'replicate error pct ' + str(m): r_error / spot,
                'cost/init_option ' + str(m): pct_cost,
                'pct_transaction ' + str(m): fee / spot,
                'cd_vol': 'histvol'
            })

            # df_res2 = replication.replicate_put(df_intraday, df_vix)
            # r_cost2 = df_res2['replication cost'].values[-1]
            # r_pnl2 = df_res2['pnl replicate'].values[-1]
            # o_pnl2 = df_res2['pnl option'].values[-1]
            # o_payoff2 = df_res2['value option'].values[-1]
            # pct_cost2 = df_res2['pct cost'].values[-1]
            # fee2 = df_res2['transaction fee'].values[-1]
            # r_error2 = df_res2['replicate error'].values[-1]  # mark to option payoff at maturity
            # res_dic2.update({
            #     str(m) + ' pnl replicate ': r_pnl2,
            #     str(m) + ' pnl option ': o_pnl2,
            #     str(m) + ' payoff option ': o_payoff2,
            #     'cost/spot ' + str(m): r_cost2 / spot,
            #     'replicate error pct ' + str(m): r_error2 / spot,
            #     'cost/init_option ' + str(m): pct_cost2,
            #     'pct_transaction ' + str(m): fee2 / spot,
            #     'cd_vol': 'vix'
            # })
        res_dic1.update({'dt_date': dt, 'init spot': spot})
        # res_dic2.update({'dt_date': dt, 'init spot': spot})
        res1.append(res_dic1)
        # res2.append(res_dic2)
    df_res1 = pd.DataFrame(res1)
    df_res2 = pd.DataFrame(res2)
    return df_res1, df_res2


def analysis_strikes_with_delta_bounds(dt1, dt2, df_daily, df_intraday, df_vix, df_underlying, cd_vol):
    res1 = []
    res2 = []
    trading_dates = sorted(df_daily[utl.col_date].unique())
    dt_list = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique()
    df_vol = get_hist_vol(cd_vol, df_daily)
    for dt in dt_list:
        print(dt, ' - ', datetime.datetime.now())
        dt_issue = dt
        idx = trading_dates.index(dt_issue)
        dt_maturity = trading_dates[idx + 20]
        if dt not in df_underlying[utl.col_date].values: continue
        spot = df_underlying[df_underlying[utl.col_date] == dt][utl.col_close].values[0]
        strike_dict = creat_replication_set(spot)
        res_dic1 = {}
        res_dic2 = {}
        replication = Replication(spot, dt_issue, dt_maturity, rf=rf, fee=fee_rate)
        for m in strike_dict.keys():
            strike = strike_dict[m]
            replication.strike = strike
            df_res1 = replication.replicate_delta_bounds(df_intraday, df_vol)
            r_cost = df_res1['replication cost'].values[-1]
            r_pnl = df_res1['pnl replicate'].values[-1]
            o_pnl = df_res1['pnl option'].values[-1]
            o_payoff = df_res1['value option'].values[-1]
            pct_cost = df_res1['pct cost'].values[-1]
            fee = df_res1['transaction fee'].values[-1]
            r_error = df_res1['replicate error'].values[-1]  # mark to option payoff at maturity
            m = round(m, 2)
            res_dic1.update({
                str(m) + ' pnl replicate ': r_pnl,
                str(m) + ' pnl option ': o_pnl,
                str(m) + ' payoff option ': o_payoff,
                'cost/spot ' + str(m): r_cost / spot,
                'replicate error pct ' + str(m): r_error / spot,
                'cost/init_option ' + str(m): pct_cost,
                'pct_transaction ' + str(m): fee / spot,
                'cd_vol': 'histvol'
            })

            df_res2 = replication.replicate_delta_bounds(df_intraday, df_vix)
            r_cost2 = df_res2['replication cost'].values[-1]
            r_pnl2 = df_res2['pnl replicate'].values[-1]
            o_pnl2 = df_res2['pnl option'].values[-1]
            o_payoff2 = df_res2['value option'].values[-1]
            pct_cost2 = df_res2['pct cost'].values[-1]
            fee2 = df_res2['transaction fee'].values[-1]
            r_error2 = df_res2['replicate error'].values[-1]  # mark to option payoff at maturity
            res_dic2.update({
                str(m) + ' pnl replicate ': r_pnl2,
                str(m) + ' pnl option ': o_pnl2,
                str(m) + ' payoff option ': o_payoff2,
                'cost/spot ' + str(m): r_cost2 / spot,
                'replicate error pct ' + str(m): r_error2 / spot,
                'cost/init_option ' + str(m): pct_cost2,
                'pct_transaction ' + str(m): fee2 / spot,
                'cd_vol': 'vix'
            })
        res_dic1.update({'dt_date': dt, 'init spot': spot})
        res_dic2.update({'dt_date': dt, 'init spot': spot})
        res1.append(res_dic1)
        res2.append(res_dic2)
    df_res1 = pd.DataFrame(res1)
    df_res2 = pd.DataFrame(res2)
    return df_res1, df_res2


def creat_replication_set(spot):
    # k = spot
    strike_dict = {0.9: 0.9 * spot,
                   0.95: 0.95 * spot,
                   1.0: 1.0 * spot,
                   1.05: 1.05 * spot,
                   1.1: 1.1 * spot,
                   }
    # strike_dict = {
    #                0.95: 0.95 * spot,
    #                1.0: 1.0 * spot,
    #                1.05: 1.05 * spot,
    #                }
    # for i in np.arange(0.9, 1.15, 0.05):
    #     strike_dict.update({i: k * i})
    return strike_dict


def historic_example(dt_issue, df_inderlying, df_cf, df_cf_minute, df_vix):
    res_histvol = pd.DataFrame()
    res_vix = pd.DataFrame()
    trading_dates = sorted(df_cf[utl.col_date].unique())
    fut = df_cf[df_cf[utl.col_date] == dt_issue][utl.col_close].values[0]
    spot = df_inderlying[df_inderlying[utl.col_date] == dt_issue][utl.col_close].values[0]
    idx = trading_dates.index(dt_issue)
    # dt_maturity = trading_dates[idx + 20]
    dt_maturity = trading_dates[idx + 20]
    strike_dict = creat_replication_set(fut)
    df_vol = get_hist_vol('1M', df_cf)
    for m in strike_dict.keys():
        strike = strike_dict[m]
        replication = Replication(strike, dt_issue, dt_maturity, rf=rf, fee=fee_rate)
        df_res1 = replication.replicate_delta_bounds(df_cf_minute, df_vol)
        df_res2 = replication.replicate_delta_bounds(df_cf_minute, df_vix)
        df_res1.loc[:, 'm'] = m
        df_res2.loc[:, 'm'] = m
        df_res1.loc[:, 'init underlying'] = spot
        df_res2.loc[:, 'init underlying'] = spot
        res_histvol = res_histvol.append(df_res1, ignore_index=True)
        res_vix = res_vix.append(df_res2, ignore_index=True)
    # strike = spot
    # m = 1
    # replication = Replication(strike, dt_issue, dt_maturity, rf=rf, fee=fee_rate)
    # df_res1 = replication.replicate_put(df_intraday, df_vol)
    # df_res2 = replication.replicate_put(df_intraday, df_vix)
    # df_res1.loc[:, 'm'] = m
    # df_res2.loc[:, 'm'] = m
    # res_histvol = res_histvol.append(df_res1, ignore_index=True)
    # res_vix = res_vix.append(df_res2, ignore_index=True)
    return res_histvol, res_vix



def hedging(dt1, dt2, df_daily, df_intraday, df_index, pct_strike, cd_vol):
    res = []
    # 构建期限为1M的平值认沽期权，距到期1W进行期权合约转换
    df_vol = get_hist_vol(cd_vol, df_index)
    df_intraday[utl.col_date] = df_intraday[utl.col_datetime].apply(
        lambda x: datetime.date(x.year, x.month, x.day))
    df_daily = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)]
    df_index = df_index[(df_index[utl.col_date] >= dt1) & (df_index[utl.col_date] <= dt2)]
    trading_dates = sorted(df_index[utl.col_date].unique())
    dt_last_issue = trading_dates[-22]
    df_intraday = df_intraday[
        (df_intraday[utl.col_date] >= dt1) & (df_intraday[utl.col_date] <= dt_last_issue)].reset_index(drop=True)
    # First Option
    dt_date = trading_dates[0]
    dt_issue = dt_date
    dt_maturity = trading_dates[21]
    spot = df_daily[df_daily[utl.col_date] == dt_date][utl.col_close].values[0]
    strike = pct_strike * df_index[df_index[utl.col_date] <= dt_date][utl.col_close].values[-1]
    Option = EuropeanOption(strike, dt_maturity, utl.type_put)
    replication = Replication(strike, dt_date, dt_maturity)
    vol = replication.get_vol(dt_date, df_vol)
    black = replication.pricing_utl.get_blackcalculator(dt_date, spot, Option, replication.rf, vol)
    delta0 = black.Delta()
    option0 = black.NPV()
    asset = delta0 * spot
    replicate = asset
    margin = abs(delta0) * spot * replication.margin_rate
    transaction_fee = abs(delta0) * (spot * replication.fee + replication.slippage)
    replicate_pnl = - transaction_fee
    dt_last = dt_date
    for (i, row) in df_intraday.iterrows():
        dt_time = pd.to_datetime(row[utl.col_datetime])
        dt_date = row[utl.col_date]
        if dt_time.time() < datetime.time(9, 30, 0) or dt_time.time() > datetime.time(15, 0, 0): continue
        if dt_time.minute % 5 != 0: continue  # 5min调整一次delta
        if dt_date <= dt_issue: continue  # issue date 按收盘价
        idx_today = trading_dates.index(dt_date)
        idx_maturity = trading_dates.index(dt_maturity)
        spot = row[utl.col_close]
        if idx_maturity - idx_today <= 5:
            dt_issue = dt_date
            dt_maturity = trading_dates[trading_dates.index(dt_issue) + 21]
            # spot = df_index[df_index[utl.col_date] == dt_issue][utl.col_close].values[0]
            spot = df_daily[df_daily[utl.col_date] == dt_date][utl.col_close].values[0]
            strike = pct_strike * df_index[df_index[utl.col_date] <= dt_date][utl.col_close].values[-1]
            # 距到期5天以内，按当日收盘价重置期权行权价与到期日
            Option = EuropeanOption(strike, dt_maturity, utl.type_put)
            replication = Replication(strike, dt_issue, dt_maturity)
        vol = replication.get_vol(dt_date, df_vol)
        black = replication.pricing_utl.get_blackcalculator(dt_date, spot, Option, replication.rf, vol)
        gamma = black.Gamma()
        delta = black.Delta()
        option_price = black.NPV()
        H = replication.whalley_wilmott(dt_time.date(), Option, gamma, vol, spot)
        # 移仓换月
        change_pnl = 0.0
        if i > 10 and dt_date != dt_last:
            id_current = row[utl.id_instrument]
            id_last = df_intraday.loc[i - 1, utl.id_instrument]
            spot_last = df_intraday.loc[i - 1, utl.col_close]
            if id_current != id_last:
                transaction_fee += abs(delta) * (spot_last + spot) * replication.fee  # 移仓换月成本
                change_pnl = delta * (spot_last - spot)
            dt_last = dt_date
            # 每日以收盘价更新组合净值
            underlying = df_index[df_index[utl.col_date] <= dt_last][utl.col_close].values[-1]
            portfolio = underlying + replicate_pnl
            res.append({'dt_date': dt_last, 'portfolio': portfolio,'if spot':spot,
                        'margin': margin,'index':underlying,'replicate_pnl':replicate_pnl,
                        'replicate position':replicate,'delta':delta})
        elif dt_date != dt_last:
            dt_last = dt_date
        if abs(delta - delta0) >= H:
            d_asset = (delta - delta0) * spot
            transaction_fee += abs(delta - delta0) * (spot * replication.fee + replication.slippage)
            delta0 = delta
        else:
            d_asset = 0.0  # delta变化在一定范围内则不选择对冲。
        asset += d_asset
        replicate = asset
        replicate_pnl = delta * spot - replicate - transaction_fee + change_pnl
        option_pnl = option_price - option0
        replicate_cost = -replicate_pnl + option_pnl
        replicate_error = -replicate_pnl + max(0, strike - spot)  # Mark to option payoff at maturity
        margin = abs(delta) * spot * replication.margin_rate
    df_res = pd.DataFrame(res)
    return df_res

def hedging_constant_ttm(dt1, dt2, df_daily, df_intraday, df_index, pct_strike, cd_vol):
    res = []
    # 构建期限为1M的平值认沽期权，距到期1W进行期权合约转换
    df_vol = get_hist_vol(cd_vol, df_index)

    df_daily = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)]
    df_index = df_index[(df_index[utl.col_date] >= dt1) & (df_index[utl.col_date] <= dt2)]
    trading_dates = sorted(df_index[utl.col_date].unique())
    dt_last_issue = trading_dates[-22]
    df_intraday = df_intraday[
        (df_intraday[utl.col_date] >= dt1) & (df_intraday[utl.col_date] <= dt_last_issue)].reset_index(drop=True)
    # First Option
    dt_date = trading_dates[0]
    dt_issue = dt_date
    spot = df_daily[df_daily[utl.col_date] == dt_date][utl.col_close].values[0]
    strike = pct_strike * df_index[df_index[utl.col_date] == dt_date][utl.col_close].values[0]
    underlying = df_index[df_index[utl.col_date] <= dt_date][utl.col_close].values[-1]
    dt_maturity = dt_issue + datetime.timedelta(days=30)
    Option = EuropeanOption(strike, dt_maturity, utl.type_put)
    replication = Replication(strike, dt_date, dt_maturity)
    vol = replication.get_vol(dt_date, df_vol)
    black = replication.pricing_utl.get_blackcalculator(dt_date, spot, Option, replication.rf, vol)
    delta0 = black.Delta()
    option0 = black.NPV()
    asset = delta0 * spot
    replicate = asset
    margin = abs(delta0) * spot * replication.margin_rate
    transaction_fee = abs(delta0) * (spot * replication.fee + replication.slippage)
    replicate_pnl = - transaction_fee
    portfolio0 = underlying + replicate_pnl
    underlying0 = underlying
    dt_last = dt_date
    delta = delta0
    for (i, row) in df_intraday.iterrows():
        dt_time = pd.to_datetime(row[utl.col_datetime])
        dt_date = row[utl.col_date]
        if dt_date == datetime.date(2017,4,4):
            print(dt_date)
        if dt_time.time() < datetime.time(9, 30, 0) or dt_time.time() > datetime.time(15, 0, 0): continue
        if dt_time.minute % 5 != 0: continue  # 5min调整一次delta
        if dt_date <= dt_issue: continue  # issue date 按收盘价
        # 移仓换月
        change_pnl = 0.0
        if i > 10 and dt_date != dt_last:
            id_current = row[utl.id_instrument]
            id_last = df_intraday.loc[i - 1, utl.id_instrument]
            spot_last = df_intraday.loc[i - 1, utl.col_close]
            dt_issue = dt_date
            dt_maturity = dt_issue + datetime.timedelta(days=30)
            # strike = pct_strike*underlying
            close_list = df_index[df_index[utl.col_date] <= dt_last][utl.col_close]
            underlying = close_list.values[-1]
            if len(close_list) >=20:
                l=close_list.values[-20:-1]
                strike = pct_strike*sum(l)/len(l)
            replication.strike = strike
            Option = EuropeanOption(strike, dt_maturity, utl.type_put)
            replication = Replication(strike, dt_date, dt_maturity)
            if id_current != id_last:
                transaction_fee += abs(delta) * (spot_last + spot) * replication.fee  # 移仓换月成本
                change_pnl = delta * (spot_last - spot)
            dt_last = dt_date
            # 每日以收盘价更新组合净值
            portfolio = (underlying + replicate_pnl)
            npv = (underlying + replicate_pnl)/portfolio0
            benchmark = underlying/underlying0
            res.append({'dt_date': dt_last, 'portfolio': portfolio,'benchmark':benchmark,
                        'npv':npv,'if spot':spot,'strike':strike,
                        'margin': margin,'index':underlying,'replicate_pnl':replicate_pnl,
                        'replicate position':replicate,'delta':delta})
        elif dt_date != dt_last:
            dt_last = dt_date
        spot = row[utl.col_close]
        vol = replication.get_vol(dt_date, df_vol)
        black = replication.pricing_utl.get_blackcalculator(dt_date, spot, Option, replication.rf, vol)
        gamma = black.Gamma()
        delta = black.Delta()
        option_price = black.NPV()
        H = replication.whalley_wilmott(dt_time.date(), Option, gamma, vol, spot)

        if abs(delta - delta0) >= H:
            d_asset = (delta - delta0) * spot
            transaction_fee += abs(delta - delta0) * (spot * replication.fee + replication.slippage)
            delta0 = delta
        else:
            d_asset = 0.0  # delta变化在一定范围内则不选择对冲。
        asset += d_asset
        replicate = asset
        replicate_pnl = delta * spot - replicate - transaction_fee + change_pnl
        option_pnl = option_price - option0
        replicate_cost = -replicate_pnl + option_pnl
        replicate_error = -replicate_pnl + max(0, strike - spot)  # Mark to option payoff at maturity
        margin = abs(delta) * spot * replication.margin_rate
    df_res = pd.DataFrame(res)
    return df_res

def hedging_2year_ttm(dt1, dt2, df_daily, df_intraday, df_index, pct_strike, cd_vol):
    res = []
    # 构建期限为1M的平值认沽期权，距到期1W进行期权合约转换
    df_vol = get_hist_vol(cd_vol, df_index)

    df_daily = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)]
    df_index = df_index[(df_index[utl.col_date] >= dt1) & (df_index[utl.col_date] <= dt2)]
    trading_dates = sorted(df_index[utl.col_date].unique())
    dt_last_issue = trading_dates[-22]
    df_intraday = df_intraday[
        (df_intraday[utl.col_date] >= dt1) & (df_intraday[utl.col_date] <= dt_last_issue)].reset_index(drop=True)
    # First Option
    dt_date = trading_dates[0]
    dt_issue = dt_date
    spot = df_daily[df_daily[utl.col_date] == dt_date][utl.col_close].values[0]
    strike = pct_strike * df_index[df_index[utl.col_date] == dt_date][utl.col_close].values[0]
    underlying = df_index[df_index[utl.col_date] <= dt_date][utl.col_close].values[-1]
    dt_maturity = dt_issue + datetime.timedelta(days=365*2)
    Option = EuropeanOption(strike, dt_maturity, utl.type_put)
    replication = Replication(strike, dt_date, dt_maturity)
    vol = replication.get_vol(dt_date, df_vol)
    black = replication.pricing_utl.get_blackcalculator(dt_date, spot, Option, replication.rf, vol)
    delta0 = black.Delta()
    option0 = black.NPV()
    asset = delta0 * spot
    replicate = asset
    margin = abs(delta0) * spot * replication.margin_rate
    transaction_fee = abs(delta0) * (spot * replication.fee + replication.slippage)
    replicate_pnl = - transaction_fee
    portfolio0 = underlying + replicate_pnl
    underlying0 = underlying
    dt_last = dt_date
    delta = delta0
    for (i, row) in df_intraday.iterrows():
        dt_time = pd.to_datetime(row[utl.col_datetime])
        dt_date = row[utl.col_date]
        if dt_date == datetime.date(2017,11,9):
            print(dt_date)
        if dt_time.time() < datetime.time(9, 30, 0) or dt_time.time() > datetime.time(15, 0, 0): continue
        if dt_time.minute % 5 != 0: continue  # 5min调整一次delta
        if dt_date <= dt_issue: continue  # issue date 按收盘价
        change_pnl = 0.0

        if i > 10 and dt_date != dt_last:
            id_current = row[utl.id_instrument]
            id_last = df_intraday.loc[i - 1, utl.id_instrument]
            spot_last = df_intraday.loc[i - 1, utl.col_close]
            close_list = df_index[df_index[utl.col_date] <= dt_last][utl.col_close]
            underlying = close_list.values[-1]
            if len(close_list) >=20:
                l=close_list.values[-20:0]
                strike = pct_strike*sum(l)/len(l)
            replication.strike = strike
            # 移仓换月
            if id_current != id_last:
                transaction_fee += abs(delta) * (spot_last + spot) * replication.fee  # 移仓换月成本
                change_pnl = delta * (spot_last - spot)
            dt_last = dt_date
            # 每日以收盘价更新组合净值
            portfolio = (underlying + replicate_pnl)
            npv = (underlying + replicate_pnl)/portfolio0
            benchmark = underlying/underlying0
            res.append({'dt_date': dt_last, 'portfolio': portfolio,'benchmark':benchmark,
                        'npv':npv,'if spot':spot,'strike':replication.strike,
                        'margin': margin,'index':underlying,'replicate_pnl':replicate_pnl,
                        'replicate position':replicate,'delta':delta})
        elif dt_date != dt_last:
            dt_last = dt_date
        spot = row[utl.col_close]
        vol = replication.get_vol(dt_date, df_vol)
        black = replication.pricing_utl.get_blackcalculator(dt_date, spot, Option, replication.rf, vol)
        gamma = black.Gamma()
        delta = black.Delta()
        option_price = black.NPV()
        H = replication.whalley_wilmott(dt_time.date(), Option, gamma, vol, spot)
        # 移仓换月
        if abs(delta - delta0) >= H:
            d_asset = (delta - delta0) * spot
            transaction_fee += abs(delta - delta0) * (spot * replication.fee + replication.slippage)
            delta0 = delta
        else:
            d_asset = 0.0  # delta变化在一定范围内则不选择对冲。
        asset += d_asset
        replicate = asset
        replicate_pnl = delta * spot - replicate - transaction_fee + change_pnl
        option_pnl = option_price - option0
        replicate_cost = -replicate_pnl + option_pnl
        replicate_error = -replicate_pnl + max(0, strike - spot)  # Mark to option payoff at maturity
        margin = abs(delta) * spot * replication.margin_rate
    df_res = pd.DataFrame(res)
    return df_res


plot_utl = PlotUtil()
# name_code = 'IF'
name_code = 'IH'
# id_index = 'index_300sh'
id_index = 'index_50sh'
vol = 0.2
rf = 0.03
fee_rate = 5.0 / 10000.0

dt1 = datetime.date(2014, 1, 5)
# dt1 = datetime.date(2018, 1, 5)
# dt2 = datetime.date(2018, 6, 30)
dt2 = datetime.date(2018, 5, 13)
#####################################################################################


"""1/2/3 data"""
# dt_start = dt1 - datetime.timedelta(days=50)
# dt_end = dt2 + datetime.timedelta(days=31)
# df_vix = get_vix(dt1, dt_end)
# df_cf = get_dzqh_cf_daily(dt_start, dt_end, name_code.lower())
# df_cf_minute = get_dzqh_cf_minute(dt_start, dt_end, name_code.lower())
# df_index = get_index_mktdata(dt_start, dt_end, id_index)
# df_intraday = get_index_intraday(dt_start, dt_end, id_index)
#
# # df_future = get_future_mktdata(dt_start, dt_end, name_code)
# # cf_vol = get_hist_vol('2W', df_cf)
# # index_vol = get_hist_vol('2W', df_index)
# # cf_vol.to_excel('../cf_vol.xlsx')
# # index_vol.to_excel('../index_vol.xlsx')
# df_vix.to_excel('../data/replicate/df_vix.xlsx')
# df_cf.to_excel('../data/replicate/df_cf.xlsx')
# df_cf_minute.to_excel('../data/replicate/df_cf_minute.xlsx')
# df_index.to_excel('../data/replicate/df_index.xlsx')
# # df_future.to_excel('../data/replicate/df_future.xlsx')
# df_intraday.to_excel('../data/replicate/df_intraday.xlsx')


""" local data  """
df_vix = pd.ExcelFile('../data/replicate/df_vix.xlsx').parse("Sheet1")
df_cf = pd.ExcelFile('../data/replicate/df_cf.xlsx').parse("Sheet1")
df_cf_minute = pd.ExcelFile('../data/replicate/df_cf_minute.xlsx').parse("Sheet1")
df_index = pd.ExcelFile('../data/replicate/df_index.xlsx').parse("Sheet1")
df_intraday = pd.ExcelFile('../data/replicate/df_intraday.xlsx').parse("Sheet1")
df_vix.loc[:, 'dt_date'] = df_vix['dt_date'].apply(lambda x: x.date())
df_cf.loc[:, 'dt_date'] = df_cf['dt_date'].apply(lambda x: x.date())
df_index.loc[:, 'dt_date'] = df_index['dt_date'].apply(lambda x: x.date())
df_cf_minute.loc[:, 'dt_date'] = df_cf_minute['dt_datetime'].apply(lambda x: x.date())
df_intraday.loc[:, 'dt_date'] = df_intraday['dt_datetime'].apply(lambda x: x.date())

""" 数据预处理 """
#
# df_cf = df_cf[df_cf[utl.col_date] != datetime.date(2017,4,4)].reset_index(drop=True)
# df_cf_minute = df_cf_minute[df_cf_minute[utl.col_date] != datetime.date(2017,4,4)].reset_index(drop=True)
# df_index = df_index[df_index[utl.col_date] != datetime.date(2017,4,4)].reset_index(drop=True)
# df_intraday = df_intraday[df_intraday[utl.col_date] != datetime.date(2017,4,4)].reset_index(drop=True)
# df_vix.to_excel('../data/replicate/df_vix.xlsx')
# df_cf.to_excel('../data/replicate/df_cf.xlsx')
# df_cf_minute.to_excel('../data/replicate/df_cf_minute.xlsx')
# df_index.to_excel('../data/replicate/df_index.xlsx')
# df_intraday.to_excel('../data/replicate/df_intraday.xlsx')

"""1、基于蒙特卡洛模拟的复制结果"""
# print('start')
# df = syncetic_payoff(dt1, df_index, 0.2, 100)
# stocks = df['stocks']
# replicates = df['replicating pnl']
# options = df['option pnl']
# option_payoff = df['option payoff']
# print(df)
# plot_utl.plot_line_chart(stocks, [replicates, options,option_payoff], ['复制组合损益', '期权到期收益-权利金','期权到期收益'])
# plt.show()

"""2、基于现货指数历史数据的复制结果"""
# print('2.start')
# cd_vol = '2W'
# res_histvol, res_vix = analysis_strikes(dt1, dt2, df_index, df_intraday, df_vix, df_index, cd_vol)
# print(res_histvol)
# print(res_vix)
# res_histvol.to_excel('../res_sh300index_histvol_2W.xlsx')
# res_vix.to_excel('../res_sh300index_vix.xlsx')

"""3、基于期货历史数据的复制结果"""
# print('3-1.start')
# cd_vol = '1M'
# res_histvol, res_vix = analysis_strikes(dt1, dt2, df_cf, df_cf_minute, df_vix, df_index, cd_vol)
# print(res_histvol)
# print(res_vix)
# res_histvol.to_excel('../res_sh300future_vix_1M.xlsx')
# res_vix.to_excel('../res_sh300future_vix.xlsx')

# print('3-2.start')
# cd_vol = '3M'
# res_histvol, res_vix = analysis_strikes(dt1, dt2, df_cf, df_cf_minute, df_vix, df_index, cd_vol)
# print(res_histvol)
# print(res_vix)
# res_histvol.to_excel('../res_sh300future_histvol_3M.xlsx')
# res_vix.to_excel('../res_sh300future_vix.xlsx')


"""4、举例"""
# print('4.start')
# dt_issue = datetime.date(2017, 12, 25) # 15
# # dt_issue = datetime.date(2017, 12, 1)
# dt_start = dt_issue - datetime.timedelta(days=50)
# dt_end = dt_issue + datetime.timedelta(days=100)
# df_vix = get_vix(dt_issue, dt_end)
#
# df_index = get_index_mktdata(dt_start, dt_end, id_index)
# df_cf = get_dzqh_cf_daily(dt_start, dt_end, name_code.lower())
# df_cf_minute = get_dzqh_cf_minute(dt_start, dt_end, name_code.lower())
# res_histvol, res_vix = historic_example(dt_issue, df_index, df_cf, df_cf_minute, df_vix)
# print(res_histvol)
# print(res_vix)
# res_histvol1 = res_histvol[res_histvol['m'] == 1.0]
# res_histvol1.loc[:,'dt_date'] = res_histvol1['dt'].apply(lambda x: datetime.date(x.year, x.month, x.day))
# res_histvol1 = res_histvol1.sort_values(by='dt', ascending=False). \
#     drop_duplicates(subset=['dt_date']). \
#     sort_values(by='dt_date', ascending=True)
# res_histvol1.to_excel('../res_historic_example_histvol_m100.xlsx')
#
#
# res_vix1 = res_vix[res_vix['m'] == 1.0]
# res_vix1.loc[:,'dt_date'] = res_vix1['dt'].apply(lambda x: datetime.date(x.year, x.month, x.day))
# res_vix1 = res_vix1.sort_values(by='dt', ascending=False). \
#     drop_duplicates(subset=['dt_date']). \
#     sort_values(by='dt_date', ascending=True)
# res_vix1.to_excel('../res_historic_example_vix_m100.xlsx')
#
# res_histvol1 = res_histvol[res_histvol['m'] == 1.05]
# res_histvol1.loc[:,'dt_date'] = res_histvol1['dt'].apply(lambda x: datetime.date(x.year, x.month, x.day))
# res_histvol1 = res_histvol1.sort_values(by='dt', ascending=False). \
#     drop_duplicates(subset=['dt_date']). \
#     sort_values(by='dt_date', ascending=True)
# res_histvol1.to_excel('../res_historic_example_histvol_m105.xlsx')
#
# res_histvol1 = res_histvol[res_histvol['m'] == 0.95]
# res_histvol1.loc[:,'dt_date'] = res_histvol1['dt'].apply(lambda x: datetime.date(x.year, x.month, x.day))
# res_histvol1 = res_histvol1.sort_values(by='dt', ascending=False). \
#     drop_duplicates(subset=['dt_date']). \
#     sort_values(by='dt_date', ascending=True)
# res_histvol1.to_excel('../res_historic_example_histvol_m95.xlsx')


#
# # dates = res_histvol['dt_date'].tolist()
# # replicate_pnls_hv = res_histvol['pnl replicate'].tolist()
# # option_pnls_hv = res_histvol['pnl option'].tolist()
# # replicate_pnls_vix = res_vix['pnl replicate'].tolist()
# # option_pnls_vix = res_vix['pnl option'].tolist()
# # fig, ax = plt.subplots()
# # ax.scatter(dates, replicate_pnls_hv, label='replicate pnl hv')
# # ax.scatter(dates, option_pnls_hv, label='option pnls hv')
# # ax.legend()
# #
# # plt.show()

"""5、With Delta Bounds"""
# print('5.start')
# cd_vol = '1M'
# res_histvol, res_vix = analysis_strikes_with_delta_bounds(dt1, dt2, df_cf, df_cf_minute, df_vix, df_index, cd_vol)
# print(res_histvol)
# print(res_vix)
# res_histvol.to_excel('../res_sh50future_histvol_with_delta_bounds_1M.xlsx')
# res_vix.to_excel('../res_sh50future_vix_with_delta_bounds.xlsx')

""" 6、Continuously hedging """
cd_vol = '1M'
res = hedging_constant_ttm(dt1, dt2, df_cf, df_cf_minute, df_index, pct_strike=1.0, cd_vol=cd_vol)
res.to_excel('../res.xlsx')
analysis = Analysis.get_netvalue_analysis(res['npv'])
analysis2 = Analysis.get_netvalue_analysis(res['benchmark'])
print('-'*100)
print('analysis hedged')
print('-'*100)
print(analysis)
print('-'*100)
print('analysis unhedged')
print('-'*100)
print(analysis2)
plot_utl.plot_line_chart(res['dt_date'].tolist(),[res['npv'].tolist(),res['benchmark'].tolist()],['portfolio','sh300'])
plt.show()

""" Active vol signaled by vix change """
# df_histvol = get_hist_vol('1M',df_cf)
# histvol_ivix_signal(df_histvol,df_vix)