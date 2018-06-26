import datetime
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from back_test.BktUtil import BktUtil
from data_access.get_data import get_future_mktdata, get_index_mktdata, get_index_intraday, get_dzqh_cf_minute, \
    get_dzqh_cf_daily, get_vix
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.OptionReplication.Replication import Replication
from Utilities.calculate import *

utl = BktUtil()


def simulation_analysis(dt1, dt2, df_daily, vol):
    dt_list = sorted(df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique())
    dt = dt_list[0]
    spot = df_daily[df_daily[utl.col_date] == dt][utl.col_close].values[0]
    df_simualte = montecarlo(spot, vol, dt_list)
    df_res = analysis_strikes(dt1, dt2, df_daily, df_simualte, vol)
    return df_res


def syncetic_payoff(dt_issue, df_daily, vol, N):
    trading_dates = sorted(df_daily[utl.col_date].unique())
    idx = trading_dates.index(dt_issue)
    dt_maturity = trading_dates[idx + 20]

    dt_list = sorted(df_daily[(df_daily[utl.col_date] >= dt_issue) &
                              (df_daily[utl.col_date] <= dt_maturity)][utl.col_date].unique())
    dt1 = dt_list[0]
    spot = df_daily[df_daily[utl.col_date] == dt1][utl.col_close].values[0]
    dict_replicates = {}
    dict_options = {}
    dict_options2 = {}
    for i in np.arange(N):
        df_simulate = montecarlo(spot, vol, dt_list)
        replication = Replication(spot, dt_issue, dt_maturity, rf=rf, fee=fee)
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


# def analysis_margin(dt1, dt2, df_daily, df_intraday, df_vix):
#     res = []
#     dt_list = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique()
#     for dt in dt_list:
#         dt_issue = dt
#         dt_maturity = dt + datetime.timedelta(days=30)
#         spot = df_daily[df_daily[utl.col_date] == dt][utl.col_close].values[0]
#         strike = spot
#         replication = Replication(strike, dt_issue, dt_maturity, rf=rf, fee=fee)
#         df_vol = replication.calculate_hist_vol('1M', df_daily)
#         res_histvol, x, xx = replication.replicate_put(df_intraday, df_vol)
#         res_vix, y, yy = replication.replicate_put(df_intraday, df_vix)
#         res.append({'dt_date': dt, 'cd_vol': 'hist vol',
#                     'replicate cost': res_histvol, 'cash':replication.cash,
#                     'delta':replication.delta, 'margin':replication.margin})
#         res.append({'dt_date': dt, 'cd_vol': 'vix',
#                     'replicate cost': res_vix, 'cash':replication.cash,
#                     'delta':replication.delta, 'margin':replication.margin})
#     df_res = pd.DataFrame(res)
#     return df_res


def analysis_strikes(dt1, dt2, df_daily, df_intraday, df_vix):
    res = []
    trading_dates = sorted(df_daily[utl.col_date].unique())
    dt_list = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique()
    for dt in dt_list:
        print(dt,' - ', datetime.datetime.now())
        dt_issue = dt
        idx = trading_dates.index(dt_issue)
        dt_maturity = trading_dates[idx + 20]
        spot = df_daily[df_daily[utl.col_date] == dt][utl.col_close].values[0]
        strike_dict = creat_replication_set(spot)
        res_dic1 = {}
        res_dic2 = {}
        for m in strike_dict.keys():
            strike = strike_dict[m]
            replication = Replication(strike, dt_issue, dt_maturity, rf=rf, fee=fee)
            df_vol = replication.calculate_hist_vol('1M', df_daily)
            df_res1 = replication.replicate_put(df_intraday, df_vol)
            r_cost = df_res1['replication cost'].values[-1]
            r_pnl = df_res1['pnl replicate'].values[-1]
            o_pnl = df_res1['pnl option'].values[-1]
            o_payoff = df_res1['value option'].values[-1]
            m = round(m,2)
            res_dic1.update({
                'pnl replicate ' + str(m): r_pnl,
                'pnl option ' + str(m): o_pnl,
                'payoff option ' + str(m): o_payoff,
                'cost ' + str(m): r_cost,
                'cd_vol':'histvol'
            })

            df_res2 = replication.replicate_put(df_intraday, df_vix)
            r_cost2 = df_res2['replication cost'].values[-1]
            r_pnl2 = df_res2['pnl replicate'].values[-1]
            o_pnl2 = df_res2['pnl option'].values[-1]
            o_payoff2 = df_res2['value option'].values[-1]
            res_dic2.update({
                'pnl replicate ' + str(m): r_pnl2,
                'pnl option ' + str(m): o_pnl2,
                'payoff option ' + str(m): o_payoff2,
                'cost ' + str(m): r_cost2,
                'cd_vol': 'vix'
            })
        res_dic1.update({'dt_date': dt})
        res_dic2.update({'dt_date': dt})
        res.append(res_dic1)
        res.append(res_dic2)
    df_res = pd.DataFrame(res)
    return df_res


def creat_replication_set(spot):
    k = spot
    strike_dict = {}
    for i in np.arange(0.8, 1.25, 0.05):
        strike_dict.update({i: k * i})
    return strike_dict


plot_utl = PlotUtil()
name_code = 'IF'
id_index = 'index_300sh'
vol = 0.2
rf = 0.03
fee = 5.0 / 10000.0
dt1 = datetime.date(2018, 4, 13)
dt2 = datetime.date(2018, 5, 13)
dt_start = dt1 - datetime.timedelta(days=50)
dt_end = dt2 + datetime.timedelta(days=31)
df_vix = get_vix(dt1, dt_end)
df_cf = get_dzqh_cf_daily(dt_start, dt_end, name_code.lower())
df_cf_minute = get_dzqh_cf_minute(dt_start, dt_end, name_code.lower())
df_index = get_index_mktdata(dt_start, dt_end, id_index)
df_future = get_future_mktdata(dt_start, dt_end, name_code)
df_intraday = get_index_intraday(dt_start, dt_end, id_index)
df_vix[utl.col_close] = df_vix[utl.col_close] / 100.0

"""1、基于蒙特卡洛模拟的复制结果"""
# print('start')
# df = syncetic_payoff(dt1, df_index, 0.2, 100)
# stocks = df['stocks']
# replicates = df['replicating pnl']
# options = df['option pnl']
# option_payoff = df['option payoff']
# print(df)
# plot_utl.plot_line_chart(stocks, [replicates, options,option_payoff], ['replicate pnl', 'option pnl','option payoff'])
# plt.show()


"""2、基于沪深300指数历史数据的复制结果"""
print('start')
df_idx = analysis_strikes(dt1, dt2, df_index, df_intraday, df_vix)
# df_fut = analysis_strikes(dt1, dt2, df_cf, df_cf_minute, df_vix)
# df_simulate = simulation_analysis(dt1, dt2, df_index, 0.2)
print(df_idx)
df_idx.to_excel('../data/res_sh300_index.xlsx')

""""""
