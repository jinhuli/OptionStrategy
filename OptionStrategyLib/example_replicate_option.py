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
from Utilities.calculate import *

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
    spot = df_daily[df_daily[utl.col_date] == dt1][utl.col_close].values[0]
    dict_replicates = {}
    dict_options = {}
    dict_options2 = {}
    for i in np.arange(N):
        df_simulate = montecarlo(spot, vol, dt_list)
        replication = Replication(spot, dt_issue, dt_maturity, rf=rf, fee=fee_rate)
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


def analysis_strikes_with_delta_bounds(dt1, dt2, df_daily, df_intraday, df_vix, df_underlying):
    res1 = []
    res2 = []
    trading_dates = sorted(df_daily[utl.col_date].unique())
    dt_list = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique()
    df_vol = get_hist_vol('1M', df_daily)
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

            # df_res2 = replication.replicate_delta_bounds(df_intraday, df_vix)
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


def creat_replication_set(spot):
    # k = spot
    strike_dict = {0.9: 0.9 * spot,
                   0.95: 0.95 * spot,
                   1.0: 1.0 * spot,
                   1.05: 1.05 * spot,
                   1.1: 1.1 * spot,
                   }
    # strike_dict = {
    #                0.98: 0.98 * spot,
    #                1.0: 1.0 * spot,
    #                1.02: 1.02 * spot,
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
    dt_maturity = trading_dates[idx + 60]
    strike_dict = creat_replication_set(fut)
    df_vol = get_hist_vol('1M', df_cf)
    for m in strike_dict.keys():
        strike = strike_dict[m]
        replication = Replication(strike, dt_issue, dt_maturity, rf=rf, fee=fee_rate)
        df_res1 = replication.replicate_put(df_cf_minute, df_vol)
        df_res2 = replication.replicate_put(df_cf_minute, df_vix)
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


def replicate_with_timing():

    return


plot_utl = PlotUtil()
name_code = 'IF'
id_index = 'index_300sh'
vol = 0.2
rf = 0.03
fee_rate = 5.0 / 10000.0

"""1/2/3 data"""
# dt1 = datetime.date(2017, 4, 5)
# # dt1 = datetime.date(2017, 3, 7)
# # dt1 = datetime.date(2018, 5, 8)
# # dt2 = datetime.date(2017, 2, 10)
# dt2 = datetime.date(2018, 5, 13)
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


# """ local data : dt1 = datetime.date(2017, 1, 1)
#                  dt2 = datetime.date(2018, 5, 13) """
dt1 = datetime.date(2017, 4, 5)
dt2 = datetime.date(2018, 5, 13)
df_vix = pd.ExcelFile('../data/replicate/df_vix.xlsx').parse("Sheet1")
df_vix.loc[:,'dt_date'] = df_vix['dt_date'].apply(lambda x:x.date())
df_cf = pd.ExcelFile('../data/replicate/df_cf.xlsx').parse("Sheet1")
df_cf.loc[:,'dt_date'] = df_cf['dt_date'].apply(lambda x:x.date())
df_cf_minute = pd.ExcelFile('../data/replicate/df_cf_minute.xlsx').parse("Sheet1")
df_cf_minute.loc[:,'dt_date'] = df_cf_minute['dt_datetime'].apply(lambda x:x.date())
df_index = pd.ExcelFile('../data/replicate/df_index.xlsx').parse("Sheet1")
df_index.loc[:,'dt_date'] = df_index['dt_date'].apply(lambda x:x.date())
df_intraday = pd.ExcelFile('../data/replicate/df_intraday.xlsx').parse("Sheet1")
df_intraday.loc[:,'dt_date'] = df_intraday['dt_datetime'].apply(lambda x:x.date())

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
print('2.start')
cd_vol = '1M'
res_histvol, res_vix = analysis_strikes(dt1, dt2, df_index, df_intraday, df_vix, df_index, cd_vol)
print(res_histvol)
print(res_vix)
res_histvol.to_excel('../res_sh300index_histvol.xlsx')
res_vix.to_excel('../res_sh300index_vix.xlsx')

"""3、基于沪深300期货历史数据的复制结果"""
# print('3.start')
# cd_vol = '1M'
# res_histvol, res_vix = analysis_strikes(dt1, dt2, df_cf, df_cf_minute, df_vix, df_index, cd_vol)
# print(res_histvol)
# print(res_vix)
# res_histvol.to_excel('../res_sh300future_histvol.xlsx')
# res_vix.to_excel('../res_sh300future_vix.xlsx')

"""4、举例"""
# print('4.start')
# # dt_issue = datetime.date(2018, 1, 2)
# dt_issue = datetime.date(2018, 3, 13)
# dt_start = dt_issue - datetime.timedelta(days=50)
# dt_end = dt_issue + datetime.timedelta(days=100)
# df_vix = get_vix(dt_issue, dt_end)
#
# df_index = get_index_mktdata(dt_start, dt_end, id_index)
# # df_intraday = get_index_intraday(dt_start, dt_end, id_index)
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
# res_histvol1 = res_histvol[res_histvol['m'] == 0.98]
# res_histvol1.loc[:,'dt_date'] = res_histvol1['dt'].apply(lambda x: datetime.date(x.year, x.month, x.day))
# res_histvol1 = res_histvol1.sort_values(by='dt', ascending=False). \
#     drop_duplicates(subset=['dt_date']). \
#     sort_values(by='dt_date', ascending=True)
# res_histvol1.to_excel('../res_historic_example_histvol_m98.xlsx')
#
# res_histvol1 = res_histvol[res_histvol['m'] == 1.02]
# res_histvol1.loc[:,'dt_date'] = res_histvol1['dt'].apply(lambda x: datetime.date(x.year, x.month, x.day))
# res_histvol1 = res_histvol1.sort_values(by='dt', ascending=False). \
#     drop_duplicates(subset=['dt_date']). \
#     sort_values(by='dt_date', ascending=True)
# res_histvol1.to_excel('../res_historic_example_histvol_m102.xlsx')
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
# res_histvol, res_vix = analysis_strikes_with_delta_bounds(dt1, dt2, df_cf, df_cf_minute, df_vix, df_index)
# print(res_histvol)
# print(res_vix)
# res_histvol.to_excel('../res_sh300future_histvol_with_delta_bounds.xlsx')
# res_vix.to_excel('../res_sh300future_vix_with_delta_bounds.xlsx')
