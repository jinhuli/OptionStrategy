from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
import data_access.get_data as get_data
import back_test.model.constant as c
import datetime
import numpy as np
from OptionStrategyLib.OptionReplication.synthetic_option import SytheticOption
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
from Utilities.timebase import LLKSR, KALMAN, LLT
from back_test.model.trade import Order


def filtration(df_iv_stats, name_column):
    """ Filtration : LLT """
    df_iv_stats['LLT_60'] = LLT(df_iv_stats[name_column], 60)
    df_iv_stats['LLT_30'] = LLT(df_iv_stats[name_column], 30)
    df_iv_stats['LLT_20'] = LLT(df_iv_stats[name_column], 20)
    df_iv_stats['LLT_15'] = LLT(df_iv_stats[name_column], 15)
    df_iv_stats['LLT_10'] = LLT(df_iv_stats[name_column], 10)
    df_iv_stats['LLT_5'] = LLT(df_iv_stats[name_column], 5)
    df_iv_stats['LLT_3'] = LLT(df_iv_stats[name_column], 3)
    df_iv_stats['diff_60'] = df_iv_stats['LLT_60'].diff()
    df_iv_stats['diff_30'] = df_iv_stats['LLT_30'].diff()
    df_iv_stats['diff_20'] = df_iv_stats['LLT_20'].diff()
    df_iv_stats['diff_15'] = df_iv_stats['LLT_15'].diff()
    df_iv_stats['diff_10'] = df_iv_stats['LLT_10'].diff()
    df_iv_stats['diff_5'] = df_iv_stats['LLT_5'].diff()
    df_iv_stats['diff_3'] = df_iv_stats['LLT_3'].diff()
    df_iv_stats = df_iv_stats.set_index(c.Util.DT_DATE)
    df_iv_stats['last_diff_60'] = df_iv_stats['diff_60'].shift()
    df_iv_stats['last_diff_30'] = df_iv_stats['diff_30'].shift()
    df_iv_stats['last_diff_20'] = df_iv_stats['diff_20'].shift()
    df_iv_stats['last_diff_15'] = df_iv_stats['diff_15'].shift()
    df_iv_stats['last_diff_10'] = df_iv_stats['diff_10'].shift()
    df_iv_stats['last_diff_5'] = df_iv_stats['diff_5'].shift()
    df_iv_stats['last_diff_3'] = df_iv_stats['diff_3'].shift()
    return df_iv_stats


pu = PlotUtil()
start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2018, 10, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 20  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
m = 1  # 期权notional倍数
cd_trade_price = c.CdTradePrice.VOLUME_WEIGHTED

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)
df_futures_all_daily = get_data.get_mktdata_future_daily(start_date, end_date,
                                                         name_code)  # daily data of all future contracts

""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(dt_histvol, end_date, name_code_option)
df_ivix = df_iv[df_iv[c.Util.CD_OPTION_TYPE] == 'ivix']
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE] == 'call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE] == 'put']
df_iv_htbr = df_iv[df_iv[c.Util.CD_OPTION_TYPE] == 'put_call_htbr']
df_data = df_iv_call[[c.Util.DT_DATE, c.Util.PCT_IMPLIED_VOL]].rename(columns={c.Util.PCT_IMPLIED_VOL: 'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE, c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE), on=c.Util.DT_DATE,
                       how='outer') \
    .rename(columns={c.Util.PCT_IMPLIED_VOL: 'iv_put'})
df_data = df_data.dropna().reset_index(drop=True)
df_data.loc[:, 'average_iv'] = (df_data.loc[:, 'iv_call'] + df_data.loc[:, 'iv_put']) / 2
# df_data = df_iv_htbr.reset_index(drop=True).rename(columns={c.Util.PCT_IMPLIED_VOL:'average_iv'})
# df_data = df_ivix.reset_index(drop=True).rename(columns={c.Util.PCT_IMPLIED_VOL:'average_iv'})
df_data['iv_htbr'] = df_iv_htbr.reset_index(drop=True)[c.Util.PCT_IMPLIED_VOL]
# df_data.to_csv('iv.csv')
df_iv_stats = df_data[[c.Util.DT_DATE, 'average_iv', 'iv_htbr']]
df_iv_stats = filtration(df_iv_stats, 'average_iv')

df_iv_stats.to_csv('../../accounts_data/iv_filtered_by_LLT.csv')