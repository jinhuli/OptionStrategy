from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from data_access import get_data
import back_test.model.constant as c
import datetime
import numpy as np
from OptionStrategyLib.OptionReplication.synthetic_option import SytheticOption
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
from Utilities.timebase import LLKSR,KALMAN,LLT


pu = PlotUtil()
start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 15
init_fund = c.Util.BILLION
slippage = 0
m = 1 # 期权notional倍数

""" commodity option """
name_code = name_code_option = c.Util.STR_M
df_metrics = get_data.get_comoption_mktdata(start_date, end_date,name_code)
df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)

""" 50ETF option """
# name_code = c.Util.STR_IH
# name_code_option = c.Util.STR_50ETF
# df_metrics = get_data.get_50option_mktdata(start_date, end_date)
# df_future_c1_daily = get_data.get_mktdata_cf_c1_daily(dt_histvol, end_date, name_code)

""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(dt_histvol,end_date,name_code_option)
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
df_data = df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna().reset_index(drop=True)
df_data.loc[:,'average_iv'] = (df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put'])/2

""" Volatility Statistics """
df_iv_stats = df_data[[c.Util.DT_DATE, 'average_iv']]
df_iv_stats.loc[:,'std'] = np.log(df_iv_stats['average_iv']).diff()

""" 1. MA """

df_iv_stats.loc[:,'ma_20'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=20)
df_iv_stats.loc[:,'ma_10'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=10)
df_iv_stats.loc[:,'ma_5'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=5)
df_iv_stats['diff_20'] = df_iv_stats['ma_20'].diff()
df_iv_stats['diff_10'] = df_iv_stats['ma_10'].diff()
df_iv_stats['diff_5'] = df_iv_stats['ma_5'].diff()

""" 2. Filtration ：LLKSR """
df_iv_stats['LLKSR_20'] = LLKSR(df_iv_stats['average_iv'], 20)
df_iv_stats['LLKSR10'] = LLKSR(df_iv_stats['average_iv'], 10)
df_iv_stats['LLKSR_5'] = LLKSR(df_iv_stats['average_iv'], 5)
df_iv_stats['diff_20'] = df_iv_stats['LLKSR_20'].diff()
df_iv_stats['diff_10'] = df_iv_stats['LLKSR10'].diff()
df_iv_stats['diff_5'] = df_iv_stats['LLKSR_5'].diff()

""" 3. Filtration : KALMAN """
df_iv_stats['KALMAN_20'] = KALMAN(df_iv_stats['average_iv'], 20)
df_iv_stats['KALMAN_10'] = KALMAN(df_iv_stats['average_iv'], 10)
df_iv_stats['KALMAN_5'] = KALMAN(df_iv_stats['average_iv'], 5)
df_iv_stats['diff_20'] = df_iv_stats['KALMAN_20'].diff()
df_iv_stats['diff_10'] = df_iv_stats['KALMAN_10'].diff()
df_iv_stats['diff_5'] = df_iv_stats['KALMAN_5'].diff()

""" 4. Filtration : LLT """
df_iv_stats['LLT_20'] = LLT(df_iv_stats['average_iv'], 20)
df_iv_stats['LLT_10'] = LLT(df_iv_stats['average_iv'], 10)
df_iv_stats['LLT_5'] = LLT(df_iv_stats['average_iv'], 5)
df_iv_stats['diff_20'] = df_iv_stats['LLT_20'].diff()
df_iv_stats['diff_10'] = df_iv_stats['LLT_10'].diff()
df_iv_stats['diff_5'] = df_iv_stats['LLT_5'].diff()

df_iv_stats = df_iv_stats.set_index(c.Util.DT_DATE)

pu.plot_line_chart(list(df_iv_stats.index), [list(df_iv_stats['average_iv']),list(df_iv_stats['std'])],
                   ['iv','chg of vol'])
pu.plot_line_chart(list(df_iv_stats.index), [list(df_iv_stats['ma_5']), list(df_iv_stats['KALMAN_5']), list(df_iv_stats['LLKSR_5']), list(df_iv_stats['LLT_5'])],
                   ['ma_5','KALMAN_5','LLKSR_5','LLT_5'])
pu.plot_line_chart(list(df_iv_stats.index), [list(df_iv_stats['ma_20']), list(df_iv_stats['KALMAN_20']), list(df_iv_stats['LLKSR_20']), list(df_iv_stats['LLT_20'])],
                   ['ma_20','KALMAN_20','LLKSR_20','LLT_20'])
plt.show()
