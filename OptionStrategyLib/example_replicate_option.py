import datetime
import pandas as pd
import numpy as np
import math
from back_test.BktUtil import BktUtil
from data_access.get_data import get_future_mktdata, get_index_mktdata, get_index_intraday
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.OptionReplication.ReplicateOption import ReplicateOption

""" Simulate  """


def senario_analysis(dt1, dt2, df_daily, df_intraday, df_vix):
    df_res = pd.DataFrame()
    res = []
    # res_vixs = []
    dt_list = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique()
    for dt in dt_list:
        dt_issue = dt
        dt_maturity = dt + datetime.timedelta(days=30)
        k = df_daily[df_daily[utl.col_date] == dt][utl.col_close].values[0]  # ATM Option
        strike_dict = {0.8: k * 0.8, 0.85: k * 0.85, 0.9: k * 0.9, 0.95: k * 0.95, 1: k,
                      1.05: k * 1.05, 1.1: k * 1.1, 1.15: k * 1.15, 1.2: k * 1.2}
        res_dic1 = {}
        res_dic2 = {}
        for m in strike_dict.keys():
            strike = strike_dict[m]
            replication = ReplicateOption(strike, dt_issue, dt_maturity, rf=rf, fee=fee)
            df_vol = replication.calculate_hist_vol('1M', df_daily)
            res_histvol = replication.replicate_put(df_intraday, df_vol)
            res_vix = replication.replicate_put(df_intraday, df_vix)
            res_dic1.update({m:res_histvol})
            res_dic2.update({m:res_vix})
        res_dic2.update({'dt_date':dt,'cd_vol':'hist vol'})
        res_dic1.update({'dt_date':dt,'cd_vol':'vix'})
        res.append(res_dic1)
        res.append(res_dic1)
    df_res = pd.DataFrame(res)
    return df_res


plot_utl = PlotUtil()
utl = BktUtil()
name_code = 'IF'
id_index = 'index_300sh'
vol = 0.2
rf = 0.03
fee = 5.0 / 10000.0
# dt_issue = datetime.date(2018, 3, 1)
# dt_maturity = datetime.date(2018, 4, 13)
dt1 = datetime.date(2017, 3, 1)
dt2 = datetime.date(2017, 4, 13)
dt_start = dt1 - datetime.timedelta(days=50)
dt_end = dt2 + datetime.timedelta(days=31)
df_index = get_index_mktdata(dt_start, dt_end, id_index)
df_future = get_future_mktdata(dt_start, dt_end, name_code)
df_intraday = get_index_intraday(dt_start, dt_end, id_index)
df_vix = get_index_mktdata(dt1, dt2, 'index_cvix')
df_vix[utl.col_close] = df_vix[utl.col_close] / 100.0

# strike = df_index[df_index[utl.dt_date] == dt_issue][utl.col_close].values[-1]  # ATM Strike
# replication = ReplicateOption(strike, dt_issue, dt_maturity, rf=rf, fee=fee)
# df_vol = replication.calculate_hist_vol('1M', df_index)
# res = replication.replicate_put( df_intraday, df_vol)
#
# df_vol1 = df_vix
# res1 = replication.replicate_put(df_intraday, df_vol1)
# print(res)
# print(res1)

df_res = senario_analysis(dt1, dt2, df_index, df_intraday, df_vix)

print(df_res)