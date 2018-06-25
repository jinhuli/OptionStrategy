import datetime
import pandas as pd
import numpy as np
import math
from back_test.BktUtil import BktUtil
from data_access.get_data import get_future_mktdata, get_index_mktdata, get_index_intraday, get_dzqh_cf_minute, get_dzqh_cf_daily, get_vix
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.OptionReplication.Replication import Replication


def senario_analysis(dt1, dt2, df_daily, df_intraday, df_vix):
    res = []
    dt_list = df_daily[(df_daily[utl.col_date] >= dt1) & (df_daily[utl.col_date] <= dt2)][utl.col_date].unique()
    for dt in dt_list:
        dt_issue = dt
        dt_maturity = dt + datetime.timedelta(days=30)
        spot = df_daily[df_daily[utl.col_date] == dt][utl.col_close].values[0]
        strike_dict = creat_replication_set(spot)
        res_dic1 = {}
        res_dic2 = {}
        for m in strike_dict.keys():
            strike = strike_dict[m]
            replication = Replication(strike, dt_issue, dt_maturity, rf=rf, fee=fee)
            df_vol = replication.calculate_hist_vol('1M', df_daily)
            res_histvol,x,xx = replication.replicate_put(df_intraday, df_vol)
            res_vix,y,yy = replication.replicate_put(df_intraday, df_vix)
            res_dic1.update({m: res_histvol})
            res_dic2.update({m: res_vix})
        res_dic2.update({'dt_date': dt, 'cd_vol': 'hist vol'})
        res_dic1.update({'dt_date': dt, 'cd_vol': 'vix'})
        res.append(res_dic2)
        res.append(res_dic1)
    df_res = pd.DataFrame(res)
    return df_res


def creat_replication_set(spot):
    k = spot
    strike_dict = {}
    for i in np.arange(0.8, 1.21, 0.05):
        strike_dict.update({i: k * i})
    return strike_dict


plot_utl = PlotUtil()
utl = BktUtil()
name_code = 'IF'
id_index = 'index_300sh'
vol = 0.2
rf = 0.03
fee = 5.0 / 10000.0
dt1 = datetime.date(2018, 4, 1)
dt2 = datetime.date(2018, 4, 13)
dt_start = dt1 - datetime.timedelta(days=50)
dt_end = dt2 + datetime.timedelta(days=31)
df_vix = get_vix(dt1, dt_end)
df_cf = get_dzqh_cf_daily(dt_start, dt_end, name_code.lower())
df_cf_minute = get_dzqh_cf_minute(dt_start, dt_end, name_code.lower())
df_index = get_index_mktdata(dt_start, dt_end, id_index)
df_future = get_future_mktdata(dt_start, dt_end, name_code)
df_intraday = get_index_intraday(dt_start, dt_end, id_index)
df_vix[utl.col_close] = df_vix[utl.col_close] / 100.0

# df_idx = senario_analysis(dt1, dt2, df_index, df_intraday, df_vix)
df_fut = senario_analysis(dt1, dt2, df_cf, df_cf_minute, df_vix)

print(df_fut)
