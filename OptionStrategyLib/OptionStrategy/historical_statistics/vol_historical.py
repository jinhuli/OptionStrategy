from data_access import get_data
import back_test.model.constant as c
import datetime
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
from Utilities import timebase
import numpy as np

pu = PlotUtil()
start_date = datetime.date(2010, 1, 1)
end_date = datetime.date.today()
dt_histvol = start_date - datetime.timedelta(days=40)
# min_holding = 18

""" commodity option """
name_code = name_code_option = c.Util.STR_CU
df_metrics = get_data.get_comoption_mktdata(start_date, end_date,name_code)
df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)


""" 历史波动率 """
vol_10 = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=10)
vol_20 = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=20)
vol_30 = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=30)
vol_60 = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=60)
vol_90 = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=90)


df = df_future_c1_daily[[c.Util.DT_DATE]]
df['hist_vol_10'] = vol_10
df['hist_vol_20'] = vol_20
df['hist_vol_30'] = vol_30
df['hist_vol_60'] = vol_60
df['hist_vol_90'] = vol_90
df = df.dropna()
df = df.sort_values(c.Util.DT_DATE,ascending=False)
print(df)
df.to_csv('../../../data/cu_histvol.csv')