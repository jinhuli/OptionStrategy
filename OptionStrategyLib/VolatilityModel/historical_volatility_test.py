from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
from Utilities.PlotUtil import PlotUtil


pu = PlotUtil()
start_date = datetime.date(2010, 1, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
# df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_dzqh_cf_c1_daily(dt_histvol, end_date, name_code)
# df_future_c1_daily = get_data.get_index_mktdata(start_date,end_date,'index_50sh')
""" 历史波动率 """
df_cc_1m = Histvol.hist_vol(df_future_c1_daily)
# df_p_1m = Histvol.parkinson_number(df_future_c1_daily)
# df_gk_1m = Histvol.garman_klass(df_future_c1_daily)
df_data = df_future_c1_daily.join(df_cc_1m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_p_1m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_gk_1m,on=c.Util.DT_DATE,how='left')

df_cc_1w = Histvol.hist_vol(df_future_c1_daily,n=5)
# df_p_1w = Histvol.parkinson_number(df_future_c1_daily,n=5)
# df_gk_1w = Histvol.garman_klass(df_future_c1_daily,n=5)
df_data = df_data.join(df_cc_1w,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_p_1w,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_gk_1w,on=c.Util.DT_DATE,how='left')

df_cc_3m = Histvol.hist_vol(df_future_c1_daily,n=60)
# df_p_3m = Histvol.parkinson_number(df_future_c1_daily,n=60)
# df_gk_3m = Histvol.garman_klass(df_future_c1_daily,n=60)
df_data = df_data.join(df_cc_3m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_p_3m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_gk_3m,on=c.Util.DT_DATE,how='left')

df_data = df_data.dropna()

# df_data.to_csv('../../data/df_data.csv')
df_data.to_csv('../../data/df_underlying.csv')