from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime
from PricingLibrary.BinomialModel import BinomialTree
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
import Utilities.admin_util as admin
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt

pu = PlotUtil()
start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=40)
min_holding = 8

""" commodity option """
# name_code = name_code_option = c.Util.STR_M
# df_metrics = get_data.get_comoption_mktdata(start_date, end_date,name_code)
# df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_dzqh_cf_c1_daily(dt_histvol, end_date, name_code)

""" 历史波动率 """
df_vol_1m = Histvol.hist_vol(df_future_c1_daily,n=21)
df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
df_garman_klass = Histvol.garman_klass(df_future_c1_daily)
df_data = df_future_c1_daily.join(df_vol_1m,on=c.Util.DT_DATE,how='left')
df_data = df_data.dropna()
""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(start_date,end_date,name_code_option)
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']

df_data = df_data.join(df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna()

df_data.loc[:,'diff_hist_call_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL]-df_data.loc[:,'iv_call']
df_data.loc[:,'diff_hist_put_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL]-df_data.loc[:,'iv_put']
df_data = df_data.sort_values(by='dt_date', ascending=False)
df_data.to_csv('../../data/df_data.csv')

# dates = list(df_data[c.Util.DT_DATE])
# ivs_call = list(df_data['iv_call'])
# ivs_put = list(df_data['iv_put'])
# histvols = list(df_data[c.Util.AMT_HISTVOL])
# diff_call = list(df_data['diff_hist_call_iv'])
# diff_put = list(df_data['diff_hist_put_iv'])
# pu.plot_line_chart(dates,[ivs_call,ivs_put, histvols],['iv call','iv_put','hist_vol'])
# pu.plot_line_chart(dates,[diff_call],['diff call'])
# plt.show()
