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
start_date = datetime.date(2017, 5, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=40)
name_code = c.Util.STR_M
min_holding = 8


df_metrics = get_data.get_comoption_mktdata(start_date, end_date,c.Util.STR_M)
df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)
""" 历史波动率 """
df_vol_1m = Histvol.hist_vol(df_future_c1_daily,n=21)
df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
df_garman_klass = Histvol.garman_klass(df_future_c1_daily)

df_vol_1m = df_vol_1m[(df_vol_1m.index >=start_date)]
""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(start_date,end_date,name_code)
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
df_iv_call = df_iv_call.join(df_vol_1m,on=c.Util.DT_DATE,how='outer')
df_iv_put = df_iv_put.join(df_vol_1m,on=c.Util.DT_DATE,how='outer')
df_iv_put = df_iv_put.join(df_garman_klass,on=c.Util.DT_DATE,how='left')

df_iv_call.loc[:,'amt_diff'] = df_iv_call.loc[:,c.Util.AMT_HISTVOL]-df_iv_call.loc[:,c.Util.PCT_IMPLIED_VOL]
df_iv_put.loc[:,'amt_diff'] = df_iv_put.loc[:,c.Util.AMT_HISTVOL]-df_iv_put.loc[:,c.Util.PCT_IMPLIED_VOL]
df_iv_put.to_csv('../../data/df_iv_put.csv')
df_iv_call.to_csv('../../data/df_iv_call.csv')
dates = list(df_iv_call[c.Util.DT_DATE])
ivs_call = list(df_iv_call[c.Util.PCT_IMPLIED_VOL])
ivs_put = list(df_iv_put[c.Util.PCT_IMPLIED_VOL])
histvols = list(df_vol_1m[c.Util.AMT_HISTVOL])
diff_call = list(df_iv_call['amt_diff'])
pu.plot_line_chart(dates,[ivs_call,ivs_put, histvols],['iv call','iv_put','hist_vol'])
pu.plot_line_chart(dates,[diff_call],['diff call'])
print('')

plt.show()
