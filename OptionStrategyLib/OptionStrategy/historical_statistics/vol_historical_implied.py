from data_access import get_data
import back_test.model.constant as c
import datetime
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol

pu = PlotUtil()
start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=40)
min_holding = 18

""" commodity option """
# name_code = name_code_option = c.Util.STR_M
# df_metrics = get_data.get_comoption_mktdata(start_date, end_date,name_code)
# df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_dzqh_cf_c1_daily(dt_histvol, end_date, name_code)
name_code_index = c.Util.STR_INDEX_50SH
df_index = get_data.get_index_mktdata(dt_histvol,end_date,name_code_index)
""" 历史波动率 """
df_vol_1m = Histvol.hist_vol(df_index)
# df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
# df_garman_klass = Histvol.garman_klass(df_future_c1_daily)
df_data = df_future_c1_daily.join(df_vol_1m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_garman_klass,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_parkinson_1m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.dropna()

# df_data = df_future_c1_daily
""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(start_date,end_date,name_code_option)

df_iv['selected_date'] = df_iv[c.Util.DT_MATURITY].apply(lambda x:x-datetime.timedelta(days=30))
df_iv = df_iv[df_iv['selected_date']==df_iv[c.Util.DT_DATE]]
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']

df_iv_call = df_iv_call[[c.Util.PCT_IMPLIED_VOL,c.Util.DT_MATURITY]].rename(columns={c.Util.DT_MATURITY:'dt_key'}).set_index('dt_key')
df_iv_put = df_iv_put[[c.Util.PCT_IMPLIED_VOL,c.Util.DT_MATURITY]].rename(columns={c.Util.DT_MATURITY:'dt_key'}).set_index('dt_key')
df_data = df_data.rename(columns={c.Util.DT_DATE:'dt_key'})
df_data = df_data.join(df_iv_call,on='dt_key',how='right').rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put,on='dt_key',how='right').rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
# df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
#     .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna()
df_data.loc[:,'average_iv'] = df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put']
df_data.loc[:,'premium_put'] = df_data.loc[:,'iv_put'] - df_data.loc[:,c.Util.AMT_HISTVOL+'_'+str(20)]
df_data.loc[:,'premium_call'] = df_data.loc[:,'iv_call'] - df_data.loc[:,c.Util.AMT_HISTVOL+'_'+str(20)]
df_data.to_csv('df_data.csv')
pu.plot_line_chart(list(df_data['dt_key']),[list(df_data['premium_put']),list(df_data['premium_call']),[0.0]*len(list(df_data['premium_call']))],['volatility premium put','volatility premium call','zero'])
plt.show()
# df_data.loc[:,'diff_hist_call_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL+'_20']-df_data.loc[:,'iv_call']
# df_data.loc[:,'diff_hist_put_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL+'_20']-df_data.loc[:,'iv_put']
# df_data = df_data.sort_values(by='dt_date', ascending=False)
# df_data.to_csv('../../data/df_data.csv')

df_iv_stats = df_data[[c.Util.DT_DATE, 'average_iv']]

df_iv_stats.loc[:,'iv_std_60'] = c.Statistics.standard_deviation(df_iv_stats['average_iv'], n=60)
df_iv_stats.loc[:,'ma_60'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=60)
df_iv_stats.loc[:,'ma_20'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=20)
df_iv_stats.loc[:,'ma_10'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=10)
df_iv_stats.loc[:,'ma_3'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=3)
df_iv_stats = df_iv_stats.dropna()
df_iv_stats.loc[:,'upper'] = upper = df_iv_stats.loc[:,'ma_60'] + df_iv_stats.loc[:,'iv_std_60']
df_iv_stats.loc[:,'lower'] = lower = df_iv_stats.loc[:,'ma_60'] - df_iv_stats.loc[:,'iv_std_60']

dates = list(df_iv_stats[c.Util.DT_DATE])
upper = list(upper)
lower = list(lower)
vol = list(df_iv_stats['average_iv'])
ma_60 = list(df_iv_stats.loc[:,'ma_60'])
ma_20 = list(df_iv_stats.loc[:,'ma_20'])
ma_10 = list(df_iv_stats.loc[:,'ma_10'])
ma_3 = list(df_iv_stats.loc[:,'ma_3'])

# plt.figure(0)
pu.plot_line_chart(dates,[vol,ma_10, ma_20,lower,upper,ma_60],['iv','ma_10','ma_20','lower','upper','ma_60'])


# plt.figure(1)
# pu.plot_line_chart(dates,[vol, ma_3,lower,upper],['vol','ma_3','lower','upper'])


plt.show()










