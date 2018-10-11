import data_access.get_data as get_data
import back_test.model.constant as c
import datetime
import pandas as pd
from Utilities.timebase import LLKSR,KALMAN,LLT
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
import numpy as np
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.VolatilityModel.kernel_density import kde_sklearn
from matplotlib import pylab as plt

def filtration(df_iv_stats, name_column):
    """ Filtration : LLT """
    df_iv_stats['LLT_20'] = LLT(df_iv_stats['average_iv'], 20)
    df_iv_stats['LLT_10'] = LLT(df_iv_stats['average_iv'], 10)
    df_iv_stats['LLT_5'] = LLT(df_iv_stats['average_iv'], 5)
    df_iv_stats['LLT_3'] = LLT(df_iv_stats['average_iv'], 3)
    df_iv_stats['diff_20'] = df_iv_stats['LLT_20'].diff()
    df_iv_stats['diff_10'] = df_iv_stats['LLT_10'].diff()
    df_iv_stats['diff_5'] = df_iv_stats['LLT_5'].diff()
    df_iv_stats['diff_3'] = df_iv_stats['LLT_3'].diff()
    df_iv_stats = df_iv_stats.set_index(c.Util.DT_DATE)
    df_iv_stats['last_diff_20'] = df_iv_stats['diff_20'].shift()
    df_iv_stats['last_diff_10'] = df_iv_stats['diff_10'].shift()
    df_iv_stats['last_diff_5'] = df_iv_stats['diff_5'].shift()
    df_iv_stats['last_diff_3'] = df_iv_stats['diff_3'].shift()
    return df_iv_stats

pu = PlotUtil()

start_date = datetime.date(2015, 2, 1)
end_date = datetime.date(2018, 10, 8)
dt_histvol = start_date - datetime.timedelta(days=90)

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)
name_code_index = c.Util.STR_INDEX_50SH
df_index = get_data.get_index_mktdata(dt_histvol,end_date,name_code_index)

"""历史波动率"""
df_histvol = pd.DataFrame(df_index[c.Util.DT_DATE])
df_histvol['histvol_50sh'] = Histvol.hist_vol(df_index[c.Util.AMT_CLOSE])

""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(dt_histvol,end_date,name_code_option)
df_ivix = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='ivix']
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
df_iv_htbr = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put_call_htbr']
df_data = df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna().reset_index(drop=True)
df_data.loc[:,'average_iv'] = (df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put'])/2
df_data.loc[:,'ivix'] = df_ivix.reset_index(drop=True)[c.Util.PCT_IMPLIED_VOL]
df_iv = df_data[[c.Util.DT_DATE, 'average_iv','ivix']]
df_iv['ivix_shift_20d'] = df_iv['ivix'].shift(20)
df_iv['iv_shift_20d'] = df_iv['average_iv'].shift(20)


df_data = pd.merge(df_histvol,df_iv,on=c.Util.DT_DATE)


df_data['iv_premium'] = df_data['ivix_shift_20d']-df_data['histvol_50sh']
df_data = df_data.dropna()
vols = np.array(df_data['iv_premium'])
# garman_klass_vols = np.array(df_data[c.Util.AMT_GARMAN_KLASS+'_20'])
# parkinson_vols = np.array(df_data[c.Util.AMT_PARKINSON_NUMBER+'_20'])
# logvols = np.log(histvols)

x_grid = np.linspace(min(vols), max(vols), 1000)

# plt.figure(1)
pdf_cc = kde_sklearn(vols, x_grid, bandwidth=0.03)
pu.plot_line_chart(x_grid,[pdf_cc],['Kernel Density of IV Premium'])
plt.show()
df_data.to_csv('../../accounts_data/implied_vol_premiums.csv')
# f.save('../../accounts_data/implied_vol_premiums.csv')


