from back_test.model.constant import Util
from data_access.get_data import  get_dzqh_cf_c1_daily
from OptionStrategyLib.VolatilityModel.historical_volatility import historical_volatility_model as Histvol
import datetime
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt


pu = PlotUtil()
start_date = datetime.date(2015, 4, 1)
end_date = datetime.date(2018, 6, 1)
hist_date = start_date - datetime.timedelta(days=40)
df_future_c1_daily = get_dzqh_cf_c1_daily(hist_date, end_date, 'if')

df_vol_1m = Histvol.hist_vol(df_future_c1_daily)
df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
df_garman_klass = Histvol.garman_klass(df_future_c1_daily)

df_hist_vol = df_vol_1m.join(df_parkinson_1m, how='left')
df_hist_vol = df_hist_vol.join(df_garman_klass, how='left')
# print(df_hist_vol)

dates = list(df_hist_vol.index)
hist_vol = list(df_hist_vol[Util.AMT_HISTVOL])
parkinson = list(df_hist_vol[Util.AMT_PARKINSON_NUMBER])
garman_klass = list(df_hist_vol[Util.AMT_GARMAN_KLASS])
pu.plot_line_chart(dates,[hist_vol, parkinson, garman_klass],['hist_vol', 'parkinson', 'garman_klass'])
plt.show()