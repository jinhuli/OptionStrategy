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
end_date = datetime.date(2017, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=40)
name_code = c.Util.STR_M
min_holding = 5


df_metrics = get_data.get_comoption_mktdata(start_date, end_date,c.Util.STR_M)
df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)

""" 历史波动率 """
df_histvol = df_future_c1_daily[[c.Util.DT_DATE]]
df_histvol['histvol_10'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=10)
df_histvol['histvol_20'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=20)
df_histvol['histvol_60'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=20*3)
df_histvol['histvol_120'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE],n=20*6)

""" ATM隐含波动率 """
df_iv_atm = get_data.get_iv_by_moneyness(start_date,end_date,name_code,nbr_moneyness=0)
df_iv_atm_call = df_iv_atm[df_iv_atm[c.Util.CD_OPTION_TYPE]=='call']
df_iv_atm_put = df_iv_atm[df_iv_atm[c.Util.CD_OPTION_TYPE]=='put']

""" 隐含波动率曲面 """
optionset = BaseOptionSet(df_metrics)
optionset.init()
df_call_curve = optionset.get_call_implied_vol_curve(nbr_maturity=0)
df_put_curve = optionset.get_put_implied_vol_curve(nbr_maturity=0)
df_otm_curve = optionset.get_otm_implied_vol_curve(nbr_maturity=0)
print(df_call_curve)

strikes = df_call_curve[[c.Util.AMT_APPLICABLE_STRIKE]]
curve_call = df_call_curve[[c.Util.PCT_IMPLIED_VOL]]
curve_put = df_put_curve[[c.Util.PCT_IMPLIED_VOL]]
curve_otm = df_otm_curve[[c.Util.PCT_IV_OTM_BY_HTBR]]


pu.plot_line_chart(strikes,[curve_call,curve_put],['curve_call','curve_put'])
pu.plot_line_chart(strikes,[curve_otm],['curve_otm'])
plt.show()




