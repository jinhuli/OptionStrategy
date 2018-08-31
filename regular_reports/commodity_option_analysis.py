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
min_holding = 5


df_metrics = get_data.get_comoption_mktdata(start_date, end_date,c.Util.STR_M)
df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)

""" 历史波动率 """
df_histvol = Histvol.hist_vol(df_future_c1_daily,n=10)
df_histvol = Histvol.hist_vol(df_histvol,n=20)
df_histvol = Histvol.hist_vol(df_histvol,n=20*3)
df_histvol = Histvol.hist_vol(df_histvol,n=20*6)

""" 隐含波动率 """
df_iv_atm = get_data.get_iv_by_moneyness(start_date,end_date,name_code,nbr_moneyness=0)
df_iv_call = df_iv_atm[df_iv_atm[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv_atm[df_iv_atm[c.Util.CD_OPTION_TYPE]=='put']

""" 隐含波动率曲面 """