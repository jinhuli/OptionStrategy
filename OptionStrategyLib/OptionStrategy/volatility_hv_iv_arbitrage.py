from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime
from PricingLibrary.BinomialModel import BinomialTree
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
import Utilities.admin_util as admin

start_date = datetime.date(2018, 8, 1)
end_date = datetime.date(2018, 8, 10)
name_code = c.Util.STR_M
min_holding = 8

df_metrics = get_data.get_comoption_mktdata(start_date, end_date,c.Util.STR_M)
df_future_c1_daily = get_data.get_future_c1_by_option_daily(start_date, end_date, name_code, min_holding)
""" 历史波动率 """
df_vol_1m = Histvol.hist_vol(df_future_c1_daily)
df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
df_garman_klass = Histvol.garman_klass(df_future_c1_daily)

""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(start_date,end_date,name_code)


print('')


