from back_test.model.base_option_set import BaseOptionSet
from data_access.get_data import get_50option_mktdata, get_comoption_mktdata
import back_test.model.constant as c
import datetime
from PricingLibrary.BinomialModel import BinomialTree
import Utilities.admin_write_util as admin
import numpy as np

start_date = datetime.date(2018, 1, 1)
end_date = datetime.date(2018, 1, 9)
init_vol = 0.2
rf = 0.03
steps = 1000

""" namecode : M """
table_iv = admin.table_implied_volatilities()
df_metrics = get_50option_mktdata(start_date, end_date)
exercise_type = c.OptionExerciseType.AMERICAN
optionset = BaseOptionSet(df_metrics)
optionset.init()
dt_maturity = optionset.select_maturity_date(0, min_holding=8)
# spot = optionset.get_underlying_close(maturitydt=dt_maturity)

mdt_calls, mdt_puts = optionset.get_orgnized_option_dict_for_moneyness_ranking()
mdt_options_dict = mdt_calls.get(dt_maturity)
spot = 2.84
res = optionset.get_strike_monenyes_rank_dict_nearest_strike(spot, list(mdt_options_dict.keys()),
                                                                             c.OptionType.CALL)
k_put = optionset.OptionUtilClass.get_strike_by_monenyes_rank_nearest_strike(spot, 0,
                                                                               list(mdt_options_dict.keys()),
                                                                               c.OptionType.PUT)
put_list = mdt_options_dict.get(k_put)
# call_list, put_list = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=dt_maturity)
print(spot)
print(res)
print(put_list)
for spot in np.arange(2.0, 3.5, 0.07):
    res = optionset.OptionUtilClass.get_strike_monenyes_rank_dict_nearest_strike(spot, list(mdt_options_dict.keys()),
                                                                                 c.OptionType.CALL)
    k_put = optionset.OptionUtilClass.get_strike_by_monenyes_rank_nearest_strike(spot, 0,
                                                                                   list(mdt_options_dict.keys()),
                                                                                   c.OptionType.PUT)
    put_list = mdt_options_dict.get(k_put)
    # call_list, put_list = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=dt_maturity)
    print(spot)
    print(res)
    print(put_list)
