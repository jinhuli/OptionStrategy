from back_test.model.base_option_set import BaseOptionSet
from data_access.get_data import get_50option_mktdata,get_comoption_mktdata
import back_test.model.constant as c
import datetime
from PricingLibrary.BinomialModel import BinomialTree

start_date = datetime.date(2018, 8, 7)
end_date = datetime.date(2018, 8, 7)
df_metrics = get_comoption_mktdata(start_date, end_date,c.Util.STR_M)
exercise_type = c.OptionExerciseType.AMERICAN
optionset = BaseOptionSet(df_metrics)
optionset.init()
# maturities = optionset.get_maturities_list()
dt_maturity = optionset.select_maturity_date(0,min_holding=8)
spot = optionset.get_underlying_close(maturitydt=dt_maturity)
init_vol = 0.2
rf = 0.03
steps = 1000
call_list, put_list = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=dt_maturity)
print(call_list)
print(put_list)
print(spot)
base_option_call = call_list[0]
binomial_tree = BinomialTree(
    base_option_call.eval_date,
    base_option_call.maturitydt(),
    base_option_call.option_type(),
    exercise_type,
    spot,base_option_call.strike(),vol=init_vol,rf=rf,n=1000)
(iv_call, estimated_call) = binomial_tree.estimate_vol(base_option_call.mktprice_close())

print(iv_call)

base_option_put = put_list[0]
binomial_tree = BinomialTree(
    base_option_put.eval_date,
    base_option_put.maturitydt(),
    base_option_put.option_type(),
    exercise_type,
    spot,base_option_put.strike(),vol=init_vol,rf=rf,n=1000)
(iv_put, estimated_put) = binomial_tree.estimate_vol(base_option_put.mktprice_close())
print(iv_put)




print('')


