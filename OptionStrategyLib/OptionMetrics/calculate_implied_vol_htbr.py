from back_test.model.base_option_set import BaseOptionSet
from data_access.get_data import get_50option_mktdata,get_comoption_mktdata
import back_test.model.constant as c
import datetime
from PricingLibrary.EngineQuantlib import QlBlackFormula, QlBinomial
import Utilities.admin_write_util as admin

start_date = datetime.date(2018, 10, 1)
end_date = datetime.date.today()

init_vol = 0.2
rf = 0.03
moneyness = 0
min_holding = 8
nbr_maturity = 0
cd_mdt_selection = 'hp_8_1st'
# nbr_maturity = 1
# cd_mdt_selection = 'hp_8_2nd'
# nbr_maturity = 2
# cd_mdt_selection = 'hp_8_3rd'

# """ namecode : M/SR """
# name_code = c.Util.STR_SR
# # name_code = c.Util.STR_M
# df_metrics = get_comoption_mktdata(start_date, end_date,name_code)
# exercise_type = c.OptionExerciseType.AMERICAN

""" namecode : 50ETF """
name_code = c.Util.STR_50ETF
df_metrics = get_50option_mktdata(start_date, end_date)
exercise_type = c.OptionExerciseType.EUROPEAN

table_iv = admin.table_implied_volatilities()
optionset = BaseOptionSet(df_metrics)
optionset.init()
dt_maturity = optionset.select_maturity_date(nbr_maturity=nbr_maturity,min_holding=min_holding)
spot = optionset.get_underlying_close(maturitydt=dt_maturity)

while optionset.current_index < optionset.nbr_index:
    try:
        call_list, put_list = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=moneyness, maturity=dt_maturity)
        iv = optionset.get_atm_iv_by_htbr(dt_maturity)
    except Exception as e:
        print(e)
        optionset.next()
        dt_maturity = optionset.select_maturity_date(nbr_maturity, min_holding=min_holding)
        spot = optionset.get_underlying_close(maturitydt=dt_maturity)
        continue
    base_option_call = call_list[0]
    res = {
        'dt_date':optionset.eval_date,
        'name_code':name_code,
        'id_underlying':base_option_call.id_underlying(),
        'cd_option_type':'put_call_htbr',
        'cd_mdt_selection':cd_mdt_selection,
        'cd_atm_criterion':'nearest_strike',
        'nbr_moneyness':moneyness,
        'cd_source': 'quantlib',
        'id_instrument':None,
        'dt_maturity':dt_maturity,
        'pct_implied_vol':iv,
        'amt_close':None,
        'amt_strike':None,
        'amt_applicable_strike':None,
        'amt_underlying_close':float(spot)
    }
    try:
        admin.conn_metrics().execute(table_iv.insert(), res)
        print('inserted into data base succefully ', res['dt_date'])
    except Exception as e:
        print(e)
        pass
    if not optionset.has_next(): break
    optionset.next()
    dt_maturity = optionset.select_maturity_date(nbr_maturity, min_holding=min_holding)
    spot = optionset.get_underlying_close(maturitydt=dt_maturity)


