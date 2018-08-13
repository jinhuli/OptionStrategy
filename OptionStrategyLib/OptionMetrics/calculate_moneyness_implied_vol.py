from back_test.model.base_option_set import BaseOptionSet
from data_access.get_data import get_50option_mktdata,get_comoption_mktdata
import back_test.model.constant as c
import datetime
from PricingLibrary.BinomialModel import BinomialTree
from PricingLibrary.BlackFormular import BlackFormula
from PricingLibrary.EngineQuantlib import QlBlackFormula
import Utilities.admin_write_util as admin

# start_date = datetime.date(2015, 1, 1)
start_date = datetime.date(2015, 7, 30)
end_date = datetime.date(2015, 8, 10)
# end_date = datetime.date(2018, 8, 10)
name_code = c.Util.STR_50ETF

init_vol = 0.2
rf = 0.03
steps = 1000
moneyness = -1


""" namecode : M/SR """
# df_metrics = get_comoption_mktdata(start_date, end_date,name_code)
# exercise_type = c.OptionExerciseType.AMERICAN

""" namecode : 50ETF """
df_metrics = get_50option_mktdata(start_date, end_date)
exercise_type = c.OptionExerciseType.EUROPEAN

table_iv = admin.table_implied_volatilities()
optionset = BaseOptionSet(df_metrics)
optionset.init()
dt_maturity = optionset.select_maturity_date(0,min_holding=8)
spot = optionset.get_underlying_close(maturitydt=dt_maturity)

while optionset.has_next():
    print(optionset.eval_date)
    call_list, put_list = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=dt_maturity)
    # print(call_list)
    # print(put_list)
    base_option_call = call_list[0]
    if exercise_type == c.OptionExerciseType.AMERICAN:
        cd_source = 'self_written_library'
        binomial_tree = BinomialTree(
            base_option_call.eval_date,
            base_option_call.maturitydt(),
            base_option_call.option_type(),
            exercise_type,
            spot,base_option_call.strike(),vol=init_vol,rf=rf,n=1000)
        (iv_call, estimated_call) = binomial_tree.estimate_vol(base_option_call.mktprice_close())
    else:
        cd_source = 'quantlib'
        black_formula = QlBlackFormula(
            dt_eval=base_option_call.eval_date,
            dt_maturity=base_option_call.maturitydt(),
            option_type=base_option_call.option_type(),
            spot=spot,
            strike=base_option_call.strike(),
            rf=rf
        )
        try:
            iv_call = black_formula.estimate_vol(base_option_call.mktprice_close())
            # iv_call2 = base_option_call.get_implied_vol()
        except:
            iv_call = 0.0
    print(base_option_call.id_instrument(), base_option_call.eval_date, iv_call)
    res = {
        'dt_date':base_option_call.eval_date,
        'name_code':name_code,
        'id_underlying':base_option_call.id_underlying(),
        'cd_option_type':'call',
        'cd_mdt_selection':'hp_8_1st',
        'cd_atm_criterion':'nearest_strike',
        'nbr_moneyness':0,
        'cd_source': cd_source,
        'id_instrument':base_option_call.id_instrument(),
        'dt_maturity':dt_maturity,
        'pct_implied_vol':iv_call,
        'amt_close':float(base_option_call.mktprice_close()),
        'amt_strike':float(base_option_call.strike()),
        'amt_applicable_strike':float(base_option_call.strike()),
        'amt_underlying_close':float(spot)
    }
    try:
        admin.conn_metrics().execute(table_iv.insert(), res)
        print('inserted into data base succefully ', res['dt_date'], 'call')
    except Exception as e:
        print(e)
        pass
    base_option_put = put_list[0]
    if exercise_type == c.OptionExerciseType.AMERICAN:
        cd_source = 'self_written_library'
        binomial_tree = BinomialTree(
            base_option_put.eval_date,
            base_option_put.maturitydt(),
            base_option_put.option_type(),
            exercise_type,
            spot,base_option_put.strike(),vol=init_vol,rf=rf,n=1000)
        (iv_put, estimated_put) = binomial_tree.estimate_vol(base_option_put.mktprice_close())
    else:
        cd_source = 'quantlib'
        black_formula = QlBlackFormula(
            dt_eval=base_option_put.eval_date,
            dt_maturity=base_option_put.maturitydt(),
            option_type=base_option_put.option_type(),
            spot=spot,
            strike=base_option_put.strike(),
            rf=rf
        )
        try:
            iv_put = black_formula.estimate_vol(base_option_put.mktprice_close())
            # iv_put2 = base_option_put.get_implied_vol()
        except:
            iv_put = 0.0
    print(base_option_put.id_instrument(), base_option_put.eval_date, iv_put)
    res = {
        'dt_date':base_option_put.eval_date,
        'name_code':name_code,
        'id_underlying':base_option_put.id_underlying(),
        'cd_option_type':'put',
        'cd_mdt_selection':'hp_8_1st',
        'cd_atm_criterion':'nearest_strike',
        'nbr_moneyness':0,
        'cd_source':cd_source,
        'id_instrument':base_option_put.id_instrument(),
        'dt_maturity':dt_maturity,
        'pct_implied_vol':iv_put,
        'amt_close':float(base_option_put.mktprice_close()),
        'amt_strike':float(base_option_put.strike()),
        'amt_applicable_strike':float(base_option_put.strike()),
        'amt_underlying_close':float(spot)
    }
    try:
        admin.conn_metrics().execute(table_iv.insert(), res)
        print('inserted into data base succefully ', res['dt_date'], ' put ')
    except Exception as e:
        print(e)
        pass
    optionset.next()
    dt_maturity = optionset.select_maturity_date(0, min_holding=8)
    spot = optionset.get_underlying_close(maturitydt=dt_maturity)


