from back_test.model.base_option_set import BaseOptionSet
from data_access.get_data import get_50option_mktdata
import back_test.model.constant as c
from PricingLibrary.EngineQuantlib import QlBlackFormula
import pandas as pd
import datetime
import math

def fun_implied_rf(df_series):
    rf = math.log(df_series[c.Util.AMT_APPLICABLE_STRIKE]/
                  (df_series[c.Util.AMT_UNDERLYING_CLOSE]+df_series[c.Util.AMT_PUT_QUOTE]
                   -df_series[c.Util.AMT_CALL_QUOTE]),math.e)/df_series[c.Util.AMT_TTM]
    return rf

def fun_iv(df_series:pd.DataFrame, option_type:c.OptionType,rf:float=0.03):
    K = df_series[c.Util.AMT_APPLICABLE_STRIKE]
    S = df_series[c.Util.AMT_UNDERLYING_CLOSE]
    dt_eval = df_series[c.Util.DT_DATE]
    dt_maturity = df_series[c.Util.DT_MATURITY]
    if option_type == c.OptionType.CALL:
        black_call = QlBlackFormula(dt_eval,dt_maturity,c.OptionType.CALL,S,K,rf=rf)
        C = df_series[c.Util.AMT_CALL_QUOTE]
        iv = black_call.estimate_vol(C)
    else:
        black_put = QlBlackFormula(dt_eval,dt_maturity,c.OptionType.PUT,S,K,rf=rf)
        P = df_series[c.Util.AMT_PUT_QUOTE]
        iv = black_put.estimate_vol(P)
    return iv

def fun_pcp_adjusted_iv(df_series:pd.DataFrame, option_type:c.OptionType):
    rf = df_series[c.Util.AMT_IMPLIED_RF]
    K = df_series[c.Util.AMT_APPLICABLE_STRIKE]
    S = df_series[c.Util.AMT_UNDERLYING_CLOSE]
    dt_eval = df_series[c.Util.DT_DATE]
    dt_maturity = df_series[c.Util.DT_MATURITY]
    if option_type == c.OptionType.CALL:
        black_call = QlBlackFormula(dt_eval,dt_maturity,c.OptionType.CALL,S,K,rf=rf)
        C = df_series[c.Util.AMT_CALL_QUOTE]
        iv = black_call.estimate_vol(C)
    else:
        black_put = QlBlackFormula(dt_eval,dt_maturity,c.OptionType.PUT,S,K,rf=rf)
        P = df_series[c.Util.AMT_PUT_QUOTE]
        iv = black_put.estimate_vol(P)
    return iv



start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2018, 1, 8)
df_metrics = get_50option_mktdata(start_date, end_date)

optionset = BaseOptionSet(df_metrics)
optionset.init()

t_qupte = optionset.get_T_quotes()

t_qupte[c.Util.AMT_IMPLIED_RF] = t_qupte.apply(fun_implied_rf,axis=1)
t_qupte['amt_iv_by_rf_adjusted'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.CALL),axis=1)
t_qupte['amt_iv_by_rf_adjusted2'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.PUT),axis=1)
t_qupte['amt_iv_call'] = t_qupte.apply(lambda x: fun_iv(x,c.OptionType.CALL),axis=1)
t_qupte['amt_iv_put'] = t_qupte.apply(lambda x: fun_iv(x,c.OptionType.PUT),axis=1)
# t_qupte['amt_iv_put_adjusted_rf'] = t_qupte.apply(lambda x: fun_calculate_iv(x,c.OptionType.PUT),axis=1)

print(t_qupte)

