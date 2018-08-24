from back_test.model.base_option_set import BaseOptionSet
from data_access.get_data import get_50option_mktdata
import back_test.model.constant as c
from PricingLibrary.EngineQuantlib import QlBlackFormula,QlBinomial
from PricingLibrary.BlackFormular import BlackFormula
from PricingLibrary.BinomialModel import BinomialTree
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import math

def fun_htb_rate(df_series,rf):
    # r = math.log(df_series[c.Util.AMT_APPLICABLE_STRIKE]/
    #               (df_series[c.Util.AMT_UNDERLYING_CLOSE]+df_series[c.Util.AMT_PUT_QUOTE]
    #                -df_series[c.Util.AMT_CALL_QUOTE]),math.e)/df_series[c.Util.AMT_TTM]
    r = -math.log((df_series[c.Util.AMT_CALL_QUOTE]-df_series[c.Util.AMT_PUT_QUOTE]
                   +df_series[c.Util.AMT_APPLICABLE_STRIKE]*math.exp(-rf*df_series[c.Util.AMT_TTM]))
                  /df_series[c.Util.AMT_UNDERLYING_CLOSE])/df_series[c.Util.AMT_TTM]
    return r

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

def fun_pcp_adjusted_iv(df_series:pd.DataFrame, option_type:c.OptionType,rf:float,htb_r:float=None):
    if htb_r is None:
        htb_r = df_series[c.Util.AMT_HTB_RATE]
    ttm = df_series[c.Util.AMT_TTM]
    K = df_series[c.Util.AMT_APPLICABLE_STRIKE]
    S = df_series[c.Util.AMT_UNDERLYING_CLOSE]*math.exp(-htb_r*ttm)
    dt_eval = df_series[c.Util.DT_DATE]
    dt_maturity = df_series[c.Util.DT_MATURITY]
    if option_type == c.OptionType.CALL:
        C = df_series[c.Util.AMT_CALL_QUOTE]
        black_call = QlBlackFormula(dt_eval,dt_maturity,c.OptionType.CALL,S,K,rf=rf)
        # black_call = BlackFormula(dt_eval,dt_maturity,c.OptionType.CALL,S,K,C,rf=rf)
        # black_call = QlBinomial(dt_eval,dt_maturity,c.OptionType.CALL,c.OptionExerciseType.EUROPEAN,S,K,0.2,rf=rf)
        # iv = black_call.ImpliedVolApproximation()
        iv = black_call.estimate_vol(C)
    else:
        P = df_series[c.Util.AMT_PUT_QUOTE]
        black_put = QlBlackFormula(dt_eval,dt_maturity,c.OptionType.PUT,S,K,rf=rf)
        # black_put = BlackFormula(dt_eval,dt_maturity,c.OptionType.PUT,S,K,P,rf=rf)
        # black_put = QlBinomial(dt_eval,dt_maturity,c.OptionType.PUT,c.OptionExerciseType.EUROPEAN,S,K,0.2,rf=rf)
        # iv = black_put.ImpliedVolApproximation()
        iv = black_put.estimate_vol(P)
    return iv



start_date = datetime.date(2015, 8, 18)
end_date = datetime.date(2015, 9, 8)
rf = 0.03
df_metrics = get_50option_mktdata(start_date, end_date)
pu = PlotUtil()

optionset = BaseOptionSet(df_metrics)
optionset.init()
nbr_maturity = 1
mdt1 = optionset.get_maturities_list()[nbr_maturity]
t_qupte = optionset.get_T_quotes(nbr_maturity)

t_qupte[c.Util.AMT_HTB_RATE] = t_qupte.apply(lambda x: fun_htb_rate(x,rf),axis=1)

htb_r_vw = (t_qupte.loc[:,c.Util.AMT_HTB_RATE]*t_qupte.loc[:,c.Util.AMT_TRADING_VOLUME]).sum()/\
           t_qupte.loc[:,c.Util.AMT_TRADING_VOLUME].sum()
# htb_r_vw = t_qupte.loc[:,c.Util.AMT_HTB_RATE].mean()
min_k_series = t_qupte.loc[t_qupte[c.Util.AMT_APPLICABLE_STRIKE].idxmin()]
htb_r_mp = fun_htb_rate(min_k_series,rf)
t_qupte.loc[:,'diff'] = abs(t_qupte.loc[:,c.Util.AMT_APPLICABLE_STRIKE]-t_qupte.loc[:,c.Util.AMT_UNDERLYING_CLOSE])
atm_series = t_qupte.loc[t_qupte['diff'].idxmin()]
htb_r_atm = fun_htb_rate(atm_series,rf)
# htb_r_vw = optionset.get_implied_rf_vwpcr(nbr_maturity)

t_qupte['amt_iv_adj_call'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.CALL,rf,htb_r=0.388),axis=1)
t_qupte['amt_iv_adj_put'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.PUT,rf,htb_r=0.388),axis=1)
t_qupte['amt_iv_adj_call_mk'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.CALL,rf,htb_r=htb_r_mp),axis=1)
t_qupte['amt_iv_adj_put_mk'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.PUT,rf,htb_r=htb_r_mp),axis=1)
t_qupte['amt_iv_adj_call_vw'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.CALL,rf,htb_r=htb_r_vw),axis=1)
t_qupte['amt_iv_adj_put_vw'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.PUT,rf,htb_r=htb_r_vw),axis=1)
t_qupte['amt_iv_adj_call_pk'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.CALL,rf),axis=1)
t_qupte['amt_iv_adj_put_pk'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.PUT,rf),axis=1)
t_qupte['amt_iv_adj_call_atm'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.CALL,rf,htb_r=htb_r_atm),axis=1)
t_qupte['amt_iv_adj_put_atm'] = t_qupte.apply(lambda x: fun_pcp_adjusted_iv(x,c.OptionType.PUT,rf,htb_r=htb_r_atm),axis=1)
t_qupte['amt_iv_call'] = t_qupte.apply(lambda x: fun_iv(x,c.OptionType.CALL),axis=1)
t_qupte['amt_iv_put'] = t_qupte.apply(lambda x: fun_iv(x,c.OptionType.PUT),axis=1)
# t_qupte['amt_iv_put_adjusted_rf'] = t_qupte.apply(lambda x: fun_calculate_iv(x,c.OptionType.PUT),axis=1)

print(t_qupte)


print('htb_r_vw : ',htb_r_vw)
print('htb_r_mp : ',htb_r_mp)
print('htb_r_atm : ',htb_r_atm)

for option in optionset.get_dict_options_by_maturities()[mdt1]:
    iv = option.get_implied_vol_adjusted_by_htbr(optionset.get_htb_rate(nbr_maturity))
    print(option.id_instrument(),iv)

k = list(t_qupte['amt_strike'])
iv_call = list(t_qupte['amt_iv_call'])
iv_put = list(t_qupte['amt_iv_put'])
iv_adj_call = list(t_qupte['amt_iv_adj_call'])
iv_adj_put = list(t_qupte['amt_iv_adj_put'])
iv_adj_call_vw = list(t_qupte['amt_iv_adj_call_vw'])
iv_adj_put_vw = list(t_qupte['amt_iv_adj_put_vw'])
iv_adj_call_pk = list(t_qupte['amt_iv_adj_call_pk'])
iv_adj_put_pk = list(t_qupte['amt_iv_adj_put_pk'])
iv_adj_call_mk = list(t_qupte['amt_iv_adj_call_mk'])
iv_adj_put_mk = list(t_qupte['amt_iv_adj_put_mk'])
iv_adj_call_atm = list(t_qupte['amt_iv_adj_call_atm'])
iv_adj_put_atm = list(t_qupte['amt_iv_adj_put_atm'])
implied_ivs = list(t_qupte[c.Util.AMT_HTB_RATE])
# plt.figure()
pu.plot_line_chart(k,[iv_call,iv_put],['iv_call','iv_put'])
pu.plot_line_chart(k,[iv_adj_call_atm,iv_adj_put_atm],['iv_adj_call_atm','iv_adj_put_atm'])
pu.plot_line_chart(k,[implied_ivs],['implied_ivs'])

plt.show()