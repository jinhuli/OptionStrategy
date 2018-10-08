from data_access import get_data
from back_test.model.base_option_set import BaseOptionSet
from back_test.model.constant import Util, OptionUtil
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import datetime
import math
import pandas as pd


def get_atm_options(optionset, maturity):
    list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=maturity)
    atm_call = optionset.select_higher_volume(list_atm_call)
    atm_put = optionset.select_higher_volume(list_atm_put)
    return atm_call, atm_put

def get_atm_iv_average(optionset, maturity):
    atm_call, atm_put = get_atm_options(optionset,maturity)
    iv_call = atm_call.get_implied_vol()
    iv_put = atm_put.get_implied_vol()
    iv_avg = (iv_call + iv_put) / 2
    return iv_avg

pu = PlotUtil()
start_date = datetime.date(2018,9,12)
end_date = datetime.date(2018,9,13)
nbr_maturity = 0
min_holding = 8
df_daily = get_data.get_comoption_mktdata(start_date, end_date,Util.STR_SR)
optionset = BaseOptionSet(df_data=df_daily,rf=0.015)
optionset.init()
# optionset.go_to(end_date)

maturity = optionset.select_maturity_date(nbr_maturity, min_holding=min_holding)
iv_htr = optionset.get_atm_iv_by_htbr(maturity)
iv_avg = get_atm_iv_average(optionset, maturity)

htbr = optionset.get_htb_rate(maturity)
print('iv_htr : ', iv_htr)
print('iv_avg : ', iv_avg)
print('htb rate : ', htbr)

# curve = optionset.get_implied_vol_curves(maturity)

# curve_htbr = optionset.get_implied_vol_curves_htbr(maturity)
curve_otm = optionset.get_otm_implied_vol_curve(maturity)
# curve_htbr[Util.PCT_IV_CALL_BY_HTBR] = curve_htbr[Util.PCT_IV_CALL_BY_HTBR].apply(lambda x: None if x<0.05 else x)
# curve_htbr[Util.PCT_IV_PUT_BY_HTBR] = curve_htbr[Util.PCT_IV_PUT_BY_HTBR].apply(lambda x: None if x<0.05 else x)
curve_otm[Util.PCT_IV_OTM_BY_HTBR] = curve_otm[Util.PCT_IV_OTM_BY_HTBR].apply(lambda x: None if x<0.05 else x)

strikes = curve_otm[Util.AMT_APPLICABLE_STRIKE]
# pu.plot_line_chart(strikes,[list(curve[Util.PCT_IV_CALL]),list(curve[Util.PCT_IV_PUT])],['CALL IV','PUT iv'])
# pu.plot_line_chart(strikes,[list(curve_htbr[Util.PCT_IV_CALL_BY_HTBR]),list(curve_htbr[Util.PCT_IV_PUT_BY_HTBR])],['CALL IV adjusted','PUT IV adjusted'])
pu.plot_line_chart(strikes,[list(curve_otm[Util.PCT_IV_OTM_BY_HTBR])],['IV : '+str(optionset.eval_date)])
ivs1 = list(curve_otm[Util.PCT_IV_OTM_BY_HTBR])
optionset.next()
maturity = optionset.select_maturity_date(nbr_maturity, min_holding=min_holding)
iv_htr = optionset.get_atm_iv_by_htbr(maturity)
iv_avg = get_atm_iv_average(optionset, maturity)

htbr = optionset.get_htb_rate(maturity)
print('iv_htr : ', iv_htr)
print('iv_avg : ', iv_avg)
print('htb rate : ', htbr)

# curve = optionset.get_implied_vol_curves(maturity)

# curve_htbr = optionset.get_implied_vol_curves_htbr(maturity)
curve_otm = optionset.get_otm_implied_vol_curve(maturity)
# curve_htbr[Util.PCT_IV_CALL_BY_HTBR] = curve_htbr[Util.PCT_IV_CALL_BY_HTBR].apply(lambda x: None if x<0.05 else x)
# curve_htbr[Util.PCT_IV_PUT_BY_HTBR] = curve_htbr[Util.PCT_IV_PUT_BY_HTBR].apply(lambda x: None if x<0.05 else x)
curve_otm[Util.PCT_IV_OTM_BY_HTBR] = curve_otm[Util.PCT_IV_OTM_BY_HTBR].apply(lambda x: None if x<0.05 else x)
ivs2 = list(curve_otm[Util.PCT_IV_OTM_BY_HTBR])

strikes = curve_otm[Util.AMT_APPLICABLE_STRIKE]
# pu.plot_line_chart(strikes,[list(curve[Util.PCT_IV_CALL]),list(curve[Util.PCT_IV_PUT])],['CALL IV','PUT iv'])
# pu.plot_line_chart(strikes,[list(curve_htbr[Util.PCT_IV_CALL_BY_HTBR]),list(curve_htbr[Util.PCT_IV_PUT_BY_HTBR])],['CALL IV adjusted','PUT IV adjusted'])
pu.plot_line_chart(strikes,[list(curve_otm[Util.PCT_IV_OTM_BY_HTBR])],['IV : '+str(optionset.eval_date)])
pu.plot_line_chart(strikes,[ivs2,ivs1],['IV : '+str(optionset.eval_date),'IV : last day'])

plt.show()









