from data_access.get_data import get_50option_mktdata as option_data, get_50option_intraday as intraday_data
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
start_date = datetime.date(2018,7,25)
end_date = datetime.date(2018,7,25)
nbr_maturity = 0
min_holding = 8
df_daily = option_data(start_date, end_date)
optionset = BaseOptionSet(df_data=df_daily,rf=0.03)
optionset.init()
# optionset.go_to(end_date)

maturity = optionset.select_maturity_date(nbr_maturity, min_holding=min_holding)
iv_htr = optionset.get_atm_iv_by_htbr(maturity)
iv_avg = get_atm_iv_average(optionset, maturity)
htbr = optionset.get_htb_rate(maturity)
print('iv_htr : ', iv_htr)
print('iv_avg : ', iv_avg)
print('htb rate : ', htbr)

curve = optionset.get_implied_vol_curves(maturity)

curve_htbr = optionset.get_implied_vol_curves_htbr(maturity)

strikes = curve[Util.AMT_APPLICABLE_STRIKE]
pu.plot_line_chart(strikes,[list(curve[Util.PCT_IV_CALL]),list(curve[Util.PCT_IV_PUT])],['50ETF 1807 IV','50ETF 1807 iv'])
pu.plot_line_chart(strikes,[list(curve_htbr[Util.PCT_IV_CALL_BY_HTBR]),list(curve_htbr[Util.PCT_IV_PUT_BY_HTBR])],['50ETF 1807 IV adjusted','50ETF 1807 IV adjusted'])


plt.show()









