from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
import data_access.get_data as get_data
import back_test.model.constant as c
import datetime
import numpy as np
from OptionStrategyLib.OptionReplication.synthetic_option import SytheticOption
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
from Utilities.timebase import LLKSR, KALMAN, LLT
from back_test.model.trade import Order

pu = PlotUtil()
start_date = datetime.date(2015, 2, 1)
end_date = datetime.date.today()
dt_histvol = start_date - datetime.timedelta(days=90)
rf = 0.03
min_holding = 20
init_fund = c.Util.BILLION
slippage = 0
m = 1  # 期权notional倍数
cd_trade_price = c.CdTradePrice.CLOSE

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
# df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)
# df_futures_all_daily = get_data.get_mktdata_future_daily(start_date, end_date,
#                                                          name_code)  # daily data of all future contracts
# """ Volatility Strategy: Straddle """
# d1 = df_future_c1_daily[c.Util.DT_DATE].values[0]
# d2 = df_metrics[c.Util.DT_DATE].values[0]
# d = max(d1, d2)
# df_metrics = df_metrics[df_metrics[c.Util.DT_DATE] >= d].reset_index(drop=True)
# df_c1 = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)
# df_c_all = df_futures_all_daily[df_futures_all_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)
#
# df_holding_period = pd.DataFrame()

optionset = BaseOptionSet(df_metrics)
optionset.init()
d1 = optionset.eval_date
res = []
while optionset.has_next():
    dt_maturity = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)
    t_qupte = optionset.get_T_quotes(dt_maturity, cd_trade_price)
    t_qupte.loc[:, 'diff'] = abs(
        t_qupte.loc[:, c.Util.AMT_APPLICABLE_STRIKE] - t_qupte.loc[:, c.Util.AMT_UNDERLYING_CLOSE])
    atm_series = t_qupte.loc[t_qupte['diff'].idxmin()]
    amt_call = atm_series[c.Util.AMT_CALL_QUOTE]
    amt_put = atm_series[c.Util.AMT_PUT_QUOTE]
    strike = atm_series[c.Util.AMT_STRIKE]
    spot = atm_series[c.Util.AMT_UNDERLYING_CLOSE]
    pcp_rate = (spot + amt_put - amt_call - strike * c.PricingUtil.get_discount(optionset.eval_date, dt_maturity,
                                                                                rf)) / spot
    ttm = c.PricingUtil.get_ttm(optionset.eval_date,dt_maturity)
    pcp_annual_rate = pcp_rate/ttm
    res.append({'dt_date': optionset.eval_date, 'pcp_rate': pcp_rate, 'pcp_annual_rate': pcp_annual_rate})
    optionset.next()

df_res = pd.DataFrame(res)
print(df_res)
df_res.to_csv('../../accounts_data/put_call_parity.csv')
