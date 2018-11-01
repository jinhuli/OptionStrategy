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
start_date = datetime.date(2015, 1, 21)
end_date = datetime.date.today()
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 15  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
m = 0.5  # 期权notional倍数
# moneyness_rank = -2
cd_trade_price = c.CdTradePrice.CLOSE

""" 50ETF option """
# name_code = c.Util.STR_IH
# name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
# df_metrics = get_data.get_comoption_mktdata(start_date, end_date,c.Util.STR_M)

df_holding_period = pd.DataFrame()

optionset = BaseOptionSet(df_metrics)
optionset.init()
d1 = optionset.eval_date


# account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)

# option_trade_times = 0
# empty_position = True
# unit = None
atm_strike = None
# butterfly = None
# buy_write = c.BuyWrite.WRITE
dates = []
costs_call = []
costs_put = []
while optionset.eval_date <= end_date:
    maturity1 = optionset.select_maturity_date(nbr_maturity=1, min_holding=min_holding)
    atm_calls, atm_puts = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=maturity1)
    atm_call = optionset.select_higher_volume(atm_calls)
    k_low1 = round(atm_call.applicable_strike() - 0.1,2)
    k_low2 = round(atm_call.applicable_strike() - 0.2,2)
    k_high1 = round(atm_call.applicable_strike() + 0.1,2)
    k_high2 = round(atm_call.applicable_strike() + 0.2,2)

    call_high1 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.CALL,k_high1,maturity1))
    call_high2 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.CALL,k_high2,maturity1))
    call_low1 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.CALL,k_low1,maturity1))
    call_low2 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.CALL,k_low2,maturity1))

    put_high1 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.PUT,k_high1,maturity1))
    put_high2 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.PUT,k_high2,maturity1))
    put_low1 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.PUT,k_low1,maturity1))
    put_low2 = optionset.select_higher_volume(optionset.get_option_by_strike(c.OptionType.PUT,k_low2,maturity1))
    if put_high2 is None or put_low2 is None:
        if not optionset.has_next(): break
        optionset.next()
        continue
    x_call =(call_high2.mktprice_close() + call_low2.mktprice_close() - call_high1.mktprice_close()- call_low1.mktprice_close())*10000 # 成本
    x_put =(put_high2.mktprice_close() + put_low2.mktprice_close() - put_high1.mktprice_close()- put_low1.mktprice_close())*10000 # 成本
    print(optionset.eval_date, x_call)
    dates.append(optionset.eval_date)
    costs_call.append(x_call)
    costs_put.append(x_put)
    if not optionset.has_next(): break
    optionset.next()

pu.plot_line_chart(dates,[costs_call,costs_put],['cost call','cost put'])

plt.show()


