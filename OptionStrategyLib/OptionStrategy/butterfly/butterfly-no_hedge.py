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
start_date = datetime.date(2018, 9, 21)
end_date = datetime.date.today()
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 15  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
m = 1  # 期权notional倍数
# moneyness_rank = -2
cd_trade_price = c.CdTradePrice.CLOSE

""" 50ETF option """
# name_code = c.Util.STR_IH
# name_code_option = c.Util.STR_50ETF
# df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_metrics = get_data.get_comoption_mktdata(start_date, end_date,c.Util.STR_CU)

df_holding_period = pd.DataFrame()

optionset = BaseOptionSet(df_metrics)
optionset.init()
d1 = optionset.eval_date


account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)

option_trade_times = 0
empty_position = True
unit = None
atm_strike = None
buy_write = c.BuyWrite.WRITE
maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)

while optionset.eval_date <= end_date:
    # print(optionset.eval_date)
    if account.cash <= 0: break
    if not optionset.has_next():  # Final close out all.
        close_out_orders = account.creat_close_out_order()
        for order in close_out_orders:
            execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                       execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
            account.add_record(execution_record, account.dict_holding[order.id_instrument])
        account.daily_accounting(optionset.eval_date)
        break

    # 平仓
    if not empty_position and (maturity1 - optionset.eval_date).days <= 8:
        for option in account.dict_holding.values():
            order = account.create_close_order(option, cd_trade_price=cd_trade_price)
            record = option.execute_order(order, slippage=slippage)
            account.add_record(record, option)
        empty_position = True
        maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)

    # 开仓：距到期1M
    # if empty_position and (maturity1 - optionset.eval_date).days <= 30:
    if empty_position :
        option_trade_times += 1
        # buy_write = c.BuyWrite.WRITE
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0,
                                                                                    maturity=maturity1)
        atm_call = optionset.select_higher_volume(list_atm_call)
        list_itm_call, list_itm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=2,
                                                                                    maturity=maturity1)
        itm_call = optionset.select_higher_volume(list_itm_call)
        list_otm_call, list_otm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=-2,
                                                                                    maturity=maturity1)
        otm_call = optionset.select_higher_volume(list_otm_call)
        if itm_call is None or otm_call is None:
            if not optionset.has_next(): break
            optionset.next()
            continue
        atm_strike = atm_call.strike()
        spot = atm_call.underlying_close()
        unit = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_call.multiplier()) * m
        order_1 = account.create_trade_order(atm_call, c.LongShort.SHORT, unit*2, cd_trade_price=cd_trade_price)
        order_2 = account.create_trade_order(itm_call, c.LongShort.LONG, unit, cd_trade_price=cd_trade_price)
        order_3 = account.create_trade_order(otm_call, c.LongShort.LONG, unit, cd_trade_price=cd_trade_price)
        record_1 = atm_call.execute_order(order_1, slippage=slippage)
        record_2 = atm_call.execute_order(order_2, slippage=slippage)
        record_3 = atm_call.execute_order(order_3, slippage=slippage)
        account.add_record(record_1, atm_call)
        account.add_record(record_2, itm_call)
        account.add_record(record_3, otm_call)
        empty_position = False

    account.daily_accounting(optionset.eval_date)
    total_liquid_asset = account.cash + account.get_portfolio_margin_capital()
    if not optionset.has_next(): break
    optionset.next()

account.account.to_csv('../../accounts_data/butterfly_account.csv')
res = account.analysis()
res['期权平均持仓天数'] = len(account.account) / option_trade_times
print(res)

dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu.plot_line_chart(dates,[npv],['npv'])
plt.show()




