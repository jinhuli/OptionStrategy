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
min_holding = 1  # 20 sharpe ratio较优
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


account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)

option_trade_times = 0
empty_position = True
unit = None
atm_strike = None
butterfly = None
# buy_write = c.BuyWrite.WRITE
maturity1 = optionset.select_maturity_date(nbr_maturity=1, min_holding=min_holding)

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
    # if not empty_position and (maturity1 - optionset.eval_date).days <= 8:
    if not empty_position and (maturity1 - optionset.eval_date).days <= 0:
        for option in account.dict_holding.values():
            order = account.create_close_order(option, cd_trade_price=cd_trade_price)
            record = option.execute_order(order, slippage=slippage)
            account.add_record(record, option)
        empty_position = True
    # 开仓：距到期1M
    # if empty_position and (maturity1 - optionset.eval_date).days <= 30:
    if empty_position :
        option_trade_times += 1
        maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)
        atm_calls, atm_puts = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=maturity1)
        atm_call = optionset.select_higher_volume(atm_calls)
        k_low1 = round(atm_call.applicable_strike() - 0.1,2)
        k_low2 = round(atm_call.applicable_strike() - 0.2,2)
        k_high1 = round(atm_call.applicable_strike() + 0.1,2)
        k_high2 = round(atm_call.applicable_strike() + 0.2,2)
        options_high1 = optionset.get_option_by_strike(c.OptionType.CALL,k_high1,maturity1)
        options_high2 = optionset.get_option_by_strike(c.OptionType.CALL,k_high2,maturity1)
        options_low2 = optionset.get_option_by_strike(c.OptionType.CALL,k_low2,maturity1)
        options_low1 = optionset.get_option_by_strike(c.OptionType.CALL,k_low1,maturity1)
        option_high1 = optionset.select_higher_volume(options_high1)
        option_high2 = optionset.select_higher_volume(options_high2)
        option_low1 = optionset.select_higher_volume(options_low1)
        option_low2 = optionset.select_higher_volume(options_low2)
        if option_high2 is None or option_low2 is None:
            if not optionset.has_next(): break
            optionset.next()
            continue
        x =(option_high2.mktprice_close() + option_low2.mktprice_close() - option_high1.mktprice_close()- option_low1.mktprice_close())*10000 # 成本
        print(optionset.eval_date, x)
        if x >=1000:
            if not optionset.has_next(): break
            optionset.next()
            continue
        atm_strike = atm_call.strike()
        spot = atm_call.underlying_close()
        unit = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_call.multiplier() * m)
        # if butterfly == 1:
        order_1 = account.create_trade_order(option_high1, c.LongShort.SHORT, unit, cd_trade_price=cd_trade_price)
        order_2 = account.create_trade_order(option_low1, c.LongShort.SHORT, unit, cd_trade_price=cd_trade_price)
        order_3 = account.create_trade_order(option_low2, c.LongShort.LONG, unit, cd_trade_price=cd_trade_price)
        order_4 = account.create_trade_order(option_high2, c.LongShort.LONG, unit, cd_trade_price=cd_trade_price)
        # elif butterfly == -1:
        #     order_1 = account.create_trade_order(atm_call, c.LongShort.LONG, unit*2, cd_trade_price=cd_trade_price)
        #     order_2 = account.create_trade_order(itm_call, c.LongShort.SHORT, unit, cd_trade_price=cd_trade_price)
        #     order_3 = account.create_trade_order(otm_call, c.LongShort.SHORT, unit, cd_trade_price=cd_trade_price)
        # else:
        #     if not optionset.has_next(): break
        #     optionset.next()
        #     continue
        record_1 = atm_call.execute_order(order_1, slippage=slippage)
        record_2 = atm_call.execute_order(order_2, slippage=slippage)
        record_3 = atm_call.execute_order(order_3, slippage=slippage)
        record_4 = atm_call.execute_order(order_4, slippage=slippage)
        account.add_record(record_1, option_high1)
        account.add_record(record_2, option_low1)
        account.add_record(record_3, option_low2)
        account.add_record(record_4, option_high2)
        empty_position = False

    account.daily_accounting(optionset.eval_date)
    total_liquid_asset = account.cash + account.get_portfolio_margin_capital()
    if not optionset.has_next(): break
    optionset.next()

account.account.to_csv('../../accounts_data/butterfly_account.csv')
account.trade_records.to_csv('../../accounts_data/butterfly_records.csv')
account.trade_book_daily.to_csv('../../accounts_data/butterfly_book.csv')
res = account.analysis()
res['期权平均持仓天数'] = len(account.account) / option_trade_times
print(res)

dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu.plot_line_chart(dates,[npv],['npv'])
# plt.show()




