from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from data_access import get_data
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
start_date = datetime.date(2016, 2, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 20  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0

""" 50ETF option """
# name_code = c.Util.STR_IH
# name_code_option = c.Util.STR_50ETF
# df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_metrics = get_data.get_comoption_mktdata(start_date,end_date,c.Util.STR_M)
optionset = BaseOptionSet(df_metrics)
optionset.init()
account = BaseAccount(init_fund=10000000, leverage=1.0, rf=0.03)

empty_position = True
unit_p = None
unit_c = None
atm_strike = None
buy_write = c.BuyWrite.WRITE
maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=20)

while optionset.eval_date <= end_date:
    if account.cash <= 0: break
    if maturity1 > end_date:  # Final close out all.
        close_out_orders = account.creat_close_out_order()
        for order in close_out_orders:
            execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                       execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
            account.add_record(execution_record, account.dict_holding[order.id_instrument])

        account.daily_accounting(optionset.eval_date)
        print(' close out ', optionset.eval_date,
              account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],
              int(account.cash))
        break

    # 展期
    if optionset.eval_date > maturity1 - datetime.timedelta(days=8):
        close_out_orders = account.creat_close_out_order()
        for order in close_out_orders:
            execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                       execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
            account.add_record(execution_record, account.dict_holding[order.id_instrument])
        empty_position = True

    # 开仓
    if empty_position:
        try:
            buy_write = c.BuyWrite.WRITE
            long_short = c.LongShort.SHORT
            maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=20)
            # 空两份平值
            list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0,
                                                                                        maturity=maturity1)
            atm_call = optionset.select_higher_volume(list_atm_put)
            # 多一份实值: 2
            list_itm_call, list_itm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=1,
                                                                                        maturity=maturity1)
            itm_call = optionset.select_higher_volume(list_itm_put)
            # 多一份虚值: -2
            list_otm_call, list_otm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=-1,
                                                                                        maturity=maturity1)
            otm_call = optionset.select_higher_volume(list_otm_put)
            # unit_fund = 2 * (atm_call.get_initial_margin(c.LongShort.SHORT) - atm_call.mktprice_last_settlement()*atm_call.multiplier()) \
            #             + itm_call.mktprice_close()*itm_call.multiplier() + otm_call.mktprice_close()*otm_call.multiplier()
            unit_fund = 2 * atm_call.get_initial_margin(
                c.LongShort.SHORT) + itm_call.mktprice_close() * itm_call.multiplier() + otm_call.mktprice_close() * otm_call.multiplier()
            total_unit = np.floor(account.cash / unit_fund/2)
            unit_atm = 2 * total_unit
            unit_itm = total_unit
            unit_otm = total_unit
            order_atm = account.create_trade_order(atm_call, c.LongShort.SHORT, unit_atm)
            order_itm = account.create_trade_order(itm_call, c.LongShort.LONG, unit_itm)
            order_otm = account.create_trade_order(otm_call, c.LongShort.LONG, unit_otm)
            record_atm = atm_call.execute_order(order_atm, slippage=slippage)
            record_itm = itm_call.execute_order(order_itm, slippage=slippage)
            record_otm = otm_call.execute_order(order_otm, slippage=slippage)
            account.add_record(record_atm, atm_call)
            account.add_record(record_itm, itm_call)
            account.add_record(record_otm, otm_call)
            empty_position = False
        except:
            empty_position = True
            pass

    account.daily_accounting(optionset.eval_date)
    if not optionset.has_next(): break
    optionset.next()

# account.account.to_csv('account.csv')
# df_records = pd.DataFrame(account.list_records)
# df_records.to_csv('df_records.csv')
res = account.get_netvalue_analysis(account.account[c.Util.PORTFOLIO_NPV])
print(res)
dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu.plot_line_chart(dates, [npv], ['npv'])

plt.show()
