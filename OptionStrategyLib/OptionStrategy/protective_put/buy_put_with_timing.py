from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from back_test.model.base_instrument import BaseInstrument
from back_test.model.base_option import BaseOption
from Utilities.timebase import LLKSR, KALMAN, LLT
from data_access import get_data
import back_test.model.constant as c
import datetime
import numpy as np
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
import math


def buy_put(moneyness, maturity1):
    list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness, maturity1)
    if list_atm_put is None:
        print('Given moneyness not available, choose min strike')
        list_atm_put = optionset.get_deepest_otm_put_list(maturity1)
    atm_put = optionset.select_higher_volume(list_atm_put)
    unit = unit_underlying * underlying.multiplier() / atm_put.multiplier()  # 等面值（标的数量）
    order = account.create_trade_order(atm_put, c.LongShort.LONG, unit)
    record = atm_put.execute_order(order, slippage=slippage)
    account.add_record(record, atm_put)
    premium = record[c.Util.TRADE_BOOK_VALUE]
    return atm_put, premium


def filtration_LLT(df, name_column):
    df['LLT_20'] = LLT(df[name_column], 20)
    df['LLT_10'] = LLT(df[name_column], 10)
    df['LLT_5'] = LLT(df[name_column], 5)
    df['LLT_3'] = LLT(df[name_column], 3)
    df['diff_20'] = df['LLT_20'].diff()
    df['diff_10'] = df['LLT_10'].diff()
    df['diff_5'] = df['LLT_5'].diff()
    df['diff_3'] = df['LLT_3'].diff()
    df['last_diff_20'] = df['diff_20'].shift()
    df = df.set_index(c.Util.DT_DATE)
    return df


def reverse(dt_date, df_status):
    if df_status.loc[dt_date, 'last_diff_20'] >= 0 and df_status.loc[dt_date, 'diff_20'] < 0:
        return True
    else:
        return False


def upward_tangent(dt_date, df_status):
    if df_status.loc[dt_date, 'diff_20'] > 0:
        return True
    else:
        return False


def downward_tangent(dt_date, df_status):
    if df_status.loc[dt_date, 'diff_20'] <= 0:
        return True
    else:
        return False


""" Settings and Collect Data """
##########
# start_date = datetime.date(2015, 2, 1)
start_date = datetime.date(2017, 1, 1)
end_date = datetime.date(2018, 8, 31)
nbr_maturity = 1
moneyness = -5

############
dt_close_position = end_date - datetime.timedelta(days=5)
min_holding = 15
slippage = 0
pct_underlying_invest = 1.0
pu = PlotUtil()

df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_underlying = get_data.get_index_mktdata(start_date, end_date, c.Util.STR_INDEX_50ETF)
df_underlying_llt = filtration_LLT(df_underlying, c.Util.AMT_CLOSE)

df_iv = get_data.get_iv_by_moneyness(start_date, end_date, c.Util.STR_50ETF)
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE] == c.Util.STR_PUT].dropna().reset_index(drop=True)
df_iv_llt = filtration_LLT(df_iv_put, c.Util.PCT_IMPLIED_VOL)

# plt.show()
""" Init Portfolio and Account """
init_fund = 10000000
optionset = BaseOptionSet(df_metrics)
optionset.init()
underlying = BaseInstrument(df_underlying)
underlying.init()
account = BaseAccount(init_fund, leverage=1.0, rf=0.03)
total_premium = 0.0
hedged = False

""" 初始开仓：基准指数 """
unit_underlying = np.floor(pct_underlying_invest * account.cash / underlying.mktprice_close() / underlying.multiplier())
order_underlying = account.create_trade_order(underlying, c.LongShort.LONG, unit_underlying)
record_underlying = underlying.execute_order(order_underlying, slippage=slippage)
account.add_record(record_underlying, underlying)
print(unit_underlying)
while optionset.has_next():
    """ 最终平仓 """
    if optionset.eval_date >= dt_close_position:
        print('4. Close out.')
        close_out_orders = account.creat_close_out_order()
        for order in close_out_orders:
            execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                       execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
            account.add_record(execution_record, account.dict_holding[order.id_instrument])
        account.daily_accounting(optionset.eval_date)
        break

    """ 期权到期展仓 """
    if hedged and optionset.eval_date > maturity - datetime.timedelta(days=30):
        print('3. 到期平仓', optionset.eval_date)
        order = account.create_close_order(atm_put)
        execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=slippage,
                                                                                   execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
        account.add_record(execution_record, account.dict_holding[order.id_instrument])
        total_premium += execution_record[c.Util.TRADE_BOOK_VALUE]
        hedged = False
        # maturity = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
        # atm_put, premium = buy_put(moneyness, maturity)
        # total_premium += premium

    # """ 平仓认沽期权 """
    # if hedged:
    #     if downward_tangent(optionset.eval_date, df_iv_llt):
    #         print('2. close option', optionset.eval_date)
    #         order = account.create_close_order(atm_put)
    #         execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=slippage,
    #                                                                                    execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
    #         account.add_record(execution_record, account.dict_holding[order.id_instrument])
    #         total_premium += execution_record[c.Util.TRADE_BOOK_VALUE]
    #         hedged = False
    #
    """ 买入认沽期权 """
    # if not hedged and upward_tangent(optionset.eval_date, df_iv_llt):
    if not hedged:
        print('1. buy option', optionset.eval_date)
        maturity = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
        atm_put, premium = buy_put(moneyness, maturity)
        total_premium += premium
        hedged = True

    account.daily_accounting(optionset.eval_date)
    print(optionset.eval_date,account.account.loc[optionset.eval_date,c.Util.PORTFOLIO_NPV])
    optionset.next()
    underlying.next()

df_records = pd.DataFrame(account.list_records)
# df_records.to_csv('df_records.csv')
analysis = account.get_netvalue_analysis(account.account[c.Util.PORTFOLIO_NPV])
analysis['total_premium'] = total_premium
print(analysis)
dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])

df_underlying.loc[:, 'npv_50etf'] = df_underlying.loc[:, c.Util.AMT_CLOSE] / \
                                               df_underlying.loc[0, c.Util.AMT_CLOSE]
analysis_50ETF = account.get_netvalue_analysis(df_underlying['npv_50etf'])
pu.plot_line_chart(dates, [npv], ['npv'])
pu.plot_line_chart(list(df_underlying[c.Util.DT_DATE]), [list(df_underlying['npv_50etf'])], ['npv base'])
plt.show()
