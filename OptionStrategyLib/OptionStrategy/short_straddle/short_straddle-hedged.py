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
end_date = datetime.date(2018, 10, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 15  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
m = 1  # 期权notional倍数
cd_trade_price = c.CdTradePrice.VOLUME_WEIGHTED
cd_hedge_price = c.CdTradePrice.CLOSE
d_critirian = 0.1

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)
df_futures_all_daily = get_data.get_mktdata_future_daily(start_date, end_date,
                                                         name_code)  # daily data of all future contracts

""" Volatility Strategy: Straddle """
d1 = df_future_c1_daily[c.Util.DT_DATE].values[0]
d2 = df_metrics[c.Util.DT_DATE].values[0]
d = max(d1, d2)
df_metrics = df_metrics[df_metrics[c.Util.DT_DATE] >= d].reset_index(drop=True)
df_c1 = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)
df_c_all = df_futures_all_daily[df_futures_all_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)

df_holding_period = pd.DataFrame()

optionset = BaseOptionSet(df_metrics)
optionset.init()
d1 = optionset.eval_date

hedging = SytheticOption(df_c1, frequency=c.FrequentType.DAILY, df_c1_daily=df_c1, df_futures_all_daily=df_c_all)
hedging.init()
hedging.amt_option = 1 / 1000  # 50ETF与IH点数之比

print(optionset.eval_date, hedging.eval_date)

account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)

option_trade_times = 0
empty_position = True
unit_p = None
unit_c = None
atm_strike = None
buy_write = c.BuyWrite.WRITE
maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
id_future = hedging.current_state[c.Util.ID_FUTURE]
idx_hedge = 0
flag_hedge = False
last_delta = 0
print(id_future)
while optionset.eval_date <= end_date:
    if account.cash <= 0: break
    if maturity1 > end_date:  # Final close out all.
        close_out_orders = account.creat_close_out_order()
        for order in close_out_orders:
            execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                       execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
            account.add_record(execution_record, account.dict_holding[order.id_instrument])

        account.daily_accounting(optionset.eval_date)
        print(optionset.eval_date, ' close out ')
        print(optionset.eval_date, hedging.eval_date,
              account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],
              int(account.cash))
        break

    # 标的移仓换月
    if id_future != hedging.current_state[c.Util.ID_FUTURE]:
        for holding in account.dict_holding.values():
            if isinstance(holding, SytheticOption):
                df = hedging.df_all_futures_daily[
                    (hedging.df_all_futures_daily[c.Util.DT_DATE] == hedging.eval_date) & (
                        hedging.df_all_futures_daily[c.Util.ID_FUTURE] == id_future)]
                trade_unit = account.trade_book.loc[hedging.name_code(), c.Util.TRADE_UNIT]
                if account.trade_book.loc[hedging.name_code(), c.Util.TRADE_LONG_SHORT] == c.LongShort.LONG:
                    long_short = c.LongShort.SHORT
                else:
                    long_short = c.LongShort.LONG
                trade_price = df[c.Util.AMT_TRADING_VALUE].values[0] / df[c.Util.AMT_TRADING_VOLUME].values[
                    0] / hedging.multiplier()
                order = Order(holding.eval_date, hedging.name_code(), trade_unit, trade_price,
                              holding.eval_datetime, long_short)
                record = hedging.execute_order(order, slippage=slippage)
                account.add_record(record, holding)
        hedging.synthetic_unit = 0
        last_delta = 0
        id_future = hedging.current_state[c.Util.ID_FUTURE]
        flag_hedge = True

    # 平仓：距到期8日
    if not empty_position and (maturity1 - optionset.eval_date).days <= 8:
        for option in account.dict_holding.values():
            order = account.create_close_order(option, cd_trade_price=cd_trade_price)
            record = option.execute_order(order, slippage=slippage)
            account.add_record(record, option)
            hedging.synthetic_unit = 0
            last_delta = 0
        empty_position = True
        maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)

    # 开仓：距到期1M
    if empty_position and (maturity1 - optionset.eval_date).days <= 30:
        option_trade_times += 1
        buy_write = c.BuyWrite.WRITE
        long_short = c.LongShort.SHORT
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0,
                                                                                    maturity=maturity1)
        atm_call = optionset.select_higher_volume(list_atm_call)
        atm_put = optionset.select_higher_volume(list_atm_put)
        atm_strike = atm_call.strike()
        spot = atm_call.underlying_close()
        unit_c = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_call.multiplier()) * m
        unit_p = np.floor(np.floor(account.portfolio_total_value / atm_put.strike()) / atm_put.multiplier()) * m
        order_c = account.create_trade_order(atm_call, long_short, unit_c, cd_trade_price=cd_trade_price)
        order_p = account.create_trade_order(atm_put, long_short, unit_p, cd_trade_price=cd_trade_price)
        record_call = atm_call.execute_order(order_c, slippage=slippage)
        record_put = atm_put.execute_order(order_p, slippage=slippage)
        account.add_record(record_call, atm_call)
        account.add_record(record_put, atm_put)
        empty_position = False

    # Delta hedge
    if not empty_position:
        iv_htbr = optionset.get_iv_by_otm_iv_curve(dt_maturity=maturity1, strike=atm_call.applicable_strike())
        delta_call = atm_call.get_delta(iv_htbr)
        delta_put = atm_put.get_delta(iv_htbr)
        options_delta = unit_c * atm_call.multiplier() * delta_call + unit_p * atm_put.multiplier() * delta_put
        delta = delta_call+delta_put
        if abs(delta - last_delta) > d_critirian:
            last_delta = delta
            hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta, buy_write)
            hedging.synthetic_unit += - hedge_unit
            if hedge_unit > 0:
                long_short = c.LongShort.LONG
            else:
                long_short = c.LongShort.SHORT
            order_u = account.create_trade_order(hedging, long_short, hedge_unit, cd_trade_price=cd_hedge_price)
            record_u = hedging.execute_order(order_u, slippage=slippage)
            account.add_record(record_u, hedging)

    idx_hedge += 1
    account.daily_accounting(optionset.eval_date)
    total_liquid_asset = account.cash + account.get_portfolio_margin_capital()
    if not optionset.has_next(): break
    optionset.next()
    hedging.next()

account.account.to_csv('../../accounts_data/short_straddle_account-hedged.csv')
account.trade_records.to_csv('../../accounts_data/short_straddle_records-hedged.csv')
account.trade_book_daily.to_csv('../../accounts_data/short_straddle_book-hedged.csv')
res = account.analysis()
res['期权平均持仓天数'] = len(account.account) / option_trade_times
print(res)

dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu.plot_line_chart(dates,[npv],['npv'])

# plt.show()
