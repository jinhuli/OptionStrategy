from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_option import BaseOption
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
start_date = datetime.date(2018, 8, 1)
end_date = datetime.date(2018, 10, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 15  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
m = 1  # 期权notional倍数
cd_trade_price = c.CdTradePrice.CLOSE
d_critirian = 0.03

""" Data: 50ETF option/Index/IH """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_intraday(start_date, end_date)
df_metrics_daily = get_data.get_50option_mktdata(start_date, end_date)
df_index = get_data.get_index_intraday(start_date, end_date, c.Util.STR_INDEX_50ETF)
df_index_daily = get_data.get_index_mktdata(start_date, end_date, c.Util.STR_INDEX_50ETF)
# df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)
# df_futures_all_daily = get_data.get_mktdata_future_daily(start_date, end_date,
#                                                          name_code)  # daily data of all future contracts
# d1 = df_future_c1_daily[c.Util.DT_DATE].values[0]
d1 = df_index[c.Util.DT_DATE].values[0]
d2 = df_metrics[c.Util.DT_DATE].values[0]
d = max(d1, d2)
df_metrics = df_metrics[df_metrics[c.Util.DT_DATE] >= d].reset_index(drop=True)
df_index = df_index[df_index[c.Util.DT_DATE] >= d].reset_index(drop=True)


# df_c1 = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)
# df_c_all = df_futures_all_daily[df_futures_all_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)

def next(optionset, hedging):
    optionset.next()
    hedging.next()
    if optionset.eval_datetime != hedging.eval_datetime:
        print(optionset.eval_datetime, hedging.eval_datetime)


""" Volatility Strategy: Straddle """
optionset = BaseOptionSet(df_metrics, df_daily_data=df_metrics_daily, frequency=c.FrequentType.MINUTE)
optionset.init()
d1 = optionset.eval_date
hedging = SytheticOption(df_index, frequency=c.FrequentType.MINUTE, df_c1_daily=df_index_daily)
hedging.init()
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
while optionset.has_next():
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

    # 平仓：距到期8日
    if not empty_position and (maturity1 - optionset.eval_date).days <= 8:
        print('close position',optionset.eval_datetime,hedging.eval_datetime)
        for instrument in account.dict_holding.values():
            instrument.calulate_volume_weighted_price_start()
        while optionset.has_next_minute():
            for instrument in account.dict_holding.values():
                instrument.calulate_volume_weighted_price()
            next(optionset, hedging)
        for instrument in account.dict_holding.values():
            order = account.create_close_order(instrument, cd_trade_price=cd_trade_price)
            order.trade_price = instrument.calulate_volume_weighted_price_stop()
            record = instrument.execute_order(order, slippage=slippage)
            account.add_record(record, instrument)
        hedging.synthetic_unit = 0
        last_delta = 0
        empty_position = True
        maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)

    # 开仓：距到期1M
    if empty_position and (maturity1 - optionset.eval_date).days <= 30:
        print('open position',optionset.eval_datetime,hedging.eval_datetime)
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
        # 开仓当日，用开盘价对应的delta建hedge头寸
        iv_htbr = optionset.get_iv_by_otm_iv_curve(dt_maturity=maturity1, strike=atm_call.applicable_strike())
        delta_call = atm_call.get_delta(iv_htbr)
        delta_put = atm_put.get_delta(iv_htbr)
        options_delta = unit_c * atm_call.multiplier() * delta_call + unit_p * atm_put.multiplier() * delta_put
        last_delta = delta = delta_call + delta_put
        hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta, buy_write)
        hedging.synthetic_unit += - hedge_unit
        if hedge_unit > 0:
            long_short = c.LongShort.LONG
        else:
            long_short = c.LongShort.SHORT
        order_hedge = account.create_trade_order(hedging, long_short, hedge_unit, cd_trade_price=cd_trade_price)
        atm_call.calulate_volume_weighted_price_start()
        atm_put.calulate_volume_weighted_price_start()
        hedging.calulate_volume_weighted_price_start()
        while optionset.has_next_minute():
            atm_call.calulate_volume_weighted_price()
            atm_put.calulate_volume_weighted_price()
            hedging.calulate_volume_weighted_price()
            next(optionset, hedging)
        order_c.trade_price = atm_call.calulate_volume_weighted_price_stop()
        order_p.trade_price = atm_put.calulate_volume_weighted_price_stop()
        order_hedge.trade_price = hedging.calulate_volume_weighted_price_stop()
        record_call = atm_call.execute_order(order_c, slippage=slippage)
        record_put = atm_put.execute_order(order_p, slippage=slippage)
        record_hedge = hedging.execute_order(order_hedge, slippage=slippage)
        account.add_record(record_call, atm_call)
        account.add_record(record_put, atm_put)
        account.add_record(record_hedge, hedging)
        empty_position = False

    # Delta hedge
    if not empty_position:
        if not optionset.has_next_minute():  # 开仓当日，以期权盘价delta调整对冲头寸
            iv_htbr = optionset.get_iv_by_otm_iv_curve(dt_maturity=maturity1, strike=atm_call.applicable_strike())
            delta_call = atm_call.get_delta(iv_htbr)
            delta_put = atm_put.get_delta(iv_htbr)
            options_delta = unit_c * atm_call.multiplier() * delta_call + unit_p * atm_put.multiplier() * delta_put
            delta = delta_call + delta_put
            if abs(delta - last_delta) > d_critirian:
                last_delta = delta
                hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta, buy_write)
                hedging.synthetic_unit += - hedge_unit
                if hedge_unit > 0:
                    long_short = c.LongShort.LONG
                else:
                    long_short = c.LongShort.SHORT
                order_u = account.create_trade_order(hedging, long_short, hedge_unit, cd_trade_price=cd_trade_price)
                record_u = hedging.execute_order(order_u, slippage=slippage)
                account.add_record(record_u, hedging)
        else:  # 非当日开仓的情况下，按分钟数据下的delta敞口调整对冲头寸
            while optionset.has_next_minute():
                iv_htbr = optionset.get_iv_by_otm_iv_curve(dt_maturity=maturity1, strike=atm_call.applicable_strike())
                if iv_htbr == 0.0 or iv_htbr is None: continue
                delta_call = atm_call.get_delta(iv_htbr)
                delta_put = atm_put.get_delta(iv_htbr)
                options_delta = unit_c * atm_call.multiplier() * delta_call + unit_p * atm_put.multiplier() * delta_put
                delta = delta_call + delta_put
                if abs(delta - last_delta) > d_critirian:
                    last_delta = delta
                    hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta, buy_write)
                    hedging.synthetic_unit += - hedge_unit
                    if hedge_unit > 0:
                        long_short = c.LongShort.LONG
                    else:
                        long_short = c.LongShort.SHORT
                    order_u = account.create_trade_order(hedging, long_short, hedge_unit, cd_trade_price=cd_trade_price)
                    record_u = hedging.execute_order(order_u, slippage=slippage)
                    account.add_record(record_u, hedging)
                next(optionset, hedging)
    account.daily_accounting(optionset.eval_date)
    total_liquid_asset = account.cash + account.get_portfolio_margin_capital()
    if not optionset.has_next(): break
    optionset.next()
    hedging.next()

# account.account.to_csv('../../accounts_data/short_straddle_account-hedged-by-index.csv')
# account.trade_records.to_csv('../../accounts_data/short_straddle_records-hedged-by-index.csv')
# account.trade_book_daily.to_csv('../../accounts_data/short_straddle_book-hedged-by-index.csv')
res = account.analysis()
res['期权平均持仓天数'] = len(account.account) / option_trade_times
print(res)

dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu.plot_line_chart(dates, [npv], ['npv'])

plt.show()
