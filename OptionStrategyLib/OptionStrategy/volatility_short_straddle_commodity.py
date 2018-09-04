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
from Utilities.timebase import LLKSR,KALMAN,LLT
from back_test.model.trade import Order

""" Open/Close Position Signal """
def open_signal(dt_date, df_status):
    return open_signal_tangent(dt_date, df_status)

def close_signal(dt_date,option_maturity, df_status):
    if dt_date >= option_maturity - datetime.timedelta(days=1):
        print('3.到期', dt_date)
        return True
    else:
        return close_signal_tangent(dt_date, df_status)

def open_signal_tangent(dt_date, df_status):
    if df_status.loc[dt_date,'diff_20'] <= 0 and df_status.loc[dt_date,'diff_10'] <= 0 and df_status.loc[dt_date,'diff_5'] <= 0:
    # if df_status.loc[dt_date,'diff_5'] <= 0:
        print('1.open', dt_date)
        return True
    else:
        return False

def close_signal_tangent(dt_date, df_status):
    if df_status.loc[dt_date,'diff_5'] > 0:
        print('2.close', dt_date)
        return True
    else:
        return False

pu = PlotUtil()
start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 20
init_fund = c.Util.BILLION
slippage = 0
m = 1 # 期权notional倍数

""" commodity option """
name_code = name_code_option = c.Util.STR_M
df_metrics = get_data.get_comoption_mktdata(start_date, end_date,name_code)
df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)
df_futures_all_daily = get_data.get_mktdata_cf_daily(start_date, end_date, name_code)  # daily data of all future contracts

""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(dt_histvol,end_date,name_code_option)
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
df_data = df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna().reset_index(drop=True)
df_data.loc[:,'average_iv'] = (df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put'])/2

""" Volatility Statistics """
df_iv_stats = df_data[[c.Util.DT_DATE, 'average_iv']]


""" 2. Filtration ：LLKSR """
# df_iv_stats['LLKSR_20'] = LLKSR(df_iv_stats['average_iv'], 20)
# df_iv_stats['LLKSR10'] = LLKSR(df_iv_stats['average_iv'], 10)
# df_iv_stats['LLKSR_5'] = LLKSR(df_iv_stats['average_iv'], 5)
# df_iv_stats['diff_20'] = df_iv_stats['LLKSR_20'].diff()
# df_iv_stats['diff_10'] = df_iv_stats['LLKSR10'].diff()
# df_iv_stats['diff_5'] = df_iv_stats['LLKSR_5'].diff()

""" Filtration : LLT """
df_iv_stats['LLT_20'] = LLT(df_iv_stats['average_iv'], 20)
df_iv_stats['LLT_10'] = LLT(df_iv_stats['average_iv'], 10)
df_iv_stats['LLT_5'] = LLT(df_iv_stats['average_iv'], 5)
df_iv_stats['diff_20'] = df_iv_stats['LLT_20'].diff()
df_iv_stats['diff_10'] = df_iv_stats['LLT_10'].diff()
df_iv_stats['diff_5'] = df_iv_stats['LLT_5'].diff()

df_iv_stats = df_iv_stats.set_index(c.Util.DT_DATE)



""" Volatility Strategy: Straddle """
d1 = df_future_c1_daily[c.Util.DT_DATE].values[0]
d2 = df_metrics[c.Util.DT_DATE].values[0]
d = max(d1,d2)
print(d1,d2,d)
df_metrics = df_metrics[df_metrics[c.Util.DT_DATE] >= d].reset_index(drop=True)
df_c1 = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)
df_c_all = df_futures_all_daily[df_futures_all_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)

optionset = BaseOptionSet(df_metrics)
optionset.init()
d1 = optionset.eval_date

hedging = SytheticOption(df_c1, frequency=c.FrequentType.DAILY,df_c1_daily=df_c1,df_futures_all_daily=df_c_all)

hedging.init()

account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)
maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
id_future = hedging.current_state[c.Util.ID_FUTURE]

empty_position = True
unit_p = None
unit_c = None
atm_strike = None
buy_write = c.BuyWrite.WRITE
print(optionset.eval_date, hedging.eval_date)
while optionset.eval_date <= end_date:
    if account.cash <=0 : break
    if maturity1 > end_date: # Final close out all.
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
                df = hedging.df_all_futures_daily[(hedging.df_all_futures_daily[c.Util.DT_DATE] == hedging.eval_date) & (
                    hedging.df_all_futures_daily[c.Util.ID_FUTURE] == id_future)]
                print('移仓：')
                print(df)
                print(id_future,hedging.current_state[c.Util.ID_FUTURE])
                trade_unit = account.trade_book.loc[hedging.name_code(), c.Util.TRADE_UNIT]
                if account.trade_book.loc[hedging.name_code(), c.Util.TRADE_LONG_SHORT] == c.LongShort.LONG:
                    long_short = c.LongShort.SHORT
                else:
                    long_short = c.LongShort.LONG
                order = Order(holding.eval_date, hedging.name_code(), trade_unit, df[c.Util.AMT_CLOSE].values[0],
                              holding.eval_datetime, long_short)
                record = hedging.execute_order(order,slippage=slippage)
                account.add_record(record, holding)
        hedging.synthetic_unit = 0
        id_future = hedging.current_state[c.Util.ID_FUTURE]

    if not empty_position:
        moneyness_put = optionset.get_option_moneyness(atm_put)
        moneyness_call = optionset.get_option_moneyness(atm_call)
        if close_signal(optionset.eval_date,maturity1,df_iv_stats):
            for option in account.dict_holding.values():
                order = account.create_close_order(option)
                record = option.execute_order(order,slippage=slippage)
                account.add_record(record, option)
                hedging.synthetic_unit = 0
            empty_position = True

    if empty_position and open_signal(optionset.eval_date,df_iv_stats):
        buy_write = c.BuyWrite.WRITE
        long_short = c.LongShort.SHORT
        maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
        atm_call = optionset.select_higher_volume(list_atm_call)
        atm_put = optionset.select_higher_volume(list_atm_put)
        atm_strike = atm_call.strike()
        spot = atm_call.underlying_close()
        # if abs(atm_strike-spot) < 0.5: # 存在平值期权
        # hedging.amt_option = 1 / 1000  # 50ETF与IH点数之比
        hedging.amt_option = 1  # 商品期权标的即为期货
        unit_c = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_call.multiplier())*m
        unit_p = np.floor(np.floor(account.portfolio_total_value / atm_put.strike()) / atm_put.multiplier())*m
        order_c = account.create_trade_order(atm_call, long_short, unit_c)
        order_p = account.create_trade_order(atm_put, long_short, unit_p)
        record_call = atm_call.execute_order(order_c, slippage=slippage)
        record_put = atm_put.execute_order(order_p, slippage=slippage)
        account.add_record(record_call, atm_call)
        account.add_record(record_put, atm_put)
        empty_position = False

    if not empty_position: # Delta hedge
        # iv_htbr = optionset.get_iv_by_otm_iv_curve(nbr_maturiy=0, strike=atm_call.applicable_strike())
        # delta_call = atm_call.get_delta(iv_htbr)
        # delta_put = atm_put.get_delta(iv_htbr)
        iv1 = atm_call.get_implied_vol()
        iv2 = atm_put.get_implied_vol()
        if iv1 is None or iv2 is None:
            print('null volatility ')
            list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
            iv1 = list_atm_call[0].get_implied_vol()
            iv2 = list_atm_put[0].get_implied_vol()
            if iv1 is None : iv1 = iv2
            if iv2 is None: iv2 = iv1
        delta_call = atm_call.get_delta(iv1)
        delta_put = atm_put.get_delta(iv2)
        options_delta = unit_c * atm_call.multiplier() * delta_call + unit_p * atm_put.multiplier() * delta_put
        hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta,  buy_write)
        hedging.synthetic_unit += - hedge_unit
        if hedge_unit > 0:
            long_short = c.LongShort.LONG
        else:
            long_short = c.LongShort.SHORT
        order_u = account.create_trade_order(hedging, long_short, hedge_unit)
        record_u = hedging.execute_order(order_u, slippage=slippage)
        account.add_record(record_u, hedging)

    account.daily_accounting(optionset.eval_date)
    total_liquid_asset = account.cash + account.get_portfolio_margin_capital()
    print(optionset.eval_date,hedging.eval_date,
          account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV], int(account.cash),int(total_liquid_asset))
    if not optionset.has_next():break
    optionset.next()
    hedging.next()


account.account.to_csv('account.csv')
df_records = pd.DataFrame(account.list_records)
df_records.to_csv('df_records.csv')
res = account.get_netvalue_analysis(account.account[c.Util.PORTFOLIO_NPV])
print(res)
dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu.plot_line_chart(dates,[npv],['npv'])

plt.show()