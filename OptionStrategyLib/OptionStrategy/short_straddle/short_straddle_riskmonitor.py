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
start_date = datetime.date(2016, 1, 1)
end_date = datetime.date(2018, 10, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 20  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
m = 1  # 期权notional倍数
cd_trade_price = c.CdTradePrice.VOLUME_WEIGHTED

""" Risk Monitor """
df_warning = pd.read_excel('../../../data/risk_monitor.xlsx')
df_warning['date'] = df_warning['dt_date'].apply(lambda x: x.date())
df_warning = df_warning[['date', 'risk warning']]
df_warning['pre_warned'] = df_warning['risk warning'].shift()
df_warning=df_warning.set_index('date')


def risk_warning(df_warning, dt_date):
    if dt_date not in df_warning.index:
        return False
    else:
        if df_warning.loc[dt_date, 'pre_warned'] > 0:
            return True
        else:
            return False


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

# hedging = SytheticOption(df_c1, frequency=c.FrequentType.DAILY, df_c1_daily=df_c1, df_futures_all_daily=df_c_all)
# hedging.init()
# print(optionset.eval_date, hedging.eval_date)

account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)

option_trade_times = 0
empty_position = True
unit_p = None
unit_c = None
atm_strike = None
buy_write = c.BuyWrite.WRITE
maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
# id_future = hedging.current_state[c.Util.ID_FUTURE]
# print(id_future)
while optionset.eval_date <= end_date:
    if account.cash <= 0: break
    if maturity1 > end_date:  # Final close out all.
        close_out_orders = account.creat_close_out_order()
        for order in close_out_orders:
            execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                       execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
            account.add_record(execution_record, account.dict_holding[order.id_instrument])
        account.daily_accounting(optionset.eval_date)

        break
    # 平仓
    if not empty_position and ((maturity1 - optionset.eval_date).days <= 8 or risk_warning(df_warning,optionset.eval_date)):
        for option in account.dict_holding.values():
            order = account.create_close_order(option, cd_trade_price=cd_trade_price)
            record = option.execute_order(order, slippage=slippage)
            account.add_record(record, option)
            # hedging.synthetic_unit = 0
        empty_position = True


    # 开仓：距到期1M
    if empty_position and not risk_warning(df_warning,optionset.eval_date):
        maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
        option_trade_times += 1
        buy_write = c.BuyWrite.WRITE
        long_short = c.LongShort.SHORT
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0,
                                                                                        maturity=maturity1,cd_price=c.CdPriceType.OPEN)
        atm_call = optionset.select_higher_volume(list_atm_call)
        atm_put = optionset.select_higher_volume(list_atm_put)
        atm_strike = atm_call.strike()
        spot = atm_call.underlying_close()
        # hedging.amt_option = 1 / 1000  # 50ETF与IH点数之比
        unit_c = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_call.multiplier()) * m
        unit_p = np.floor(np.floor(account.portfolio_total_value / atm_put.strike()) / atm_put.multiplier()) * m
        order_c = account.create_trade_order(atm_call, long_short, unit_c, cd_trade_price=cd_trade_price)
        order_p = account.create_trade_order(atm_put, long_short, unit_p, cd_trade_price=cd_trade_price)
        record_call = atm_call.execute_order(order_c, slippage=slippage)
        record_put = atm_put.execute_order(order_p, slippage=slippage)
        account.add_record(record_call, atm_call)
        account.add_record(record_put, atm_put)
        empty_position = False

    account.daily_accounting(optionset.eval_date)
    total_liquid_asset = account.cash + account.get_portfolio_margin_capital()
    if not optionset.has_next(): break
    optionset.next()
    # hedging.next()

account.account.to_csv('../../accounts_data/short_straddle_account-riskmonitor.csv')
account.trade_records.to_csv('../../accounts_data/short_straddle_records-riskmonitor.csv')
res = account.analysis()
res['option_average_holding_days'] = len(account.account) / option_trade_times
print(res)

dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu.plot_line_chart(dates, [npv], ['npv'])
plt.show()
