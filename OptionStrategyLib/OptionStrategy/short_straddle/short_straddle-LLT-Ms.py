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

""" Open/Close Position Signal """
def open_signal(dt_date, df_status):
    return write_signal_tangent(dt_date, df_status)

def close_signal(dt_date,option_maturity, df_status):
    if dt_date >= option_maturity - datetime.timedelta(days=8):
        print('3.到期', dt_date)
        return True
    else:
        return close_signal_tangent(dt_date, df_status)

def write_signal_tangent(dt_date, df_status):
    if df_status.loc[dt_date,'last_diff_5'] <= 0:
        print('1.open', dt_date)
        return True
    else:
        return False

def close_signal_tangent(dt_date, df_status):
    if df_status.loc[dt_date,'last_diff_5'] > 0:
        print('2.close', dt_date)
        return True
    else:
        return False


def filtration(df_iv_stats, name_column):
    """ Filtration : LLT """
    df_iv_stats['LLT_20'] = LLT(df_iv_stats[name_column], 20)
    df_iv_stats['LLT_10'] = LLT(df_iv_stats[name_column], 10)
    df_iv_stats['LLT_5'] = LLT(df_iv_stats[name_column], 5)
    df_iv_stats['LLT_3'] = LLT(df_iv_stats[name_column], 3)
    df_iv_stats['diff_20'] = df_iv_stats['LLT_20'].diff()
    df_iv_stats['diff_10'] = df_iv_stats['LLT_10'].diff()
    df_iv_stats['diff_5'] = df_iv_stats['LLT_5'].diff()
    df_iv_stats['diff_3'] = df_iv_stats['LLT_3'].diff()
    df_iv_stats = df_iv_stats.set_index(c.Util.DT_DATE)
    df_iv_stats['last_diff_20'] = df_iv_stats['diff_20'].shift()
    df_iv_stats['last_diff_10'] = df_iv_stats['diff_10'].shift()
    df_iv_stats['last_diff_5'] = df_iv_stats['diff_5'].shift()
    df_iv_stats['last_diff_3'] = df_iv_stats['diff_3'].shift()
    return df_iv_stats

pu = PlotUtil()
start_date = datetime.date(2016, 1, 1)
end_date = datetime.date(2018, 10, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 20 # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
Ms = [1,2,3] # 期权notional倍数
cd_trade_price=c.CdTradePrice.VOLUME_WEIGHTED

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)
df_futures_all_daily = get_data.get_mktdata_future_daily(start_date, end_date, name_code)  # daily data of all future contracts

""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(dt_histvol,end_date,name_code_option)
df_ivix = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='ivix']
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
df_iv_htbr = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put_call_htbr']
df_data = df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna().reset_index(drop=True)
df_data.loc[:,'average_iv'] = (df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put'])/2
# df_data = df_iv_htbr.reset_index(drop=True).rename(columns={c.Util.PCT_IMPLIED_VOL:'average_iv'})
# df_data = df_ivix.reset_index(drop=True).rename(columns={c.Util.PCT_IMPLIED_VOL:'average_iv'})
df_data['iv_htbr'] = df_iv_htbr.reset_index(drop=True)[c.Util.PCT_IMPLIED_VOL]
# df_data.to_csv('iv.csv')
df_iv_stats = df_data[[c.Util.DT_DATE, 'average_iv','iv_htbr']]
df_iv_stats = filtration(df_iv_stats,'average_iv')

d1 = df_future_c1_daily[c.Util.DT_DATE].values[0]
d2 = df_metrics[c.Util.DT_DATE].values[0]
d = max(d1, d2)
df_metrics = df_metrics[df_metrics[c.Util.DT_DATE] >= d].reset_index(drop=True)
df_c1 = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)
df_c_all = df_futures_all_daily[df_futures_all_daily[c.Util.DT_DATE] >= d].reset_index(drop=True)

df_res = pd.DataFrame()
""" Volatility Strategy: Straddle """
for m in Ms:
    optionset = BaseOptionSet(df_metrics)
    optionset.init()

    account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)

    option_trade_times = 0
    empty_position = True
    unit_p = None
    unit_c = None
    atm_strike = None
    buy_write = c.BuyWrite.WRITE
    maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)
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
        if not empty_position and close_signal(optionset.eval_date,maturity1,df_iv_stats):
            for option in account.dict_holding.values():
                order = account.create_close_order(option, cd_trade_price=cd_trade_price)
                record = option.execute_order(order, slippage=slippage)
                account.add_record(record, option)
            empty_position = True

        # 开仓：距到期1M
        if empty_position and open_signal(optionset.eval_date,df_iv_stats):
            maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)
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

        account.daily_accounting(optionset.eval_date)
        total_liquid_asset = account.cash + account.get_portfolio_margin_capital()
        if not optionset.has_next(): break
        optionset.next()

    account.account.to_csv('../../accounts_data/short_straddle_account-LLT-'+str(m)+'.csv')
    res = account.analysis()
    res['option_average_holding_days'] = len(account.account) / option_trade_times
    print(res)
    df_res[str(m)] = res
    # dates = list(account.account.index)
    # npv = list(account.account[c.Util.PORTFOLIO_NPV])
    # pu.plot_line_chart(dates,[npv],['npv'])
    # plt.show()

df_res.to_csv('../../accounts_data/short_straddle_LLT-res.csv')


