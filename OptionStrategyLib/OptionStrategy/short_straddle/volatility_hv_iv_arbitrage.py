from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from back_test.model.base_option import BaseOption
from data_access import get_data
import back_test.model.constant as c
import datetime
import numpy as np
from OptionStrategyLib.OptionReplication.synthetic_option import SytheticOption
from PricingLibrary.Options import EuropeanOption
from PricingLibrary.BinomialModel import BinomialTree
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
import Utilities.admin_util as admin
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
import pandas as pd

pu = PlotUtil()
start_date = datetime.date(2016, 6, 1)
# start_date = datetime.date(2018, 1, 1)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 15
init_fund = c.Util.BILLION
slippage = 0
m = 2
""" commodity option """
# name_code = name_code_option = c.Util.STR_M
# df_metrics = get_data.get_comoption_mktdata(start_date, end_date,name_code)
# df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)

""" 50ETF option """
name_code = c.Util.STR_IH
name_code_option = c.Util.STR_50ETF
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
# df_future_c1_daily = get_data.get_dzqh_cf_c1_daily(dt_histvol, end_date, name_code)
df_future_c1_daily = get_data.get_mktdata_cf_c1_daily(dt_histvol, end_date, name_code)

""" 历史波动率 """
# df_vol_1m = Histvol.hist_vol(df_future_c1_daily)
# df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
# df_garman_klass = Histvol.garman_klass(df_future_c1_daily)
# df_data = df_future_c1_daily.join(df_vol_1m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_garman_klass,on=c.Util.DT_DATE,how='left')
# df_data = df_data.join(df_parkinson_1m,on=c.Util.DT_DATE,how='left')
# df_data = df_data.dropna()
""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(dt_histvol,end_date,name_code_option)
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
# df_data = df_data.join(df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
#     .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna()
df_data.loc[:,'average_iv'] = df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put']
# # df_data.loc[:,'diff_hist_call_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL+'_20']-df_data.loc[:,'iv_call']
# # df_data.loc[:,'diff_hist_put_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL+'_20']-df_data.loc[:,'iv_put']
# df_data = df_data.sort_values(by='dt_date', ascending=False)
# df_data.to_csv('../../data/df_data.csv')

""" Volatility Statistics """
df_iv_stats = df_data[[c.Util.DT_DATE, 'average_iv']]

df_iv_stats.loc[:,'iv_std_60'] = c.Statistics.standard_deviation(df_iv_stats['average_iv'], n=60)
df_iv_stats.loc[:,'ma_60'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=60)
df_iv_stats.loc[:,'ma_20'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=20)
df_iv_stats.loc[:,'ma_10'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=10)
df_iv_stats.loc[:,'ma_3'] = c.Statistics.moving_average(df_iv_stats['average_iv'], n=3)
df_iv_stats = df_iv_stats.dropna()
df_iv_stats.loc[:,'upper'] = upper = df_iv_stats.loc[:,'ma_60'] + df_iv_stats.loc[:,'iv_std_60']
df_iv_stats.loc[:,'lower'] = lower = df_iv_stats.loc[:,'ma_60'] - df_iv_stats.loc[:,'iv_std_60']
df_iv_stats = df_iv_stats.set_index(c.Util.DT_DATE)
""" Open/Close Position Signal """

def open_signal_ma(dt_date,df_stats)->bool:
    df_stats.loc[:, 'last_ma_10'] = df_stats['ma_10'].shift()
    iv = df_stats.loc[dt_date,'average_iv']
    ma_10 = df_stats.loc[dt_date,'ma_10']
    ma_20 = df_stats.loc[dt_date,'ma_20']
    if iv < ma_10 and ma_10 < ma_20:
        print('0.OPEN: short trend ', dt_date)
        return True
    else:
        return False

def close_signal_ma(dt_date,option_maturity,df_stats,k_straddle=None,k0=None)->bool:
    if dt_date >= maturity1 - datetime.timedelta(days=1):
        print('3.到期')
        return True
    else:
        df_stats.loc[:,'last_ma_10'] = df_stats['ma_10'].shift()
        df_stats.loc[:,'last_iv'] = df_stats['average_iv'].shift()
        iv = df_stats.loc[dt_date, 'average_iv']
        iv_last = df_stats.loc[dt_date, 'last_iv']
        ma_10_last = df_stats.loc[dt_date, 'last_ma_10']
        iv_upper = df_stats.loc[dt_date, 'upper']
        ma_10 = df_stats.loc[dt_date, 'ma_10']
        ma_60 = df_stats.loc[dt_date, 'ma_60']
        if iv_last <= iv_upper and iv >= iv_upper:  # 止损
            print('1.STOP LOSS ',dt_date)
            return True
        elif ma_10_last <= ma_60 and ma_10 > ma_60: # 止盈
            print('2.STOP EARNING', dt_date)
            return True
        else:
            return False


""" Volatility Strategy: Straddle """
optionset = BaseOptionSet(df_metrics)
optionset.init()
d1 = optionset.eval_date
df_c1 = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= d1].reset_index(drop=True)
df_c1 = df_c1.rename(columns={c.Util.ID_INSTRUMENT:'id_future'})
df_c1.loc[:,c.Util.ID_INSTRUMENT] = 'ih'
# df_tmp = df_metrics.drop_duplicates(c.Util.DT_DATE).join(df_c1[[c.Util.DT_DATE, c.Util.ID_INSTRUMENT]].set_index(c.Util.DT_DATE).rename(columns={c.Util.ID_INSTRUMENT:'id_future'}),
#                 on=c.Util.DT_DATE,how='left')
# df_tmp2 = df_metrics.drop_duplicates(c.Util.DT_DATE).join(df_c1[[c.Util.DT_DATE, c.Util.ID_INSTRUMENT]].set_index(c.Util.DT_DATE).rename(columns={c.Util.ID_INSTRUMENT:'id_future'}),
#                 on=c.Util.DT_DATE,how='left')
# check_data = df_tmp[df_tmp['id_future'].isnull()]
# check_data2 = df_tmp[df_tmp2['dt_date'].isnull()]
# print(check_data)

hedging = SytheticOption(df_c1, frequency=c.FrequentType.DAILY)
hedging.init()

account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)
maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
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
    # k0 = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)[0][0].strike()
    if not empty_position:
        # if optionset.eval_date >= maturity1 - datetime.timedelta(days=1) or abs(atm_strike - k0) > 1.0:
        # if optionset.eval_date >= maturity1 - datetime.timedelta(days=1):
        if close_signal_ma(optionset.eval_date,maturity1,df_iv_stats):
            # print('REBALANCED : ', optionset.eval_date)
            for option in account.dict_holding.values():
                # if isinstance(option, BaseOption):
                order = account.create_close_order(option)
                record = option.execute_order(order,slippage=slippage)
                account.add_record(record, option)
                hedging.synthetic_unit = 0
            empty_position = True

    if empty_position and open_signal_ma(optionset.eval_date,df_iv_stats):
        buy_write = c.BuyWrite.WRITE
        long_short = c.LongShort.SHORT
        maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
        atm_call = optionset.select_higher_volume(list_atm_call)
        atm_put = optionset.select_higher_volume(list_atm_put)
        atm_strike = atm_call.strike()
        hedging.amt_option = 1 / 1000  # 50ETF与IH点数之比
        unit_c = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_call.multiplier())*m
        unit_p = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_put.multiplier())*m

        order_c = account.create_trade_order(atm_call, long_short, unit_c)
        order_p = account.create_trade_order(atm_put, long_short, unit_p)
        record_call = atm_call.execute_order(order_c, slippage=slippage)
        record_put = atm_put.execute_order(order_p, slippage=slippage)
        account.add_record(record_call, atm_call)
        account.add_record(record_put, atm_put)
        empty_position = False

    if not empty_position: # Delta hedge
        delta_call = atm_call.get_delta(atm_call.get_implied_vol())
        delta_put = atm_put.get_delta(atm_put.get_implied_vol())
        options_delta = unit_c * atm_call.multiplier() * delta_call + unit_p * atm_put.multiplier() * delta_put
        hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta, c.DeltaBound.NONE, buy_write,
                                                        atm_call.get_implied_vol(), atm_call.underlying_close(),
                                                        atm_call.get_gamma(atm_call.get_implied_vol()), atm_call.maturitydt())
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
    # print(optionset.eval_date,hedging.eval_date,
    #       account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV], int(account.cash),int(total_liquid_asset))
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