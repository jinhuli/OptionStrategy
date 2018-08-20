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
# start_date = datetime.date(2016, 6, 1)
start_date = datetime.date(2018, 1, 17)
end_date = datetime.date(2018, 8, 8)
dt_histvol = start_date - datetime.timedelta(days=40)
min_holding = 15
init_fund = c.Util.BILLION
slippage = 0

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
# df_iv = get_data.get_iv_by_moneyness(start_date,end_date,name_code_option)
# df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
# df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
# df_data = df_data.join(df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
#     .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
# df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
#     .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
# df_data = df_data.dropna()
# # df_data.loc[:,'average_iv'] = df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put']
# # df_data.loc[:,'diff_hist_call_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL+'_20']-df_data.loc[:,'iv_call']
# # df_data.loc[:,'diff_hist_put_iv'] = df_data.loc[:,c.Util.AMT_HISTVOL+'_20']-df_data.loc[:,'iv_put']
# df_data = df_data.sort_values(by='dt_date', ascending=False)
# df_data.to_csv('../../data/df_data.csv')

""" Volatility Strategy: Straddle """
df_c1 = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= start_date].reset_index(drop=True)
# df_tmp = df_metrics.drop_duplicates(c.Util.DT_DATE).join(df_c1[[c.Util.DT_DATE, c.Util.ID_INSTRUMENT]].set_index(c.Util.DT_DATE).rename(columns={c.Util.ID_INSTRUMENT:'id_future'}),
#                 on=c.Util.DT_DATE,how='left')
# df_tmp2 = df_metrics.drop_duplicates(c.Util.DT_DATE).join(df_c1[[c.Util.DT_DATE, c.Util.ID_INSTRUMENT]].set_index(c.Util.DT_DATE).rename(columns={c.Util.ID_INSTRUMENT:'id_future'}),
#                 on=c.Util.DT_DATE,how='left')
# check_data = df_tmp[df_tmp['id_future'].isnull()]
# check_data2 = df_tmp[df_tmp2['dt_date'].isnull()]
# print(check_data)

hedging = SytheticOption(df_c1, frequency=c.FrequentType.DAILY)
hedging.init()
optionset = BaseOptionSet(df_metrics)
optionset.init()
account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)
maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
empty_position = True

# TODO: CASH DECREASES WHEN NPV INCREASED:
# 原因应该是option execute order的时候对longshort分开处理吗，但平仓的时候没有加回来之前short的保证金.
print(optionset.eval_date, hedging.eval_date)
while maturity1 <= end_date:
    k0 = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)[0][0].strike()
    # k1 = optionset.get_options_list_by_moneyness_mthd1(1, maturity1)[0][0].strike()
    for option in account.dict_holding.values():
        if isinstance(option, BaseOption):
            if optionset.eval_date >= maturity1 - datetime.timedelta(days=1) or abs(option.strike() - k0) > 1.0:
                print('REBALANCED : ', optionset.eval_date)
                order = account.create_close_order(option)
                record = option.execute_order(order,slippage=slippage)
                account.add_record(record, option)
                maturity1 = optionset.select_maturity_date(nbr_maturity=0, min_holding=15)
                empty_position = True
    if empty_position:
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
        atm_call = optionset.select_higher_volume(list_atm_call)
        atm_put = optionset.select_higher_volume(list_atm_put)
        hedging.amt_option = amt_option = np.floor(account.cash / atm_call.strike()) / 1000  # 50ETF与IH点数之比
        unit_c = np.floor(amt_option / atm_call.multiplier())
        unit_p = np.floor(amt_option / atm_put.multiplier())
        print(account.cash, amt_option, unit_c, unit_p)
        order_c = account.create_trade_order(atm_call, c.LongShort.SHORT, unit_c)
        order_p = account.create_trade_order(atm_put, c.LongShort.SHORT, unit_p)
        record_call = atm_call.execute_order(order_c, slippage=slippage)
        record_put = atm_put.execute_order(order_p, slippage=slippage)
        account.add_record(record_call, atm_call)
        account.add_record(record_put, atm_put)
        empty_position = False

    delta_call = atm_call.get_delta()
    delta_put = atm_put.get_delta()
    options_delta = unit_c * atm_call.multiplier() / amt_option * delta_call + unit_p * atm_put.multiplier() / amt_option * delta_put
    hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta, c.DeltaBound.NONE, c.BuyWrite.WRITE,
                                                    atm_call.get_implied_vol(), atm_call.underlying_close(),
                                                    atm_call.get_gamma(), atm_call.maturitydt())
    hedging.synthetic_unit = - hedge_unit
    if hedge_unit > 0:
        long_short = c.LongShort.LONG
    else:
        long_short = c.LongShort.SHORT
    order_u = account.create_trade_order(hedging, long_short, hedge_unit)
    record_u = hedging.execute_order(order_u, slippage=slippage)
    account.add_record(record_u, hedging)

    account.daily_accounting(optionset.eval_date)
    print(optionset.eval_date,hedging.eval_date,
          account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],options_delta, hedge_unit, account.cash)
    optionset.next()
    hedging.next()


account.account.to_csv('account.csv')