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
start_date = datetime.date(2015, 6, 1)
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
df_vol_1m = Histvol.hist_vol(df_future_c1_daily)
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
print(optionset.eval_date, hedging.eval_date)
while optionset.eval_date <= end_date:
    if account.cash <=0 : break
    if maturity1 > end_date:
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
        if optionset.eval_date >= maturity1 - datetime.timedelta(days=1):
            for option in account.dict_holding.values():
                if isinstance(option, BaseOption):
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
        atm_strike = atm_call.strike()
        # hedging.amt_option = amt_option = np.floor(account.portfolio_total_value / atm_call.strike()) / 1000  # 50ETF与IH点数之比
        hedging.amt_option = 1 / 1000  # 50ETF与IH点数之比
        unit_c = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_call.multiplier())
        unit_p = np.floor(np.floor(account.portfolio_total_value / atm_call.strike()) / atm_put.multiplier())
        print(account.portfolio_total_value, unit_c, unit_p)
        order_c = account.create_trade_order(atm_call, c.LongShort.SHORT, unit_c)
        order_p = account.create_trade_order(atm_put, c.LongShort.SHORT, unit_p)
        record_call = atm_call.execute_order(order_c, slippage=slippage)
        record_put = atm_put.execute_order(order_p, slippage=slippage)
        account.add_record(record_call, atm_call)
        account.add_record(record_put, atm_put)
        empty_position = False

    delta_call = atm_call.get_delta()
    delta_put = atm_put.get_delta()
    options_delta = unit_c * atm_call.multiplier() * delta_call + unit_p * atm_put.multiplier() * delta_put
    hedge_unit = hedging.get_hedge_rebalancing_unit(options_delta, c.DeltaBound.NONE, c.BuyWrite.WRITE,
                                                    atm_call.get_implied_vol(), atm_call.underlying_close(),
                                                    atm_call.get_gamma(), atm_call.maturitydt())
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
          account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],round(options_delta,2), hedge_unit, int(account.cash),int(total_liquid_asset))
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