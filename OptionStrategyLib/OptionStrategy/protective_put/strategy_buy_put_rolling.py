from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from back_test.model.base_instrument import BaseInstrument
from back_test.model.base_option import BaseOption
from data_access import get_data
import back_test.model.constant as c
import datetime
import numpy as np
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
import math


def get_option_unit(option_put: BaseOption, underlying_value: float):
    # unit = np.floor(underlying_value / option_put.strike() / option_put.multiplier())  # 期权名义本金等于标的市值
    unit = np.floor(underlying_value / option_put.underlying_close() / option_put.multiplier())  # 期权名义本金等于标的市值
    return unit

def buy_put(moneyness, maturity1):
    list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness, maturity1)
    if list_atm_put is None:
        print('Given moneyness not available, choose min strike')
        list_atm_put = optionset.get_deepest_otm_put_list(maturity1)
    atm_put = optionset.select_higher_volume(list_atm_put)
    # unit = unit_underlying * underlying.multiplier()/atm_put.multiplier() # 50ETF
    unit = equal_50etf_unit * underlying.multiplier()/atm_put.multiplier() # 沪深300指数
    order = account.create_trade_order(atm_put, c.LongShort.LONG, unit)
    record = atm_put.execute_order(order, slippage=slippage)
    account.add_record(record, atm_put)
    premium = record[c.Util.TRADE_BOOK_VALUE]
    return atm_put,premium



start_date = datetime.date(2015, 2, 1)
# start_date = datetime.date(2017, 2, 1)
end_date = datetime.date(2018, 8, 31)
d1 = start_date
min_holding = 15
nbr_maturity = 1
slippage = 0
pct_underlying_invest = 1.0

##############
alpha = 0.0
moneyness = -5
#################

df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_option_underlying = get_data.get_index_mktdata(start_date, end_date, c.Util.STR_INDEX_50ETF)
df_underlying = get_data.get_index_mktdata(start_date, end_date, c.Util.STR_INDEX_300SH_TOTAL_RETURN)


calendar = c.Calendar(sorted(df_underlying[c.Util.DT_DATE].unique()))
pu = PlotUtil()



df = pd.DataFrame()
#
d1 = calendar.firstBusinessDayNextMonth(d1)
d2 = d1 + datetime.timedelta(days=365)
while d2 <= end_date:
    print(d1)
    df_metrics_1 = df_metrics[(df_metrics[c.Util.DT_DATE] >= d1)&(df_metrics[c.Util.DT_DATE] <= d2)].reset_index(drop=True)
    df_underlying_1 = df_underlying[(df_underlying[c.Util.DT_DATE] >= d1)&(df_underlying[c.Util.DT_DATE] <= d2)].reset_index(drop=True)
    # df_option_underlying_1 = df_option_underlying[(df_option_underlying[c.Util.DT_DATE] >= d1)&(df_option_underlying[c.Util.DT_DATE] <= d2)].reset_index(drop=True)
    df_underlying_with_alpha = df_underlying_1[[c.Util.DT_DATE, c.Util.ID_INSTRUMENT, c.Util.AMT_CLOSE]]
    df_underlying_with_alpha.loc[:, 'r'] = np.log(df_underlying_with_alpha[c.Util.AMT_CLOSE]).diff()
    df_underlying_with_alpha.loc[:, 'r1'] = np.log(df_underlying_with_alpha[c.Util.AMT_CLOSE]).diff() + alpha / 252
    df_underlying_with_alpha.loc[:, 'close_alpha'] = None
    p0 = df_underlying_with_alpha.loc[0, c.Util.AMT_CLOSE]
    for (idx, r) in df_underlying_with_alpha.iterrows():
        if idx == 0:
            df_underlying_with_alpha.loc[idx, 'close_alpha'] = df_underlying_with_alpha.loc[0, c.Util.AMT_CLOSE]
        else:
            df_underlying_with_alpha.loc[idx, 'close_alpha'] = df_underlying_with_alpha.loc[
                                                                   idx - 1, 'close_alpha'] * np.exp(
                df_underlying_with_alpha.loc[idx, 'r1'])

    df_underlying_with_alpha = df_underlying_with_alpha[
        [c.Util.DT_DATE, c.Util.ID_INSTRUMENT, c.Util.AMT_CLOSE, 'close_alpha']].rename(
        columns={c.Util.AMT_CLOSE: 'etf_close'})
    df_underlying_with_alpha = df_underlying_with_alpha.rename(columns={'close_alpha': c.Util.AMT_CLOSE})
    # df_underlying_with_alpha.to_csv('../accounts_data/df_underlying_with_alpha='+str(alpha)+'.csv')
    """ Init Portfolio and Account """
    init_fund=10000000
    optionset = BaseOptionSet(df_metrics_1)
    optionset.init()
    underlying = BaseInstrument(df_underlying_with_alpha)
    underlying.init()
    account = BaseAccount(init_fund, leverage=1.0, rf=0.03)

    """ 初始开仓 """
    unit_underlying = np.floor(pct_underlying_invest * account.cash / underlying.mktprice_close() / underlying.multiplier())
    order_underlying = account.create_trade_order(underlying, c.LongShort.LONG, unit_underlying)
    record_underlying = underlying.execute_order(order_underlying, slippage=slippage)
    account.add_record(record_underlying, underlying)
    maturity1 = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
    equal_50etf_unit = unit_underlying*underlying.mktprice_close()/optionset.eligible_options[0].underlying_close()
    atm_put,premium = buy_put(moneyness,maturity1)

    # SH300指数

    total_premium = premium
    while optionset.has_next():
        """ 最终平仓 """
        if optionset.eval_date >= d2:
            print('Close out.')
            close_out_orders = account.creat_close_out_order()
            for order in close_out_orders:
                execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                           execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
                account.add_record(execution_record, account.dict_holding[order.id_instrument])
            account.daily_accounting(optionset.eval_date)
            break
        " Roll to next maturity "
        if optionset.eval_date > maturity1 - datetime.timedelta(days=30):
            order = account.create_close_order(atm_put)
            execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=slippage,
                                                                                       execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
            account.add_record(execution_record, account.dict_holding[order.id_instrument])
            maturity1 = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
            atm_put,premium = buy_put(moneyness,maturity1)
            total_premium += premium + execution_record[c.Util.TRADE_BOOK_VALUE]

        account.daily_accounting(optionset.eval_date)
        optionset.next()
        underlying.next()

    # series_npv = account.account[c.Util.PORTFOLIO_NPV]
    # series_npv.iloc[-1] = series_npv.iloc[-1] * (1+alpha) # plus alpha
    analysis = account.get_netvalue_analysis(account.account[c.Util.PORTFOLIO_NPV])
    analysis['权利金占比'] = total_premium/init_fund
    df_underlying_with_alpha.loc[:,'npv_50etf'] = df_underlying_with_alpha.loc[:,'etf_close']/df_underlying_with_alpha.loc[0,'etf_close']
    analysis_50ETF = account.get_netvalue_analysis(df_underlying_with_alpha['npv_50etf'])
    df_underlying_with_alpha.loc[:,'npv_50etf_alpha'] = df_underlying_with_alpha.loc[:,c.Util.AMT_CLOSE]/df_underlying_with_alpha.loc[0,c.Util.AMT_CLOSE]
    analysis_50ETF_alpha = account.get_netvalue_analysis(df_underlying_with_alpha['npv_50etf_alpha'])

    df[str(d1)+':hedged'] = analysis
    df[str(d1)+':etf'] = analysis_50ETF
    df[str(d1)+':etf_alpha'] = analysis_50ETF_alpha
    d1 = calendar.firstBusinessDayNextMonth(d1)
    d2 = d1 + datetime.timedelta(days=365)
print(df)
df.to_csv('../../accounts_data/buy_put_rolling-sh300-_alpha='+str(alpha)+'_m='+str(moneyness)+'-unitmatch.csv')

