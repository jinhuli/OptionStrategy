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

pu = PlotUtil()
# start_date = datetime.date(2018, 1, 1)
# end_date = datetime.date(2018, 8, 20)
# start_date = datetime.date(2015, 3, 1)
# end_date = datetime.date(2015, 12, 31)
# start_date = datetime.date(2016, 1, 1)
# end_date = datetime.date(2016, 12, 31)
# start_date = datetime.date(2017, 1, 1)
# end_date = datetime.date(2017, 12, 31)
start_date = datetime.date(2016, 2, 1)
end_date = datetime.date(2017, 1, 31)
# start_date = datetime.date(2015, 3, 1)
# end_date = datetime.date(2018, 8, 20)
close_out_date = end_date
min_holding = 15
nbr_maturity = 1
# nbr_maturity = 0
slippage = 0
pct_underlying_invest = 0.7
res = {}
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_underlying = get_data.get_index_mktdata(start_date, end_date, c.Util.STR_INDEX_50ETF)
# df_underlying.to_csv('../accounts_data/df_underlying.csv')

def get_option_unit(option_put: BaseOption, underlying_value:float, target_delta):
    if target_delta is None:
        unit = np.floor(underlying_value/ option_put.strike() / option_put.multiplier()) # 期权名义本金等于标的市值
    else:
        delta = option_put.get_delta(option_put.get_implied_vol())
        unit = np.floor(target_delta * underlying_value / delta / option_put.multiplier())  # 根据目标delta值
    return unit


list_annualized_yield = []
list_annualized_volatility = []
list_max_drawdown = []
list_sharpe_ratio = []
list_pct_option_amt = []

for moneyness in [0, -1, -2, -3, -4]:
# for moneyness in [-1, -2, -3]:
# for moneyness in [-3]:
    dict_annualized_yield = {'m':moneyness}
    dict_annualized_volatility = {'m':moneyness}
    dict_max_drawdown = {'m':moneyness}
    dict_sharpe_ratio = {'m':moneyness}
    dict_pct_option_amt = {'m':moneyness}
    # for target_delta in [-0.1,-0.2,-0.3,-0.4]:
    for target_delta in [None]:
        option_amt = []
        optionset = BaseOptionSet(df_metrics)
        optionset.init()
        underlying = BaseInstrument(df_underlying)
        underlying.init()
        account = BaseAccount(init_fund=10000000, leverage=1.0, rf=0.03)

        """ init open position """
        unit_underlying = np.floor(
            pct_underlying_invest * account.cash / underlying.mktprice_close() / underlying.multiplier())
        order_underlying = account.create_trade_order(underlying, c.LongShort.LONG, unit_underlying)
        record_underlying = underlying.execute_order(order_underlying, slippage=slippage)
        account.add_record(record_underlying, underlying)
        maturity1 = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness, maturity1)
        if list_atm_put is None:
            # list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
            print('choose min strike')
            list_atm_put = optionset.get_deepest_otm_put_list(maturity1)
        atm_put = optionset.select_higher_volume(list_atm_put)
        underlying_value = unit_underlying * underlying.mktprice_close() * underlying.multiplier()
        unit = get_option_unit(atm_put, underlying_value,target_delta)
        order = account.create_trade_order(atm_put, c.LongShort.LONG, unit)
        record = atm_put.execute_order(order, slippage=slippage)
        account.add_record(record, atm_put)

        while optionset.has_next():
            """ 最终平仓 """
            if optionset.eval_date >= close_out_date:
                close_out_orders = account.creat_close_out_order()
                for order in close_out_orders:
                    execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                               execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
                    account.add_record(execution_record, account.dict_holding[order.id_instrument])
                account.daily_accounting(optionset.eval_date)
                # print(optionset.eval_date, ' close out option')
                # print(optionset.eval_date, underlying.eval_date,
                #       account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],
                #       int(account.cash))
                break
            # if optionset.eval_date == maturity1:
            if optionset.eval_date > maturity1 - datetime.timedelta(days=30):  # Roll to next maturity
                order = account.create_close_order(atm_put)
                execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=slippage,
                                                                                           execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
                account.add_record(execution_record, account.dict_holding[order.id_instrument])
                # print('roll', optionset.eval_date, maturity1, account.cash)
                maturity1 = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
                list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness, maturity1)
                if list_atm_put is None:
                    # list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
                    list_atm_put = optionset.get_deepest_otm_put_list(maturity1)
                atm_put = optionset.select_higher_volume(list_atm_put)
                underlying_value = unit_underlying * underlying.mktprice_close() * underlying.multiplier()
                unit = get_option_unit(atm_put, underlying_value,target_delta)
                order = account.create_trade_order(atm_put, c.LongShort.LONG, unit)
                record = atm_put.execute_order(order, slippage=slippage)
                account.add_record(record, atm_put)
            else:
                option_moneyness = optionset.get_option_moneyness(atm_put)
                if abs(option_moneyness - moneyness) > 1: # shift strike
                    order = account.create_close_order(atm_put)
                    execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=slippage,
                                                                                               execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
                    account.add_record(execution_record, account.dict_holding[order.id_instrument])
                    list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness, maturity1)
                    if list_atm_put is None:
                        # list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
                        list_atm_put = optionset.get_deepest_otm_put_list(maturity1)
                    atm_put = optionset.select_higher_volume(list_atm_put)
                    underlying_value = unit_underlying * underlying.mktprice_close() * underlying.multiplier()
                    unit = get_option_unit(atm_put, underlying_value,target_delta)
                    order = account.create_trade_order(atm_put, c.LongShort.LONG, unit)
                    record = atm_put.execute_order(order, slippage=slippage)
                    account.add_record(record, atm_put)
                else: # rebalance delta
                    if target_delta is not None:
                        underlying_value = unit_underlying * underlying.mktprice_close() * underlying.multiplier()
                        unit_new = get_option_unit(atm_put, underlying_value,target_delta)
                        d_unit = unit_new - unit
                        if d_unit >= 0:
                            long_short = c.LongShort.LONG
                        else:
                            long_short = c.LongShort.SHORT
                        unit = unit_new
                        order = account.create_trade_order(atm_put, long_short, d_unit)
                        record = atm_put.execute_order(order, slippage=slippage)
                        account.add_record(record, atm_put)
            account.daily_accounting(optionset.eval_date)
            option_amt.append(unit*atm_put.multiplier())
            print(optionset.eval_date, underlying.eval_date,
                  account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],
                  unit)
            optionset.next()
            underlying.next()

        # res.update({'date': list(account.account['dt_date'])})
        # res.update({moneyness: list(account.account[c.Util.PORTFOLIO_NPV])})
        # pu.plot_line_chart(account.account[c.Util.DT_DATE], [account.account[c.Util.PORTFOLIO_NPV]], ['npv'])
        # pu.plot_line_chart(account.account[c.Util.DT_DATE], [account.account[c.Util.CASH] / account.init_fund], ['cash'])
        # pu.plot_line_chart(account.account[c.Util.DT_DATE], [account.account[c.Util.PORTFOLIO_DELTA]], ['delta'])
        # print(account.get_netvalue_analysis(account.account[c.Util.PORTFOLIO_NPV]))
        account.account.to_csv('../accounts_data/account'+str(slippage)+str(target_delta)+str(moneyness)+'.csv')
        r = account.get_netvalue_analysis(account.account[c.Util.PORTFOLIO_NPV])
        return_yr = r['年化收益率']
        volatility_yr = r['年化波动率']
        maxdrawdown = r['最大回撤率']
        sharpe = r['夏普比率']
        pct_option_amt = (sum(option_amt)/len(option_amt))/unit_underlying/underlying.multiplier()
        dict_annualized_yield.update({target_delta:return_yr})
        dict_annualized_volatility.update({target_delta:volatility_yr})
        dict_max_drawdown.update({target_delta:maxdrawdown})
        dict_sharpe_ratio.update({target_delta:sharpe})
        dict_pct_option_amt.update({target_delta:pct_option_amt})
    list_annualized_yield.append(dict_annualized_yield)
    list_annualized_volatility.append(dict_annualized_volatility)
    list_max_drawdown.append(dict_max_drawdown)
    list_sharpe_ratio.append(dict_sharpe_ratio)
    list_pct_option_amt.append(dict_pct_option_amt)

df_annualized_yield = pd.DataFrame(list_annualized_yield)
df_annualized_volatility = pd.DataFrame(list_annualized_volatility)
df_max_drawdown = pd.DataFrame(list_max_drawdown)
df_sharpe_ratio = pd.DataFrame(list_sharpe_ratio)
df_pct_option_amt = pd.DataFrame(list_pct_option_amt)

# df_annualized_yield.to_csv('../accounts_data/df_annualized_yield_ALL.csv')
# df_annualized_volatility.to_csv('../accounts_data/df_annualized_volatility_ALL.csv')
# df_max_drawdown.to_csv('../accounts_data/df_max_drawdown_ALL.csv')
# df_sharpe_ratio.to_csv('../accounts_data/df_sharpe_ratio_ALL.csv')
# df_pct_option_amt.to_csv('../accounts_data/df_pct_option_amt_ALL.csv')

df_annualized_yield.to_csv('../accounts_data/df_annualized_yield_熔断.csv')
df_annualized_volatility.to_csv('../accounts_data/df_annualized_volatility_熔断.csv')
df_max_drawdown.to_csv('../accounts_data/df_max_drawdown_熔断.csv')
df_sharpe_ratio.to_csv('../accounts_data/df_sharpe_ratio_熔断.csv')
df_pct_option_amt.to_csv('../accounts_data/df_pct_option_amt_熔断.csv')

# df_annualized_yield.to_csv('../accounts_data/df_annualized_yield_'+str(start_date.year)+'.csv')
# df_annualized_volatility.to_csv('../accounts_data/df_annualized_volatility_'+str(start_date.year)+'.csv')
# df_max_drawdown.to_csv('../accounts_data/df_max_drawdown_'+str(start_date.year)+'.csv')
# df_sharpe_ratio.to_csv('../accounts_data/df_sharpe_ratio_'+str(start_date.year)+'.csv')
# df_pct_option_amt.to_csv('../accounts_data/df_pct_option_amt_'+str(start_date.year)+'.csv')

print(df_annualized_yield)
print(df_pct_option_amt)

# df_res = pd.DataFrame(res)
# df_res.to_csv('df_res.csv')
# pu.plot_line_chart(res['date'],[res[0],res[-1]],['npv: buy atm put','npv: buy otm 1 put'])
# plt.show()
