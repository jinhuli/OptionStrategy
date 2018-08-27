from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from data_access import get_data
import back_test.model.constant as c
import datetime
import numpy as np
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt

pu = PlotUtil()
start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2018, 8, 8)
min_holding = 15
nbr_maturity = 1
slippage = 0
res  ={}
for moneyness in [0,-1]:

    df_metrics = get_data.get_50option_mktdata(start_date, end_date)
    optionset = BaseOptionSet(df_metrics)
    optionset.init()
    account = BaseAccount(init_fund=c.Util.BILLION, leverage=1.0, rf=0.03)
    """ init open position """
    maturity1 = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
    list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness, maturity1)
    if list_atm_put is None:
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
    atm_put = optionset.select_higher_volume(list_atm_put)
    unit = np.floor(account.cash/atm_put.mktprice_close()/atm_put.multiplier()/5)
    order = account.create_trade_order(atm_put, c.LongShort.LONG, unit)
    record = atm_put.execute_order(order, slippage=slippage)
    account.add_record(record, atm_put)

    while optionset.has_next():
        """ 最终平仓 """
        if maturity1 > end_date:
            close_out_orders = account.creat_close_out_order()
            for order in close_out_orders:
                execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                           execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
                account.add_record(execution_record, account.dict_holding[order.id_instrument])
            account.daily_accounting(optionset.eval_date)
            print(optionset.eval_date, ' close out ')
            print(optionset.eval_date,
                  account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],
                  int(account.cash))
            break
        """ roll """
        # if optionset.eval_date == maturity1:
        if optionset.eval_date > maturity1-datetime.timedelta(days=30):
            close_out_orders = account.creat_close_out_order()
            for order in close_out_orders:
                execution_record = account.dict_holding[order.id_instrument].execute_order(order, slippage=slippage,
                                                                                           execute_type=c.ExecuteType.EXECUTE_ALL_UNITS)
                account.add_record(execution_record, account.dict_holding[order.id_instrument])
            print('roll',optionset.eval_date,maturity1,account.cash)
            maturity1 = optionset.select_maturity_date(nbr_maturity=nbr_maturity, min_holding=min_holding)
            list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness, maturity1)
            if list_atm_put is None:
                list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(0, maturity1)
            atm_put = optionset.select_higher_volume(list_atm_put)
            unit = np.floor(account.cash / atm_put.mktprice_close()/atm_put.multiplier()/5)
            order = account.create_trade_order(atm_put, c.LongShort.LONG, unit)
            record = atm_put.execute_order(order, slippage=slippage)
            account.add_record(record, atm_put)
        account.daily_accounting(optionset.eval_date)
        print(optionset.eval_date,
              account.account.loc[optionset.eval_date, c.Util.PORTFOLIO_NPV],
              int(account.cash))
        optionset.next()

    res.update({'date':list(account.account['dt_date'])})
    res.update({moneyness:list(account.account[c.Util.PORTFOLIO_NPV])})

pu.plot_line_chart(res['date'],[res[0],res[-1]],['npv: buy atm put','npv: buy otm 1 put'])
plt.show()