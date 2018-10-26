from data_access.get_data import get_comoption_mktdata
import datetime
import numpy as np
import back_test.model.constant as c
from back_test.model.base_option import BaseOption
from back_test.model.base_account import BaseAccount
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt


dt_start = datetime.date(2018, 9, 21)
dt_end = datetime.date.today()
df_data = get_comoption_mktdata(dt_start, dt_end, c.Util.STR_CU)
m = 0.8
cd_trade_price = c.CdTradePrice.VOLUME_WEIGHTED

df_option1 = df_data[df_data[c.Util.ID_INSTRUMENT] == 'cu_1901_c_51000'].reset_index(drop=True)
df_option2 = df_data[df_data[c.Util.ID_INSTRUMENT] == 'cu_1901_p_47000'].reset_index(drop=True)

baseoption1 = BaseOption(df_data=df_option1, df_daily_data=df_option1, flag_calculate_iv=True)
baseoption2 = BaseOption(df_data=df_option2, df_daily_data=df_option2, flag_calculate_iv=True)
account = BaseAccount(init_fund=10000000, leverage=1.0, rf=0.03)

dt_open = datetime.date(2018, 10, 15)
baseoption1._reprocess_if_genenate_single_option()
baseoption2._reprocess_if_genenate_single_option()
baseoption1.init()
baseoption2.init()


baseoption1.go_to(dt_open)
baseoption2.go_to(dt_open)
long_short = c.LongShort.SHORT
margin_capital1 = baseoption1.get_initial_margin(long_short)
margin_capital2 = baseoption2.get_initial_margin(long_short)
unit_c = np.floor(np.floor(account.portfolio_total_value*m / (margin_capital1+margin_capital2)))
order_c = account.create_trade_order(baseoption1, long_short, unit_c, cd_trade_price=cd_trade_price)
order_p = account.create_trade_order(baseoption2, long_short, unit_c, cd_trade_price=cd_trade_price)
record_call = baseoption1.execute_order(order_c, slippage=0)
record_put = baseoption2.execute_order(order_p, slippage=0)
account.add_record(record_call, baseoption1)
account.add_record(record_put, baseoption2)

while not baseoption1.is_last():
    account.daily_accounting(baseoption1.eval_date)
    baseoption1.next()
    baseoption2.next()

account.account.to_csv('cu_daily_report_npv.csv')
account.trade_records.to_csv('cu_daily_report_trade_records.csv')
dates = list(account.account.index)
npv = list(account.account[c.Util.PORTFOLIO_NPV])
pu = PlotUtil()
pu.plot_line_chart(dates,[npv],['npv'])
plt.show()