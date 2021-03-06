import datetime

from back_test.model.base_account import BaseAccount
from back_test.model.base_future_coutinuous import BaseFutureCoutinuous
from back_test.model.constant import TradeType, Util, FrequentType
from back_test.model.trade import Trade
from data_access.get_data import get_dzqh_cf_minute, get_dzqh_cf_daily

start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2017, 11, 21)

# df_option_metrics = get_50option_mktdata(start_date, end_date)
# df_index_metrics = get_index_mktdata(start_date, end_date, 'index_50etf')
# option_set = BaseOptionSet(df_option_metrics)
# option_set.init()
# index = BaseInstrument(df_index_metrics)
df_cf_minute = get_dzqh_cf_minute(start_date, end_date, 'if')
df_cf = get_dzqh_cf_daily(start_date, end_date, 'if')
future = BaseFutureCoutinuous(df_cf_minute, df_cf, frequency=FrequentType.MINUTE)
future.init()
account = BaseAccount(Util.BILLION)
trading_desk = Trade()
# while future.has_next():
# for option in option_set.eligible_options:
# TODO: Create and execute order could be implemented in base_product class.
order = account.create_trade_order(future,
                                   TradeType.OPEN_LONG,
                                   10)

execution_res = future.execute_order(order)
account.add_record(execution_res, future)
trading_desk.add_pending_order(order)
future.next()
order = account.create_trade_order(future,
                                   TradeType.OPEN_SHORT,
                                   5)
execution_res = future.execute_order(order)
account.add_record(execution_res, future)
trading_desk.add_pending_order(order)
future.next()
order = account.create_trade_order(future,
                                   TradeType.OPEN_SHORT,
                                   10)
execution_res = future.execute_order(order)
account.add_record(execution_res, future)
trading_desk.add_pending_order(order)
future.next()

account.daily_accounting(future.eval_date)

order = account.create_trade_order(future,
                                   TradeType.CLOSE_SHORT,
                                   )
execution_res = future.execute_order(order)
account.add_record(execution_res, future)
trading_desk.add_pending_order(order)
