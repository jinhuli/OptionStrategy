from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_instrument import BaseInstrument
from back_test.model.base_future_coutinuous import BaseFutureCoutinuous
from back_test.model.base_account import BaseAccount
from data_access.get_data import get_50option_mktdata, \
    get_50option_minute_with_underlying, get_index_mktdata, \
    get_dzqh_cf_minute,get_dzqh_cf_daily,get_index_intraday
import datetime
from back_test.model.trade import Order, Trade
from back_test.model.constant import TradeType, Util, FrequentType

start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2017, 10, 10)


df_option = get_50option_mktdata(start_date, end_date)
df_option_intraday = get_50option_minute_with_underlying(start_date, end_date)
df_index = get_index_intraday(start_date, end_date, 'index_50etf')
# print(df_option.iloc[0])
# print(df_option_intraday.iloc[0])
# print(df_option.iloc[-1])
# print(df_option_intraday.iloc[-1])
option_set = BaseOptionSet(df_data=df_option_intraday, df_daily_data=df_option, df_underlying=df_index,
                           frequency=FrequentType.MINUTE)
option_set.init()
option_set.next()

print(len(option_set.eligible_options))
while option_set.has_next():
    p = option_set.get_put_by_moneyness(0)
    print()


