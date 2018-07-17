from back_test.model.option_account import OptionAccount
from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_instrument import BaseInstrument
from back_test.model.base_future_coutinuous import BaseFutureCoutinuous
from back_test.model.base_account import BaseAccount
from data_access.get_data import get_50option_mktdata, get_index_mktdata,get_dzqh_cf_minute
import datetime
from back_test.model.trade import Order
from back_test.model.constant import TradeType

start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2017, 11, 21)

# df_option_metrics = get_50option_mktdata(start_date, end_date)
# df_index_metrics = get_index_mktdata(start_date, end_date, 'index_50etf')
# option_set = BaseOptionSet(df_option_metrics)
# option_set.init()
# index = BaseInstrument(df_index_metrics)
df_cf_minute = get_dzqh_cf_minute(start_date,end_date,'if')
future = BaseFutureCoutinuous(df_cf_minute)
future.init()
account = BaseAccount(100000.0)
while future.has_next():
    # for option in option_set.eligible_options:
    execution_record = future.execute_order(Order(future.eval_date,
                               future.id_instrument(),
                                TradeType.OPEN_LONG,
                                1,
                               future.mktprice_close(),
                               future.eval_datetime)
                             )
    account.add_record(execution_record)
    future.next()




