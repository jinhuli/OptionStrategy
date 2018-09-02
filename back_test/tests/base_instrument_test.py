from back_test.model.base_instrument import BaseInstrument
import datetime
from data_access.get_data import get_index_intraday
from back_test.model.constant import FrequentType

"""Back Test Settings"""
# start_date = datetime.date(2015, 3, 1)
start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2017, 11, 1)

"""Collect Mkt Date"""

df_index_metrics = get_index_intraday(start_date, end_date, 'index_50etf')

a = BaseInstrument(df_index_metrics, frequency=FrequentType.MINUTE)
a.init()
a.set_date(datetime.date(2017,10,10))
a.set_date(datetime.date(2017,1,1))
print(a)
