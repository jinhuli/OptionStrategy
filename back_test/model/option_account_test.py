from back_test.model.option_account import OptionAccount
from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_instrument import BaseInstrument
from data_access.get_data import get_50option_mktdata, get_index_mktdata
import datetime


start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2018, 1, 21)

df_option_metrics = get_50option_mktdata(start_date, end_date)
df_index_metrics = get_index_mktdata(start_date, end_date, 'index_50etf')
option_set = BaseOptionSet(df_option_metrics)
option_set.init()
index = BaseInstrument(df_index_metrics)
account = OptionAccount(fee_rate=2.0/10000, rf=0.03)