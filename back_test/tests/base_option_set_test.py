import datetime
from data_access.get_data import get_50option_mktdata, get_50option_minute_with_underlying
from back_test.model.base_option_set import BaseOptionSet

start_date = datetime.date(2017, 11, 28)
end_date = datetime.date(2017, 12, 31)
df_option_metrics = get_50option_mktdata(start_date, end_date)
bkt_optionset = BaseOptionSet(df_option_metrics)
bkt_optionset.init()
bkt_optionset.set_date(datetime.date(2017, 12, 5))
bkt_optionset.set_date(datetime.date(2017, 12, 1))
bkt_optionset.set_date(datetime.date(2017, 11, 30))
bkt_optionset.set_date(datetime.date(2017, 12, 7))
print(bkt_optionset)
df_50_metrics = get_50option_minute_with_underlying(start_date, end_date)
etf_optionset = BaseOptionSet(df_50_metrics)
etf_optionset.init()
etf_optionset.set_date(datetime.date(2017,12,5))
etf_optionset.set_date(datetime.date(2017,12,1))
