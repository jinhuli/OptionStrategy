from data_access.get_data import get_50option_mktdata
from back_test.model.base_option_set import BaseOptionSet
import datetime

start_date = datetime.date(2017, 11, 18)
end_date = datetime.date(2018, 1, 22)

df_option_metrics = get_50option_mktdata(start_date, end_date)
bkt_optionset = BaseOptionSet(df_option_metrics)
bkt_optionset.init()
while bkt_optionset.has_next():
    bkt_optionset.next()
    d = bkt_optionset.get_maturities_option_dict()
print(bkt_optionset)
