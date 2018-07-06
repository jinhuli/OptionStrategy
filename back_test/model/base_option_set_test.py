from data_access.get_data import get_50option_mktdata
from back_test.model.base_option_set import BaseOptionSet
import datetime

start_date = datetime.date(2018,4,18)
end_date = datetime.date(2018,6,22)

df_option_metrics = get_50option_mktdata(start_date,end_date)
bkt_optionset = BaseOptionSet(df_option_metrics)
bkt_optionset.init()
bkt_optionset.next()
print(bkt_optionset)