import pandas as pd
import QuantLib as ql
import datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from data_access.db_tables import DataBaseTables as dbt
from back_test.data_option import get_sr_option_mktdata

start_date = datetime.date(2018,1,1)
end_date = datetime.date(2018,2,22)

# df_option_metrics = get_sr_option_mktdata(start_date,end_date)
# print(df_option_metrics)


name = '50etf_1'

print(name[0:name.index('_')])





