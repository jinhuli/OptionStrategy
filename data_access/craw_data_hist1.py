# encoding: utf-8

import datetime
from WindPy import w
from data_access import spider_api_dce as dce
from data_access import spider_api_sfe as sfe
from data_access import spider_api_czce as czce
from data_access.db_data_collection import DataCollection
from Utilities import admin_write_util as admin
import pandas as pd
import numpy as np

"""
SH300 TR
wind data
"""

w.start()

conn = admin.conn_mktdata()
conn_intraday = admin.conn_intraday()

index_daily = admin.table_indexes_mktdata()


dc = DataCollection()


today = datetime.date.today()
beg_date = datetime.date(2015, 1, 1)
# beg_date = datetime.date(2018, 9, 1)
end_date = datetime.date.today()

date_range = w.tdays(beg_date, end_date, "").Data[0]
date_range = sorted(date_range,reverse=True)
for dt in date_range:
    windcode = "H00300.CSI"
    id_instrument = 'index_300sh_total_return'
    datestr = dt.strftime("%Y-%m-%d")
    db_data = dc.table_index().wind_data_index(windcode, datestr, id_instrument)
    # print(db_data)
    try:
        conn.execute(index_daily.insert(), db_data)
        print('index_300sh_total_return -- inserted into data base succefully')
    except Exception as e:
        print(e)

