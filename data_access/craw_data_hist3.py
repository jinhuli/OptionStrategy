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
Option data from Wind
"""

w.start()

conn = admin.conn_gc()
conn_intraday = admin.conn_intraday()

options_mktdata_daily = admin.table_options_mktdata_gc()
option_contracts = admin.table_option_contracts()

dc = DataCollection()
today = datetime.date.today()
# beg_date = datetime.date(2015, 1, 1)
beg_date = datetime.date(2017, 1, 1).strftime("%Y-%m-%d")
end_date = datetime.date.today().strftime("%Y-%m-%d")

date_range = w.tdays(beg_date, end_date, "").Data[0]
date_range = sorted(date_range, reverse=True)
# dt_date = dt.strftime("%Y-%m-%d")

# code_list = [
#     # 'M1707',
#     # 'M1708',
#     # 'M1709',
#     # 'M1711',
#     # 'M1712',
#     # 'M1801',
#     'M1803',
#     'M1805',
#     'M1807',
#     'M1808',
#     'M1809',
#     'M1811',
#     'M1812',
#     'M1901',
#     'M1903',
#     'M1905',
#     'M1907',
#     'M1908',
#     'M1909'
# ]
# for code in code_list:
#     db_data = dc.table_options().wind_data_m_option(beg_date, end_date, code+'.DCE')
#     if len(db_data) == 0: print('no data')
#     try:
#         conn.execute(options_mktdata_daily.insert(), db_data)
#         print('wind m option -- inserted into data base succefully')
#     except Exception as e:
#         print(e)

code_list = [
    # 'SR707',
    'SR709',
    'SR711',
    'SR801',
    'SR803',
    'SR805',
    'SR807',
    'SR809',
    'SR811',
    'SR901',
    'SR903',
    'SR905',
    'SR907',
    'SR909',

]

for code in code_list:
    db_data = dc.table_options().wind_data_sr_option(beg_date, end_date, code+'.CZC')
    if len(db_data) == 0: print('no data')
    try:
        conn.execute(options_mktdata_daily.insert(), db_data)
        print('wind m option -- inserted into data base succefully')
    except Exception as e:
        print(e)