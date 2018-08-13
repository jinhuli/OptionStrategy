# encoding: utf-8

from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
import datetime
import pandas as pd
from WindPy import w
import os
from data_access import db_utilities as du
from data_access import spider_api_dce as dce
from data_access import spider_api_sfe as sfe
from data_access import spider_api_czce as czce
from data_access.db_data_collection import DataCollection
from Utilities import admin_write_util as admin

w.start()
dc = DataCollection()
conn = admin.conn_intraday()
table_option_intraday = admin.table_option_mktdata_intraday()
table_option = admin.table_option_contracts()

date = datetime.date(2018,7,18)
dt_date = date.strftime("%Y-%m-%d")

df = dc.table_options().get_option_contracts(dt_date,'m_1809')
for (idx_oc, row) in df.iterrows():
    db_data = dc.table_option_intraday().wind_data_50etf_option_intraday(dt_date, row)
    try:
        conn.execute(table_option_intraday.insert(), db_data)
        print('option_mktdata_intraday -- inserted into data base succefully')
    except Exception as e:
        print(e)
