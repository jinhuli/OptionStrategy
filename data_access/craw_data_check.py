# encoding: utf-8

import datetime
from WindPy import w
from data_access import spider_api_dce as dce
from data_access import spider_api_sfe as sfe
from data_access import spider_api_czce as czce
from data_access.db_data_collection import DataCollection
from Utilities import admin_write_util as admin

w.start()

conn = admin.conn_mktdata()
conn_intraday = admin.conn_intraday()

options_mktdata_daily = admin.table_options_mktdata()
futures_mktdata_daily = admin.table_futures_mktdata()
futures_institution_positions = admin.table_futures_institution_positions()
option_contracts = admin.table_option_contracts()
future_contracts = admin.table_future_contracts()
index_daily = admin.table_indexes_mktdata()

equity_index_intraday = admin.table_index_mktdata_intraday()
option_mktdata_intraday = admin.table_option_mktdata_intraday()

dc = DataCollection()


beg_date = datetime.date(2015, 1, 1)
end_date = datetime.date.today()

date_range = w.tdays(beg_date, end_date, "").Data[0]
for dt in date_range:
    date = dt.strftime("%Y-%m-%d")
    df = dc.table_future_contracts().get_future_contract_ids(date)
    for (idx_oc, row) in df.iterrows():
        db_data = dc.table_futures().wind_index_future_daily(date, row['id_instrument'], row['windcode'])
        try:
            conn.execute(futures_mktdata_daily.insert(), db_data)
            print(row)
            print('equity index futures -- inserted into data base succefully')
        except Exception as e:
            print(e)
