# encoding: utf-8

import datetime
from WindPy import w
from data_access import spider_api_dce as dce
from data_access import spider_api_sfe as sfe
from data_access import spider_api_czce as czce
from data_access.db_data_collection import DataCollection
from Utilities import admin_write_util as admin

w.start()

date = datetime.date(2018,5,11)

dt_date = date.strftime("%Y-%m-%d")
print(dt_date)

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


# res = futures_mktdata_daily.select((futures_mktdata_daily.c.dt_date == dt_date)
#                                    & (futures_mktdata_daily.c.cd_exchange == 'cfe')).execute()
# if res.rowcount == 0:
df = dc.table_future_contracts().get_future_contract_ids(dt_date)
for (idx_oc, row) in df.iterrows():
    # print(row)
    db_data = dc.table_futures().wind_index_future_daily(dt_date, row['id_instrument'], row['windcode'])
    # print(db_data)
    try:
        conn.execute(futures_mktdata_daily.insert(), db_data)
        print(row)
        print('equity index futures -- inserted into data base succefully')
    except Exception as e:
        print(e)
# else:
#     print('equity index futures -- already exists')