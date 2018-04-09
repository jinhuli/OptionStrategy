# encoding: utf-8

from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
import datetime
from WindPy import w
from data_access import spider_api_dce as dce
from data_access import spider_api_sfe as sfe
from data_access import spider_api_czce as czce
from data_access.db_data_collection import DataCollection
from back_test.data_option import get_50option_mktdata
from back_test.bkt_option_set import BktOptionSet

w.start()

# date = datetime.date(2018, 4, 4)
# dt_date = date.strftime("%Y-%m-%d")
# print(dt_date)

engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata', echo=False)
conn = engine.connect()
metadata = MetaData(engine)
stocks = Table('stocks', metadata, autoload=True)
stocks_mktdata = Table('stocks_mktdata', metadata, autoload=True)

dc = DataCollection()

beg_date = datetime.date(2017, 10, 27)
end_date = datetime.date(2018, 1, 9)

date_range = w.tdays(beg_date, end_date, "").Data[0]
#####################CONTRACT INFO#########################################
# option_contracts

# db_datas = dc.table_stocks().wind_A_shares_total(dt_date)
# for db_data in db_datas:
#     try:
#         conn.execute(stocks.insert(), db_data)
#     except Exception as e:
#         print(e)
#         continue

for dt in date_range:
    print(dt)
    df_A_shares = dc.table_stocks().get_A_shares_total()
    for (idx,row) in df_A_shares.iterrows():
        windcode = row['windcode']
        res = stocks_mktdata.select((stocks_mktdata.c.dt_date == dt) &
                                    (stocks_mktdata.c.code_instrument == windcode)).execute()
        if res.rowcount == 0:
            db_datas = dc.table_stocks().wind_stocks_daily(dt,windcode)
            for db_data in db_datas:
                try:
                    conn.execute(stocks_mktdata.insert(), db_data)
                except Exception as e:
                    print(e)
                    continue

