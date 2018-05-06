# encoding: utf-8

from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
import datetime
from WindPy import w
from data_access.db_data_collection import DataCollection
from sqlalchemy.orm import sessionmaker
import pandas as pd
from back_test.data_option import get_50option_mktdata
from back_test.bkt_option_set import BktOptionSet


w.start()
engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
options_mktdata = Table('options_mktdata', metadata, autoload=True)

engine_intraday = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata_intraday', echo=False)
conn_intraday = engine_intraday.connect()
Session_intraday = sessionmaker(bind=engine_intraday)
sess_intraday = Session_intraday()
metadata_intraday = MetaData(engine_intraday)
option_mktdata_intraday = Table('option_mktdata_intraday', metadata_intraday, autoload=True)

engine_metrics = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
Session_metrics = sessionmaker(bind=engine_metrics)
sess_metrics = Session_metrics()
metadata_metrics = MetaData(engine_metrics)
options_golden = Table('options_mktdata_goldencopy', metadata_metrics, autoload=True)

dc = DataCollection()

beg_date = datetime.date(2016, 11, 1)
end_date = datetime.date(2016, 11, 28)

############################################# OPTION MKT INTRADAY #############################################


query_mkt = sess_metrics.query(options_golden) \
    .filter(options_golden.c.amt_daily_avg == -999.0)

dataset = pd.read_sql_query(query_mkt.statement,query_mkt.session.bind)
dates = dataset['dt_date'].unique()

for date in dates:
    df = dataset[dataset['dt_date'] == date]

    for index, row in df.iterrows():
        cur = row['dt_date']
        next_day = row['dt_date'] + datetime.timedelta(days=1)
        id = row['id_instrument']
        windcode = row['code_instrument']
        dt_date = date.strftime("%Y-%m-%d")
        db_data = dc.table_option_intraday().wind_data_50etf_option_intraday2(dt_date, windcode,id)

        if len(db_data)>0:
            try:
                option_mktdata_intraday.delete(
                    (option_mktdata_intraday.c.dt_datetime >= dt_date + " 09:25:00")&
                    (option_mktdata_intraday.c.dt_datetime <= dt_date + " 15:00:00")&
                    (option_mktdata_intraday.c.id_instrument == id)).execute()
            except Exception as e:
                print(e)
                pass
            try:
                conn_intraday.execute(option_mktdata_intraday.insert(), db_data)
                print(date,' -- option_mktdata_intraday -- inserted into data base succefully')
            except Exception as e:
                print(e)
        else:
            print('wind collect data failed')
