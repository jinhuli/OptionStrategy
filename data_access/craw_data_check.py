# encoding: utf-8

from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
import datetime
from WindPy import w
from data_access.db_data_collection import DataCollection
from sqlalchemy.orm import sessionmaker
import pandas as pd


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

# engine_metrics = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
# Session_metrics = sessionmaker(bind=engine_metrics)
# sess_metrics = Session_metrics()
# metadata_metrics = MetaData(engine_metrics)
# options_golden = Table('options_mktdata_goldencopy', metadata_metrics, autoload=True)

dc = DataCollection()
miss_data = pd.read_csv('../data/data_validation.csv')

miss_data['name_code'] = miss_data['id'].astype(str).apply(lambda x:x.split('_')[0])
miss_data = miss_data[miss_data['name_code']=='50etf'].reset_index(drop=True)
miss_data.to_csv('../data/miss_data.csv')
print(len(miss_data))
miss_data = miss_data.drop_duplicates(['date','id'])
print(len(miss_data))
succeeded = []
# dates = ['2016-01-18']

# miss_data = {
#     '2016-01-18':['50etf_1602_p_1.95', '50etf_1606_c_1.95', '50etf_1606_p_1.95'],
#     '2016-01-27':['50etf_1601_c_1.85','50etf_1601_p_1.85','50etf_1606_c_1.9','50etf_1606_p_1.9'],
#              }
df = dc.table_options().get_option_contracts_all()
for (i,row) in miss_data.iterrows():
    dt_date = row['date']
    id_instrument = row['id']
    dt_datetime = row['datetime']
    row['windcode'] = df[df['id_instrument']==id_instrument]['windcode'].values[0]
    row['id_instrument'] = id_instrument
    print(dt_date, dt_datetime, id_instrument, ' -- data missed')
    db_data_list = dc.table_option_intraday().wind_data_50etf_option_intraday(dt_date, row)
    for db_data in db_data_list:
        try:
            conn_intraday.execute(option_mktdata_intraday.insert(), db_data)
            print(dt_date ,db_data['dt_datetime'], id_instrument,' -- inserted into data base succefully')
            succeeded.append({'dt_date':dt_date,'dt_datetime':db_data['dt_datetime'],'id_instrument':id_instrument})
        except Exception as e:
            # print(e)
            pass

df_succeeded = pd.DataFrame(succeeded)
df_succeeded.to_csv('../data/succeeded.csv')
print(df_succeeded)
