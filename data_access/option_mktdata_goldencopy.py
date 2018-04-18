import pandas as pd
import numpy as np
import pymysql
import datetime
from sqlalchemy import create_engine
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker


username = 'root'
password = 'liz1128'
host = '101.132.148.152'


def average(df):
    sum = (df['amt_close'] * df['amt_trading_volume']).sum()
    vol = df['amt_trading_volume'].sum()
    if vol != 0:
        return sum / vol
    return -999.0


# mktdataEngine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata')
mktdataEngine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username, password, host, 'mktdata'))
mktdataIntradayEngine = create_engine(
    'mysql+pymysql://{}:{}@{}/{}'.format(username, password, host, 'mktdata_intraday'))
# cnt = 0
# date = datetime.date(2015, 3, 1)

beg_date = datetime.date(2018, 3, 8)
end_date = datetime.date(2018, 3, 10)

engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
engine_metrics = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
options_mktdata = Table('options_mktdata', metadata, autoload=True)

query_mkt = sess.query(options_mktdata) \
    .filter(options_mktdata.c.datasource == 'wind')\
    .filter(options_mktdata.c.id_underlying == 'index_50etf')\
    .filter(options_mktdata.c.dt_date >= beg_date).filter(options_mktdata.c.dt_date <= end_date)


dataset = pd.read_sql_query(query_mkt.statement,query_mkt.session.bind)
# print(dataset)
dates = dataset['dt_date'].unique()
for date in dates:
    df = dataset[dataset['dt_date'] == date]

    for index, row in df.iterrows():
        cur = row['dt_date']
        next = row['dt_date'] + datetime.timedelta(days=1)
        id = row['id_instrument']
        daily_df = pd.read_sql_query(
            'SELECT * FROM option_mktdata_intraday where id_instrument=\'{}\' and dt_datetime>=\'{}\' and dt_datetime<\'{}\''.format(
                id, cur, next), mktdataIntradayEngine)
        if len(daily_df) != 242:
            print(cur,id,len(daily_df))
            continue
        df_morning_open_15min = daily_df.iloc[1:17, :]
        df_morning_close_15min = daily_df.iloc[106:121, :]
        df_afternoon_open_15min = daily_df.iloc[-121:-105, :]
        df_afternoon_close_15min = daily_df.iloc[-16:, :]
        df_morning = daily_df.iloc[1:121, :]
        df_afternoon = daily_df.iloc[-121:, :]

        amt_morning_open_15min = average(df_morning_open_15min)
        amt_morning_close_15min = average(df_morning_close_15min)
        amt_afternoon_open_15min = average(df_afternoon_open_15min)
        amt_afternoon_close_15min = average(df_afternoon_close_15min)
        amt_daily_avg = average(daily_df)
        amt_morning_avg = average(df_morning)
        amt_afternoon_avg = average(df_afternoon)
        df.loc[index,'amt_morning_open_15min'] = amt_morning_open_15min
        df.loc[index,'amt_morning_close_15min'] = amt_morning_close_15min
        df.loc[index,'amt_afternoon_open_15min'] = amt_afternoon_open_15min
        df.loc[index,'amt_afternoon_close_15min'] = amt_afternoon_close_15min
        df.loc[index,'amt_daily_avg'] = amt_daily_avg
        df.loc[index,'amt_morning_avg'] = amt_morning_avg
        df.loc[index,'amt_afternoon_avg'] = amt_afternoon_avg
        # print(df.loc[index])
        if row['amt_open'] == -999 or row['amt_close'] == -999:
            if row['amt_settlement'] == -999.0:
                print(row)
                print('No settlement and close data, No can do !!!!!')
            else:
                df.loc[index,'amt_open'] = row['amt_settlement']
                df.loc[index,'amt_close'] = row['amt_settlement']
                df.loc[index,'cd_remark'] = 'no trading volume'

    for r in df.iterrows():
        try:
            r.to_sql('options_mktdata_goldencopy', engine_metrics, if_exists='append',index=False)
        except:
            continue
    print(date,'inserted into database')
