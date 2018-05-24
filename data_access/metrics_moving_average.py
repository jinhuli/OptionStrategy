import pandas as pd
import datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import numpy as np

def average(df):
    if df.empty: return -999.0
    sum = (df['amt_close'] * df['amt_trading_volume']).sum()
    vol = df['amt_trading_volume'].sum()
    if vol != 0:
        return sum / vol
    return -999.0

def moving_average(df,n):
    ma =df['amt_close'].rolling(window = n).mean()
    return ma

# beg_date = datetime.date(2014, 6, 1)
date = datetime.date(2018, 2, 14)

engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
engine_metrics = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
index_mktdata = Table('indexes_mktdata', metadata, autoload=True)


# query_mkt = sess.query(index_mktdata.c.dt_date,index_mktdata.c.id_instrument,
#                        index_mktdata.c.amt_open,index_mktdata.c.amt_close) \
#     .filter(index_mktdata.c.datasource == 'wind')\
#     .filter(index_mktdata.c.id_instrument == 'index_cvix')\
#     .filter(index_mktdata.c.dt_date >= date)
#
# df_dataset = pd.read_sql_query(query_mkt.statement,query_mkt.session.bind)

xl = pd.ExcelFile('../data/VIX_daily.xlsx')
df_dataset = xl.parse("Sheet1", header=None)
# df_dataset['dt_date'] = df_dataset[0].datetime.strftime('%Y-%m-%d')
df_dataset['dt_date']= df_dataset[0]
df_dataset['amt_close']= df_dataset[1]
df_dataset = df_dataset[['dt_date','amt_close']]
df_dataset = df_dataset[df_dataset['dt_date']>datetime.date(2017,6,1)]
df_dataset['id_instrument'] = 'index_cvix'


for n in [3,5,20,60]:
    ma = moving_average(df_dataset,n)
    df = df_dataset.copy()
    df['cd_period']='ma_'+str(n)
    df['cd_calculation'] = 'simple_average'
    df['amt_ma'] = ma
    df = df.dropna()
    df = df[df['dt_date']>date]
    df['dt_date'] = df['dt_date'].dt.strftime('%Y-%m-%d')
    print(df)
    df.to_sql(name='moving_average', con=engine_metrics, if_exists = 'append', index=False)
