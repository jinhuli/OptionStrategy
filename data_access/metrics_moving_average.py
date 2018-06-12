import pandas as pd
import datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import numpy as np
from Utilities import admin_util as admin

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

def standard_deviation(df,n):
    std =df['amt_close'].rolling(window = n).std()
    return std

def percentile(df,n,percent):
    return df['amt_close'].rolling(window=n).quantile(percent)

# beg_date = datetime.date(2014, 6, 1)
date = datetime.date(2015, 1, 1)

index_mktdata = admin.table_indexes_mktdata()
ma_metrics = admin.table_moving_average()


# query_mkt = admin.session_mktdata().query(index_mktdata.c.dt_date,index_mktdata.c.id_instrument,
#                        index_mktdata.c.amt_close) \
#     .filter(index_mktdata.c.datasource == 'wind')\
#     .filter(index_mktdata.c.id_instrument == 'index_50etf')\
#     .filter(index_mktdata.c.dt_date >= date)
# df_dataset = pd.read_sql_query(query_mkt.statement,query_mkt.session.bind)

query_mkt = admin.session_metrics().query(ma_metrics.c.dt_date,ma_metrics.c.id_instrument,
                       ma_metrics.c.amt_close) \
    .filter(ma_metrics.c.cd_period == 'ma_3')\
    .filter(ma_metrics.c.id_instrument == 'index_cvix')
    # .filter(ma_metrics.c.dt_date >= date)
df_dataset = pd.read_sql_query(query_mkt.statement,query_mkt.session.bind)

print('s')
# xl = pd.ExcelFile('../data/VIX_daily.xlsx')
# df_dataset = xl.parse("Sheet1", header=None)
# df_dataset['dt_date']= df_dataset[0]
# df_dataset['amt_close']= df_dataset[1]
# df_dataset = df_dataset[['dt_date','amt_close']]
# df_dataset = df_dataset[df_dataset['dt_date']>datetime.date(2017,6,1)]
# df_dataset['id_instrument'] = 'index_cvix'


for p in [0.25,0.5,0.75]:
    # ma = moving_average(df_dataset,n)
    # std = standard_deviation(df_dataset,n)
    pct = percentile(df_dataset,126,p)
    df = df_dataset.copy()
    df['cd_period']='percentileHY_'+str(int(100*p))
    df['cd_calculation'] = 'percentile_HY'
    df['amt_value'] = pct
    df = df.dropna()
    df = df[df['dt_date']>date]
    # df['dt_date'] = df['dt_date'].dt.strftime('%Y-%m-%d')
    print(df)
    df.to_sql(name='moving_average', con=engine_metrics, if_exists = 'append', index=False)




# for (idx,row) in df_dataset.iterrows():
#     if idx <= 20 :
#         df_dataset.ix[idx,'amt_std'] = np.nan
#     else:
#         data = df_dataset.loc[0:idx,'amt_close']
