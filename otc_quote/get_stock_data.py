import datetime
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import numpy as np


engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
table_stocks = Table('stocks_mktdata', metadata, autoload=True)

start_date = datetime.date(2017, 4, 6)
end_date = datetime.date(2018, 4, 6)
query_mkt = sess.query(table_stocks.c.dt_date,table_stocks.c.code_instrument,table_stocks.c.amt_close) \
    .filter(table_stocks.c.dt_date >= start_date).filter(table_stocks.c.dt_date <= end_date)



df = pd.read_sql(query_mkt.statement,query_mkt.session.bind).set_index('dt_date')

print(df)


def hisvol(data,n):
    datas=np.log(data)
    df=datas.diff()
    vol=df.rolling(window = n).std()*np.sqrt(252)
    return vol

data = df[df['code_instrument']=='000026.SZ']['amt_close']

print(data)

vol = hisvol(data,22).shift(1)


print(vol.iloc[-1])