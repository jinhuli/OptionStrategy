import datetime
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
table_stocks = Table('stocks_mktdata', metadata, autoload=True)

start_date = datetime.date(2018, 4, 6)
end_date = datetime.date(2018, 4, 6)
query_mkt = sess.query(table_stocks) \
    .filter(table_stocks.c.dt_date >= start_date).filter(table_stocks.c.dt_date <= end_date)



df = pd.read_excel('../data/A-160701-180411-close.xlsx')

print(df)

codes = []
for col in df:
    codes.append(col)

stock_yields = {}
for code in codes[1:3]:
    yields = []
    for (i,row) in df.iterrows():
        if i==0:continue
        r = (row[code]-df.loc[i-1,code])/df.loc[i-1,code]
        yields.append(r)
    stock_yields.update({code:yields})

df_yields = pd.DataFrame(stock_yields)