import datetime
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import numpy as np
import QuantLib as ql


# TODO: CHANGE URL
url = '../data/'
# TODO : CHANGE TO TEH LAST TEADING DAY
end_date = datetime.date(2018, 4, 20)
evaluation_date = end_date


engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
table_stocks = Table('stocks', metadata, autoload=True)


start_date = datetime.date(2017, 4, 7) # 数据起始日期

query_mkt = sess.query(table_stocks.c.windcode)

# Read stock data from mysql database
df = pd.read_sql(query_mkt.statement,query_mkt.session.bind)
df['code'] = df['windcode'].apply(lambda x:x.split('.')[0])
print(df)
# # read stock codes to quotes
# df_codes = pd.read_excel(url+'20180411_东证润和_期权卖方报价.xls',converters={'股票代码': lambda x: str(x)})
# code_stocks = df_codes['股票代码'].unique() # 所有待报价的股票代码