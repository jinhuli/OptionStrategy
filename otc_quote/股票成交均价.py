# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 09:55:30 2018

@author: DF
"""
import numpy as np
from WindPy import *
import datetime
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker


def obtain_code(stockcode, df, i):
    code = stockcode['标的'][i].astype(str)
    if len(code) == 1:
        code = '00000' + code
    elif len(code) == 2:
        code = '0000' + code
    elif len(code) == 3:
        code = '000' + code
    elif len(code) == 4:
        code = '00' + code
    elif len(code) == 5:
        code = '0' + code
    # elif len(code) == 6:
    code = df[df['code'] == code]['windcode']
    return code


w.start()

engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
table_stocks = Table('stocks', metadata, autoload=True)
start_date = datetime.date(2017, 4, 7)  # 数据起始日期
query_mkt = sess.query(table_stocks.c.windcode)
# Read stock data from mysql database
df = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
df['code'] = df['windcode'].apply(lambda x: x.split('.')[0])

#######################
dtstr = '20180509'
date = '2018-05-09'
url = '../data/'
#######################


stockcodes = pd.read_excel(url + dtstr + '到期标的.xlsx')
stockcode = stockcodes[['标的']].drop_duplicates()
stockcode = stockcode.reset_index(drop=True)
price_m = []
code_list = []
for i in range(len(stockcode)):
    code = obtain_code(stockcode, df, i)
    code = list(code)[0]
    code_list.append(code)
    data = w.wsi(code, "amt,volume", date + " 09:30:00", date + " 15:00:00", "")
    print(data)
    dataset = pd.DataFrame(np.array([data.Times, data.Data[0], data.Data[1]]).T, columns=['timestamp', 'amt', 'volume'])
    dataset = dataset[dataset['volume'] > 0]
    meanprice = dataset['amt'].div(dataset['volume']).mean()
    price_m.append(round(meanprice, 2))

result = pd.DataFrame(columns=['股票代码', '成交均价'])
result['股票代码'] = code_list
result['成交均价'] = price_m
result['标的'] = stockcodes['标的'].drop_duplicates().reset_index(drop=True)

output_data = pd.merge(stockcodes, result, on='标的')
output_data.to_excel(url + dtstr + '全天成交均价.xlsx', 'data')  ####
