# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 09:55:30 2018

@author: DF
"""
import pandas as pd 
from pandas import Series, DataFrame 
import numpy as np
import statsmodels.api as sm 
import scipy.stats as scs 
import matplotlib.pyplot as plt
from statsmodels import regression
import statsmodels.api as sm
import datetime
from WindPy import *
import datetime
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
w.start()

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

#####上午成交均价
stockcodes=pd.read_excel('4_27.xlsx','data')####
stockcode=stockcodes[['标的名称']].drop_duplicates()
stockcode=stockcode.reset_index(drop=True)
price_am=[]
price_pm=[]
code_list=[]
for i in range(len(stockcode)):
    
	code= obtain_code(stockcode,df,i)
	code=list(code)[0]
	code_list.append(code)
	#code='601888.SH'
	data=w.wsi(code, "amt,volume", "2018-04-27 11:00:00", "2018-04-27 11:30:00", "")#####
	dataset=pd.DataFrame(np.array([data.Times, data.Data[0],data.Data[1]]).T, columns=['timestamp', 'amt','volume'])
	dataset=dataset[dataset['volume']>0]
	meanprice=dataset['amt'].div(dataset['volume']).mean()
	print('上午成交均价',round(meanprice,2))
	price_am.append(round(meanprice,2))
    
result=pd.DataFrame(columns=['股票代码','上午成交均价'])
result['股票代码'] =code_list
result['上午成交均价'] =price_am
result['标的名称']=stockcodes['标的名称'].drop_duplicates().reset_index(drop=True)

output_data=pd.merge(stockcodes, result, on = '标的名称')
output_data.to_excel('4-27上午成交均价1.xlsx','data')####
####上午成交均价


####下午成交均价
stockcodes=pd.read_excel('广州冠盛_卖_交易指令_2018年4月27日下午.xlsx','data')####
stockcode=stockcodes[['标的名称']].drop_duplicates()
stockcode=stockcode.reset_index(drop=True)
price_pm=[]
code_list=[]
for i in range(len(stockcode)):
    
	code= obtain_code(stockcode,df,i)
	code=list(code)[0]
	code_list.append(code)
	#code='600050.SH'
	data1=w.wsi(code, "amt,volume", "2018-04-27 14:30:00", "2018-04-27 14:57:00", "")######
	dataset1=pd.DataFrame(np.array([data1.Times, data1.Data[0],data1.Data[1]]).T, columns=['timestamp', 'amt','volume'])
	#meanprice1=dataset1['close'].mul(dataset1['volume']).sum()/dataset1['volume'].sum()
	dataset1=dataset1[dataset1['volume']>0]
	meanprice1=dataset1['amt'].div(dataset1['volume']).mean()
	print('下午成交均价',round(meanprice1,2))
	price_pm.append(round(meanprice1,2))
    
result=pd.DataFrame(columns=['股票代码','下午成交均价'])
result['股票代码'] =code_list
result['下午成交均价'] =price_pm
result['标的名称']=stockcodes['标的名称'].drop_duplicates().reset_index(drop=True)

output_data=pd.merge(stockcodes, result, on = '标的名称')
output_data.to_excel('4-27下午成交均价.xlsx','data')#####
####下午成交均价


def obtain_code(stockcode,df,i):
	
	code=stockcode['标的名称'][i].astype(str)
	if len(code)==1:
	    code='00000'+code
	elif len(code)==2:
	    code='0000'+code
	elif len(code)==3:
	    code='000'+code
	elif len(code)==4:
	    code='00'+code
	elif len(code)==5:
	    code='0'+code
	elif len(code)==6:
	code=df[df['code']==code]['windcode']
	return code