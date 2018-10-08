# encoding: utf-8

import datetime
from WindPy import w
from data_access import spider_api_dce as dce
from data_access import spider_api_sfe as sfe
from data_access import spider_api_czce as czce
from data_access.db_data_collection import DataCollection
from Utilities import admin_write_util as admin
import pandas as pd
import numpy as np

"""
CU ： 沪铜期货
wind data
"""

w.start()

conn = admin.conn_mktdata()
conn_intraday = admin.conn_intraday()

futures_mktdata_daily = admin.table_futures_mktdata()


def wind_future_daily(dt,contracts):
    datestr = dt.strftime("%Y-%m-%d")
    try:
        res = w.wss(contracts,"pre_close,open,high,low,close,volume,amt,oi,pre_settle,settle,windcode","tradeDate="+datestr+";priceAdj=U;cycle=D")
        d = res.Data
        f = res.Fields
        df = pd.DataFrame(data=np.transpose(d), columns=f,)
        df1 = df.dropna(subset=['CLOSE'])
        df1['id_instrument'] = df1['WINDCODE'].apply(lambda x:(x[-len(x):-8]+'_'+x[-8:-4]).lower())
        df1['name_code'] = df1['WINDCODE'].apply(lambda x:x[-len(x):-8].lower())
        df1['cd_exchange'] = df1['WINDCODE'].apply(lambda x:x[-3:].lower())
        df1.loc[:,'datasource'] = 'wind'
        df1.loc[:,'timestamp'] = datetime.datetime.today()
        df1.loc[:,'dt_date'] = dt
        df1=df1.rename(columns={'PRE_CLOSE':'amt_last_close',
                            'OPEN':'amt_open',
                            'HIGH':'amt_high',
                           'LOW':'amt_low',
                           'CLOSE':'amt_close',
                           'VOLUME':'amt_trading_volume',
                           'AMT':'amt_trading_value',
                           'OI':'amt_holding_volume',
                           'PRE_SETTLE':'amt_last_settlement',
                           'SETTLE':'amt_settlement',
                           'WINDCODE':'code_instrument'
        })
        return df1
    except Exception as e:
        print(e)
        return pd.DataFrame()



today = datetime.date.today()
beg_date = datetime.date(2018, 9, 10)
# end_date = datetime.date(2018, 9, 14)
end_date = datetime.date.today()

# data_contracts = w.wset("futurecc","startdate=2010-01-01;enddate="+end_date.strftime("%Y-%m-%d")+";wind_code=CU.SHF;field=wind_code,contract_issue_date,last_trade_date,last_delivery_mouth")
data_contracts = w.wset("futurecc","startdate=2017-09-15;enddate=2018-09-15;wind_code=CU.SHF")
df_contracts = pd.DataFrame(data=np.transpose(data_contracts.Data), columns=data_contracts.Fields)
date_range = w.tdays(beg_date, end_date, "").Data[0]
date_range = sorted(date_range,reverse=True)
for dt in date_range:
    c_str = ""
    contracts = df_contracts[(df_contracts['contract_issue_date'] <=dt)&(df_contracts['last_delivery_month'] >=dt)]['wind_code'].values
    for c in contracts:
        c_str += c +","
    c_str = c_str[0:len(c_str)-2]
    # c_str += '\"'
    df1 = wind_future_daily(dt,c_str)
    try:
        df1.to_sql('futures_mktdata', con=admin.engine_gc, if_exists='append', index=False)
        print(dt, ' finished.')
    except Exception as e:
        print(e)
        pass
