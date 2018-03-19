import pandas as pd
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
from scipy.optimize import minimize
import numpy as np



engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata?charset=utf8mb4', echo=False)
conn = engine.connect()
metadata = MetaData(engine)
events = Table('events', metadata, autoload=True)

location = 'C:/Users/wangd/Desktop/事件波动率策略/A股重大事件_DB.xlsx'
df = pd.read_excel(location)

df = df.rename(columns={"dt_date": "dt_date_1", "dt_first_trading": "dt_first_trading_1"})
for (idx, r) in df.iterrows():
    df.loc[idx,'dt_date'] = r['dt_date_1'].strftime('%Y-%m-%d')
    df.loc[idx,'dt_first_trading'] = r['dt_first_trading_1'].strftime('%Y-%m-%d')
    if pd.isnull(r['dt_time']):
        if df.loc[idx,'dt_date'] < df.loc[idx,'dt_first_trading']: df.loc[idx,'dt_time'] = datetime.time(23,59,0)
        else: df.loc[idx,'dt_time'] = datetime.time(0,0,0)
df_event = df.drop(['dt_date_1','dt_first_trading_1'], axis=1)

print(df_event)

# db_data = []
# for (idx,r) in df_event.iterrows():
#     cols = df_event.columns.values
#     dic = {}
#     for c in cols:
#         if c == 'id_event' :continue
#         v = r[c]
#         if type(v) == str:
#             v.encode('utf-8')
#         if pd.isnull(v): v = None
#         dic.update({c:v})
#     db_data.append(dic)
#     print(dic)
#
# print(df)
# conn.execute(events.insert(), db_data)

def lik(parameters):
    m = parameters[0]
    b = parameters[1]
    sigma = parameters[2]
    for i in np.arange(0, len(x)):
        y_exp = m * x + b
    L = (len(x)/2 * np.log(2 * np.pi) + len(x)/2 * np.log(sigma ** 2) + 1 /
         (2 * sigma ** 2) * sum((y - y_exp) ** 2))
    return L



def derm(alpha,delta,beta,miu,sigma,yt,yt1,dt):
    yt - (alpha+delta*yt1+)
    return None

lik_model = minimize(lik, np.array([1,1,1]), method='L-BFGS-B')

