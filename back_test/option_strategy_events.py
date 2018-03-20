import pandas as pd
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from scipy.optimize import minimize
import numpy as np
import math

class option_strategy_events(object):

    def __init__(self,df_events,df_vix):
        self.df_events = df_events
        self.df_vix = df_vix
        self.dt_list = sorted(df_vix['dt_date'].unique())
        self.event_ids = list(df_events['id_event'])
        self.nbr_events = len(self.event_ids)

    # Analyze Event Influence Significance
    def add_nbr_days_from_events(self):
        for (idx_e,r_e) in self.df_events.iterrows():
            dt_event = r_e['dt_first_trading']
            id_event = r_e['id_event']
            self.df_vix[id_event] = self.calculate_nbr_days(self.df_vix['dt_date'], dt_event)

    def calculate_nbr_days(self,dt_list,dt_event):
        nbr_days = []
        for dt in dt_list:
            idx_dt = self.dt_list.index(dt)
            idx_evt = self.dt_list.index(dt_event)
            nbr = idx_dt-idx_evt
            nbr_days.append(nbr)
        return nbr_days

    def func_derm(self,params):
        a = params[0]
        d = params[1]
        beta_list = params[2:2+self.nbr_events]
        mu_list = params[2+self.nbr_events:2+self.nbr_events*2]
        sigma_list = params[2+self.nbr_events*2:]
        obj = 0
        for (idx_vix,r_vix) in self.df_vix.iterrows():
            if idx_vix == 0 : continue
            norm = 0
            for (idx_e, r_e) in self.df_events.iterrows():
                event_id = r_e['id_event']
                dt = r_vix[event_id]
                norm += beta_list[idx_e] * self.normal_distribution(dt,mu_list[idx_e], sigma_list[idx_e])
            y_t = r_vix['amt_close']
            y_t1 = self.df_vix.loc[idx_vix-1,'amt_close']
            obj_t = - y_t + a + d*y_t1 + norm
            obj += obj_t
        return obj

    def normal_distribution(self,dt,mu,s):
        return math.exp(-(dt-mu)**2/(2*s**2))/(s*math.sqrt(2*math.pi))

    def optimization(self):
        init_params = np.zeros(3*self.nbr_events+2)
        res = minimize(self.func_derm, init_params, method='Nelder-Mead', tol=1e-6)
        print(res)

engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata?charset=utf8mb4', echo=False)
conn = engine.connect()
metadata = MetaData(engine)
Session = sessionmaker(bind=engine)
sess = Session()
events = Table('events', metadata, autoload=True)
indexes = Table('indexes_mktdata', metadata, autoload=True)
query_events = sess.query(events)
query_vix = sess.query(indexes.c.dt_date,indexes.c.id_instrument,indexes.c.amt_close)\
    .filter(indexes.c.id_instrument == 'index_cvix').filter(indexes.c.dt_date > '2015-3-1')
df_events = pd.read_sql(query_events.statement,query_events.session.bind)
df_vix = pd.read_sql(query_vix.statement,query_vix.session.bind)

s = option_strategy_events(df_events,df_vix)
s.add_nbr_days_from_events()
s.optimization()
# print(s.df_vix)
print('')