import pandas as pd
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from scipy.optimize import minimize
from matplotlib import pyplot as plt
import numpy as np
import math

class option_strategy_events(object):

    def __init__(self,df_events,df_vix):
        self.df_events = df_events
        self.df_vix = df_vix
        self.dt_list = sorted(df_vix['dt_date'].unique())
        self.event_ids = list(df_events['id_event'])
        self.nbr_events = len(self.event_ids)
        self.event_id = self.event_ids[0]
        self.x = []
        self.y = []
        self.y_1 = []

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
        # a = params[0]
        # d = params[1]
        # beta_list = params[2:2+self.nbr_events]
        # mu_list = params[2+self.nbr_events:2+self.nbr_events*2]
        # sigma_list = params[2+self.nbr_events*2:]
        a,d,beta,mu,sigma = params
        obj = 0
        for (idx_vix,r_vix) in self.df_vix.iterrows():
            if idx_vix == 0 : continue
            norm = 0
            # for (idx_e, r_e) in self.df_events.iterrows():
            #     event_id = r_e['id_event']
            dt = r_vix[self.event_id]
            if dt > 30 or dt < -30: continue
                # norm += beta_list[idx_e] * self.normal_distribution(dt,mu_list[idx_e], sigma_list[idx_e])
            norm += beta * self.normal_distribution(dt, mu, sigma)
            y_t = r_vix['amt_close']
            y_t1 = self.df_vix.loc[idx_vix-1,'amt_close']
            obj_t = - y_t + a + d*y_t1 + norm
            obj += obj_t
            self.x.append(dt)
            self.y.append(dt)
            self.y_1.append(dt)
        # print(params)
        return obj

    def optimize_event(self):
        # init_params = np.ones(3*self.nbr_events+2)
        init_params = np.ones(5)
        bnds = ((1e-3,50), (-1,1), (None,None),(1e-3,10),(1e-3,1))
        res = minimize(self.func_derm, init_params, method='L-BFGS-B', bounds =bnds,tol=1e-3)
        # res = minimize(self.func_derm, init_params, method='Nelder-Mead',tol=1e-3)
        # res = minimize(self.func_derm, init_params, method='SLSQP')
        return res


    def event_analysis(self):
        for (idx_e, r_e) in self.df_events.iterrows():
            try:
                self.event_id = r_e['id_event']
                res = self.optimize_event()
                x = self.x
                y = self.y

                print('='*50)
                print(self.event_id)
                print('-'*50)
                print(res.x)
                print(res.success)
                print(res.message)
                plt.scatter(x, y,label='data')
                a, d, beta, mu, sigma = res.x
                res_y = []
                for (i,xi) in enumerate(x):
                    yi = a + d*self.y_1[i] + beta*self.normal_distribution(xi,mu,sigma)
                    res_y.append(yi)
                plt.plot(x, res_y,label='regress')
                plt.legend()
                plt.show()
            except:
                pass


    def generate_data(self,t, A, sigma, omega, noise=0, n_outliers=0, random_state=0):
        y = A * np.exp(-sigma * t) * np.sin(omega * t)
        rnd = np.random.RandomState(random_state)
        error = noise * rnd.randn(t.size,0)
        outliers = rnd.randint(0, t.size, n_outliers)
        error[outliers] *= 35
        return y + error








    def func_derm2(self,params):
        a = params[0]
        d = params[1]
        beta_list = params[2:2+self.nbr_events]
        mu_list = params[2+self.nbr_events:2+self.nbr_events*2]
        sigma_list = params[2+self.nbr_events*2:]
        # a,d,beta,mu,sigma = params
        obj = 0
        for (idx_vix,r_vix) in self.df_vix.iterrows():
            if idx_vix == 0 : continue
            norm = 0
            for (idx_e, r_e) in self.df_events.iterrows():
                event_id = r_e['id_event']
                dt = r_vix[event_id]
                if dt >30 or dt< -30 : continue
                norm += beta_list[idx_e] * self.normal_distribution(dt,mu_list[idx_e], sigma_list[idx_e])
            dt = r_vix[self.event_id]
            # norm += beta * self.normal_distribution(dt, mu, sigma)
            y_t = r_vix['amt_close']
            y_t1 = self.df_vix.loc[idx_vix-1,'amt_close']
            obj_t = - y_t + a + d*y_t1 + norm
            obj += obj_t
        print(params)
        return obj

    def normal_distribution(self,dt,mu,s):
        # mu = max(1e-10,mu)
        # s = max(1e-10,s)
        return math.exp(-(dt-mu)**2/(2*s**2))/(s*math.sqrt(2*math.pi))


    def optimize_events(self):
        init_params = np.ones(3*self.nbr_events+2)
        # init_params = np.ones(5)
        bnds = ((0,50), (-1,1), (None,None),(0,math.inf),(0,math.inf))
        res = minimize(self.func_derm2, init_params, method='L-BFGS-B',bounds=bnds,tol=1e-3)
        x = self.x
        y = self.y

        print('=' * 50)
        print(self.event_id)
        print('-' * 50)
        print(res.x)
        print(res.success)
        print(res.message)
        plt.scatter(x, y, label='data')
        a, d, beta, mu, sigma = res.x
        res_y = []
        for (i, xi) in enumerate(x):
            yi = a + d * self.y_1[i] + beta * self.normal_distribution(xi, mu, sigma)
            res_y.append(yi)
        plt.plot(x, res_y, label='regress')
        plt.legend()
        plt.show()
        return res




engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata?charset=utf8mb4', echo=False)
conn = engine.connect()
metadata = MetaData(engine)
Session = sessionmaker(bind=engine)
sess = Session()
events = Table('events', metadata, autoload=True)
indexes = Table('indexes_mktdata', metadata, autoload=True)
query_events = sess.query(events).filter(events.c.dt_first_trading > '2016-1-1')
query_vix = sess.query(indexes.c.dt_date,indexes.c.id_instrument,indexes.c.amt_close)\
    .filter(indexes.c.id_instrument == 'index_cvix').filter(indexes.c.dt_date > '2016-1-1')
df_events = pd.read_sql(query_events.statement,query_events.session.bind)
df_vix = pd.read_sql(query_vix.statement,query_vix.session.bind)

s = option_strategy_events(df_events,df_vix)
s.add_nbr_days_from_events()
s.event_analysis()
# s.optimize_events()
# print(s.df_vix)
print('')