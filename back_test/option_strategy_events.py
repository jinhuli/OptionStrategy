import pandas as pd
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from scipy.optimize import minimize
import scipy.optimize as optimize
from matplotlib import pyplot as plt
from scipy.optimize import curve_fit
# from statsmodels.graphics.tsaplots import plot_acf
# from statsmodels.tsa.ar_model import AR
# from arch import arch_model
import numpy as np
import math


class option_strategy_events(object):
    def __init__(self, df_events, df_metrics, event_model='normal'):
        self.df_events = df_events.sort_values(by='dt_date', ascending=True)
        self.df_metrics = df_metrics.sort_values(by='dt_date', ascending=True)
        self.event_model = event_model

        self.dt_list = sorted(df_vix['dt_date'].unique())
        self.event_ids = list(df_events['id_event'])
        self.nbr_events = len(self.event_ids)
        self.event_id = self.event_ids[0]
        self.add_nbr_days_from_events()
        self.x = []
        self.y = []
        self.y_1 = []

    # Analyze Event Influence Significance
    def add_nbr_days_from_events(self):
        for (idx_e, r_e) in self.df_events.iterrows():
            dt_event = r_e['dt_first_trading']
            id_event = r_e['id_event']
            self.df_metrics[id_event] = self.calculate_nbr_days(self.df_metrics['dt_date'], dt_event)
            # self.df_index[id_event] = self.calculate_nbr_days(self.df_index['dt_date'], dt_event)

    def calculate_nbr_days(self, dt_list, dt_event):
        nbr_days = []
        for dt in dt_list:
            idx_dt = self.dt_list.index(dt)
            idx_evt = self.dt_list.index(dt_event)
            nbr = idx_dt - idx_evt
            nbr_days.append(nbr)
        return nbr_days

    def normal_distribution(self, dt, mu, s):
        return math.exp(-(dt - mu) ** 2 / (2 * s ** 2)) / (s * math.sqrt(2 * math.pi))

    def lognormal_distribution(self, dt, mu, s):
        f_norm = self.normal_distribution( dt, 0, 1)
        return math.exp(mu+s*f_norm)

    def residual_fun_nerm(self, params):
        # a,d,beta,mu,sigma = params
        beta, mu, sigma = params
        # self.a = a = 0.24
        # self.d = d = 0.99
        square_e = 0
        a = self.df_metrics.loc[0,'amt_close']
        self.y0 = a
        for (idx_vix, r_vix) in self.df_metrics.iterrows():
            if idx_vix == 0: continue
            dt = r_vix[self.event_id]
            if dt > 100 or dt < -30: continue
            # norm = beta * self.normal_distribution(dt, mu, sigma)
            norm = beta * self.lognormal_distribution(dt, mu, sigma)
            y_t = r_vix['amt_close']
            # y_t1 = self.df_metrics.loc[idx_vix - 1, 'amt_close']
            # obj_t = - y_t + a + norm
            obj_t = - y_t + a + norm
            square_e += obj_t ** 2
            if dt not in self.x:
                self.x.append(dt)
                self.y.append(y_t)
                # self.y_1.append(y_t1)
        return square_e

    def residual_fun_polinomial(self, params):
        return None

    def regress_ar(self):
        # a,d = params
        square_e = 0
        yt = self.df_metrics['amt_close']
        yt_l1 = yt.shift(1)
        df = self.df_metrics
        df['amt_close_l1'] = yt_l1
        # GARCH MODEL
        # garch11 = arch_model(yt, p=1, q=1)
        # res = garch11.fit(update_freq=10)
        # Auto-regression Plot
        # plot_acf(yt, lags=31)
        # plt.show()
        #
        df1 = df[['dt_date', 'amt_close']]
        df1 = df1.set_index('dt_date')
        X = df1.values
        # train, test = X[1:len(X) - 7], X[len(X) - 7:]
        # train autoregression
        # model = AR(X)
        # res = model.fit()
        # residuals = res.resid
        # res_x = df1['amt_close'][res.k_ar :]
        # plt.plot(residuals)
        plt.show()

    def residuals_ar1(self, params):
        a, d = params
        yt = self.df_metrics['amt_close']
        yt_l1 = yt.shift(1)
        df = self.df_metrics
        df['amt_close_l1'] = yt_l1
        square_e = 0
        for (idx, r) in df.iterrows():
            if idx == 0: continue
            y_t = r['amt_close']
            y_t1 = df.loc[idx, 'amt_close_l1']
            obj_t = - y_t + a + d * y_t1
            square_e += obj_t ** 2
        return square_e

    def residual_delta_vol(self, params):
        beta, mu, sigma = params
        square_e = 0
        for (idx_vix, r_vix) in self.df_metrics.iterrows():
            if idx_vix == 0: continue
            norm = 0
            dt = r_vix[self.event_id]
            if dt > 100 or dt < -30: continue
            norm += beta * self.normal_distribution(dt, mu, sigma)
            y_t = r_vix['amt_close']
            y_t1 = self.df_metrics.loc[idx_vix - 1, 'amt_close']
            obj_t = - y_t + y_t1 + norm
            # obj_t = - y_t + a + norm
            square_e += obj_t ** 2
            if dt not in self.x:
                self.x.append(dt)
                self.y.append(y_t)
                self.y_1.append(y_t1)
        return square_e

    # def obj_curvefit(self):


    # def optimize_curvefit(self):
    #     for (idx_e, r_e) in self.df_events.iterrows():
    #         x = []
    #         delta_y = []
    #         self.event_id = r_e['id_event']
    #         print(self.event_id)
    #         for (idx_vix, r_vix) in self.df_metrics.iterrows():
    #             if idx_vix == 0: continue
    #             dt = r_vix[self.event_id]
    #             if dt > 30 or dt < -30: continue
    #             y_t = r_vix['amt_close']
    #             y_t1 = self.df_metrics.loc[idx_vix - 1, 'amt_close']
    #             x.append(dt)
    #             delta_y.append(y_t - y_t1)
    #         func = self.normal_distribution
    #         popt, pcov = curve_fit(func, np.array(x), np.array(delta_y))
    #         print(popt)
    #         print(pcov)

    def optimize_ar1(self):
        init_params = np.ones(2)
        res = minimize(self.residuals_ar1, init_params, method='L-BFGS-B', tol=1e-3)
        params = res.x
        res_y = []
        res_norm = []
        x_norm = []
        events = []
        x = []
        y = []
        a = params[0]
        d = params[1]
        tss = 0
        ess = 0  # explained sum of squared errors
        for (idx_vix, r_vix) in self.df_metrics.iterrows():
            if idx_vix == 0: continue
            date = r_vix['dt_date']
            for (idx_e, r_e) in self.df_events.iterrows():
                event_id = r_e['id_event']
                dt = r_vix[event_id]
                if dt == 0:
                    x_norm.append(date)
                    events.append(15)
            y_t = r_vix['amt_close']
            y_t1 = self.df_metrics.loc[idx_vix - 1, 'amt_close']
            res_y.append(a + d * y_t1)
            x.append(date)
            y.append(y_t)
        regression_errors = []
        y_mean = np.mean(y)
        for (idx, res_yi) in enumerate(res_y):
            ess += (res_yi - y_mean) ** 2
            yi = y[idx]
            tss += (yi - y_mean) ** 2
            regression_errors.append(yi - res_yi)
        rss = tss - ess  # residual sum of squared errors
        print('=' * 100)
        cov = res.hess_inv.todense()
        print('cov : ', cov)
        cov_ii = np.diag(cov)
        df = len(self.dt_list) - 1 - 3
        t_a = a / np.sqrt(cov_ii[0] * rss / df)
        t_d = d / np.sqrt(cov_ii[1] * rss / df)
        print('a : ', a, ' ', t_a)
        print('d : ', d, ' ', t_d)
        print('TSS : ', tss)
        print('RSS : ', rss)
        print('R square : ', ess / tss)
        plt.figure(1)
        plt.scatter(x, y, label='data')
        plt.plot(x, res_y, label='regress', color='r')
        plt.legend()
        plt.figure(2)
        plt.plot(x, regression_errors, label='nerm')
        plt.bar(x_norm, events, label='event', color='r')
        plt.legend()
        plt.show()
        return res

    def optimize_per_event(self):
        for (idx_e, r_e) in self.df_events.iterrows():
            self.x.clear()
            self.y.clear()
            self.y_1.clear()
            self.event_id = r_e['id_event']
            # init_params = np.ones(5)
            init_params = np.ones(3)
            # init_params = np.ones(4)
            # bnds = ((1e-3, 50), (-1, 1), (None, None), (1e-3, 30), (1e-3, 100))
            # bnds = ((None, None), (1e-3, 30), (1e-3, 100))
            if self.event_model == 'normal':
                # res = minimize(self.residual_fun_nerm, init_params, method='L-BFGS-B', bounds=bnds, tol=1e-3)
                res = minimize(self.residual_fun_nerm, init_params, method='L-BFGS-B', tol=1e-3)
                # res = minimize(self.residual_delta_vol, init_params, method='L-BFGS-B', tol=1e-3)
            else:
                break
            x = self.x
            y = self.y
            print('=' * 50)
            print(r_e['dt_date'])
            print(self.event_id, r_e['name_event'])
            print('-' * 50)

            # a, d, beta, mu, sigma = res.x
            # a, beta, mu, sigma = res.x
            # a = self.a
            # d = self.d
            beta, mu, sigma = res.x
            cov = res.hess_inv.todense()
            cov_ii = np.diag(cov)
            res_y = []
            res_norm = []
            residual_norm = []
            for (i, xi) in enumerate(x):
                # yi = a + beta * self.normal_distribution(xi, mu, sigma)
                yi = np.exp(beta * self.normal_distribution(xi, mu, sigma))*self.y0
                # yi = self.y_1[i] + beta * self.normal_distribution(xi, mu, sigma)
                res_y.append(yi)
            x_norm = np.arange(-10, 30, 1)
            for xj in x_norm:
                res_norm.append(beta * self.normal_distribution(xj, mu, sigma))
                residual_norm.append()
            ess = 0
            tss = 0
            rss = 0
            y_mean = np.mean(y)
            for (idx, res_yi) in enumerate(res_y):
                ess += (res_yi - y_mean) ** 2
                yi = y[idx]
                tss += (yi - y_mean) ** 2
                rss += (res_yi - yi) ** 2
            # df = len(x) - 6
            df = len(x) - 4
            # t_a = a / np.sqrt(cov_ii[0] * rss / df)
            # t_d = d / np.sqrt(cov_ii[1] * rss / df)
            # t_b = beta / np.sqrt(cov_ii[2] * rss / df)
            # t_mu = mu / np.sqrt(cov_ii[3] * rss / df)
            # t_s = sigma / np.sqrt(cov_ii[4] * rss / df)
            t_b = beta / np.sqrt(cov_ii[0] * rss / df)
            t_mu = mu / np.sqrt(cov_ii[1] * rss / df)
            t_s = sigma / np.sqrt(cov_ii[2] * rss / df)
            # print('a : ', a, ' ', t_a)
            # print('d : ', d, ' ', t_d)
            print('beta : ', beta, ' ', t_b)
            print('mu : ', mu, ' ', t_mu)
            print('sigma : ', sigma, ' ', t_s)
            plt.figure(idx_e)
            plt.scatter(x, y,label='data')
            plt.plot(x, res_y,label='regress',color='r')
            plt.legend()
            plt.figure(idx_e+1)
            plt.plot(x_norm, res_norm,label='regress 2',color='y')
            plt.plot(x_norm, res_norm,label='residual data',color='r')
            plt.legend()
            plt.show()
            plt.clf()

    def residuals_fun_nerm(self, params, df):
        a = params[0]
        d = params[1]
        beta_list = params[2:2 + self.nbr_events]
        mu_list = params[2 + self.nbr_events:2 + self.nbr_events * 2]
        sigma_list = params[2 + self.nbr_events * 2:]
        squared_e = 0
        for (idx_vix, r_vix) in self.df_metrics.iterrows():
            if idx_vix == 0: continue
            norm = 0
            for (idx_e, r_e) in self.df_events.iterrows():
                event_id = r_e['id_event']
                dt = r_vix[event_id]
                if dt > 30 or dt < -30: continue
                norm += beta_list[idx_e] * self.normal_distribution(dt, mu_list[idx_e], sigma_list[idx_e])
            y_t = r_vix['amt_close']
            y_t1 = self.df_metrics.loc[idx_vix - 1, 'amt_close']
            obj_t = - y_t + a + d * y_t1 + norm
            squared_e += obj_t ** 2
            date = r_vix['dt_date']
        print(params)
        return squared_e

    def residuals_fun_lnerm(self, params):
        a = params[0]
        d = params[1]
        beta_list = params[2:2 + self.nbr_events]
        mu_list = params[2 + self.nbr_events:2 + self.nbr_events * 2]
        sigma_list = params[2 + self.nbr_events * 2:]
        squared_e = 0
        for (idx_vix, r_vix) in self.df_metrics.iterrows():
            if idx_vix == 0: continue
            norm = 0
            for (idx_e, r_e) in self.df_events.iterrows():
                event_id = r_e['id_event']
                dt = r_vix[event_id]
                if dt > 30 or dt < -30: continue
                norm += beta_list[idx_e] * self.normal_distribution(dt, mu_list[idx_e], sigma_list[idx_e])
            y_t = r_vix['amt_close']
            y_t1 = self.df_metrics.loc[idx_vix - 1, 'amt_close']
            obj_t = - math.log(y_t, math.e) + a + d * math.log(y_t1, math.e) + norm
            squared_e += obj_t ** 2
            date = r_vix['dt_date']
        print(params)
        return squared_e

    def optimize_events(self):
        init_params = np.ones(3 * self.nbr_events + 2)
        res = minimize(self.residuals_fun_lnerm, init_params, method='L-BFGS-B', tol=1e-3)
        params = res.x
        res_y = []
        res_norm = []
        x_norm = []
        events = []
        x = []
        y = []
        a = params[0]
        d = params[1]
        beta_list = params[2:2 + self.nbr_events]
        mu_list = params[2 + self.nbr_events:2 + self.nbr_events * 2]
        sigma_list = params[2 + self.nbr_events * 2:]
        tss = 0

        ess = 0  # explained sum of squared errors
        for (idx_vix, r_vix) in self.df_metrics.iterrows():
            if idx_vix == 0: continue
            norm = 0
            date = r_vix['dt_date']
            for (idx_e, r_e) in self.df_events.iterrows():
                event_id = r_e['id_event']
                dt = r_vix[event_id]
                # if dt >30 or dt< -30 : continue
                if dt == 0:
                    x_norm.append(date)
                    events.append(2)
                norm += beta_list[idx_e] * self.normal_distribution(dt, mu_list[idx_e], sigma_list[idx_e])
            y_t = r_vix['amt_close']
            y_t1 = self.df_metrics.loc[idx_vix - 1, 'amt_close']
            res_y.append(a + d * y_t1 + norm)
            x.append(date)
            y.append(y_t)
            res_norm.append(norm)
        y_mean = np.mean(y)
        for res_yi in res_y:
            ess += (res_yi - y_mean) ** 2
        for yi in y:
            tss += (yi - y_mean) ** 2
        rss = tss - ess  # residual sum of squared errors
        print('=' * 100)
        df_res = pd.DataFrame()
        cov = res.hess_inv.todense()
        print('cov : ', cov)
        cov_ii = np.diag(cov)
        beta_cov = cov_ii[2:2 + self.nbr_events]
        mu_cov = cov_ii[2 + self.nbr_events:2 + self.nbr_events * 2]
        sigma_cov = cov_ii[2 + self.nbr_events * 2:]
        df = len(self.dt_list) - self.nbr_events * 3 - 3
        print('df : ', df)
        for (idx_e, r_e) in self.df_events.iterrows():
            event_id = r_e['id_event']
            print(event_id)
            b = beta_list[idx_e]
            mu = mu_list[idx_e]
            s = sigma_list[idx_e]
            # SE(Pi) = sqrt[(SS / DF) * Cov(i, i)]
            t_b = b / np.sqrt(beta_cov[idx_e] * rss / df)
            t_mu = mu / np.sqrt(mu_cov[idx_e] * rss / df)
            t_s = s / np.sqrt(sigma_cov[idx_e] * rss / df)
            # t_b = b/np.sqrt(beta_cov[idx_e])
            # t_mu = mu/np.sqrt(mu_cov[idx_e])
            # t_s = s/np.sqrt(sigma_cov[idx_e])
            print('beta : ', b, ' ', t_b)
            print('mu : ', mu, ' ', t_mu)
            print('sigma : ', s, ' ', t_s)
            r = pd.DataFrame(
                data={'1-id_event': [event_id], '2-name_event': [r_e['name_event']],
                      '3-beta': [beta_list[idx_e]],
                      '4-mu': [mu_list[idx_e]], '5-sigma': [sigma_list[idx_e]],
                      '6-a': [a], '7-d': [d], '8-ttest_beta': [t_b], '9-ttest_mu': [t_mu],
                      '10-ttest': [t_s]},
                index=[event_id])
            df_res = df_res.append(r)
        print('TSS : ', tss)
        print('RSS : ', rss)
        print('R square : ', ess / tss)
        r = pd.DataFrame(
            data={'1-TSS': [tss], '2-ESS': [ess], '3-R square': [ess / tss]}, index=[1111])
        df_res = df_res.append(r)
        df_res.to_csv('../save_results/drem_regression_results.csv')
        plt.figure(1)
        plt.scatter(x, y, label='data')
        plt.plot(x, res_y, label='regress', color='r')
        plt.legend()
        plt.figure(2)
        plt.plot(x, res_norm, label='nerm')
        plt.bar(x_norm, events, label='event', color='r')
        plt.legend()
        plt.show()
        return res


def add_index_yield(df_index):
    df_index = df_index.sort_values(by='dt_date', ascending=True)
    p0 = df_index.loc[0, 'amt_close']
    yields = [0.0]
    for (idx, r) in df_index.iterrows():
        if idx == 0: continue
        p1 = r['amt_close']
        y = 100 * (p1 - p0) / p0
        yields.append(y)
        p0 = p1
    df_index['amt_close'] = yields
    df_index = df_index.loc[1:, :].reset_index()
    return df_index


engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata?charset=utf8mb4', echo=False)
conn = engine.connect()
metadata = MetaData(engine)
Session = sessionmaker(bind=engine)
sess = Session()
events = Table('events', metadata, autoload=True)
indexes = Table('indexes_mktdata', metadata, autoload=True)
start_date = '2015-3-1'
end_date = '2018-3-1'
query_events = sess.query(events).filter(events.c.dt_first_trading > start_date) \
    .filter(events.c.dt_first_trading < end_date) \
    .filter(events.c.flag_impact == 1)
query_vix = sess.query(indexes.c.dt_date, indexes.c.id_instrument, indexes.c.amt_close) \
    .filter(indexes.c.id_instrument == 'index_cvix') \
    .filter(indexes.c.dt_date > start_date) \
    .filter(indexes.c.dt_date < end_date)
query_index = sess.query(indexes.c.dt_date, indexes.c.id_instrument, indexes.c.amt_close) \
    .filter(indexes.c.id_instrument == 'index_50sh') \
    .filter(indexes.c.dt_date > start_date) \
    .filter(indexes.c.dt_date < end_date)
df_events = pd.read_sql(query_events.statement, query_events.session.bind)
df_vix = pd.read_sql(query_vix.statement, query_vix.session.bind)
df_index = pd.read_sql(query_index.statement, query_index.session.bind)

df_metrics = add_index_yield(df_vix)
# s = option_strategy_events(df_events,df_metrics)
s = option_strategy_events(df_events, df_vix)
# s.optimize_ar1()
# s.regress_ar()
s.optimize_per_event()
# res = s.optimize_events()
