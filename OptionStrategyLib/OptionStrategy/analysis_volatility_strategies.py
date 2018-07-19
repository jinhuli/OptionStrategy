from abc import ABCMeta, abstractmethod
import QuantLib as ql
import numpy as np
import pandas as pd
import datetime
from PricingLibrary.Options import OptionPlainEuropean
from PricingLibrary.OptionMetrics import OptionMetrics
from PricingLibrary.Evaluation import Evaluation
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt


class Portfolio(object):
    __metaclass__ = ABCMeta

    def __init__(self, evalDate,rf,df_theory=None):
        self.calendar = ql.China()
        self.daycounter = ql.ActualActual()
        self.evaluation = Evaluation(evalDate, self.daycounter, self.calendar)
        self.rf = rf
        self.df_theory = df_theory
        self.engineType = 'AnalyticEuropeanEngine'
        self.init_v = 100.0
        self.init_theory_margin = 5.0
        self.metrics_set = []

    def reset_evaluation(self, evalDate):
        self.evaluation = Evaluation(evalDate, self.daycounter, self.calendar)
        for metric in self.metrics_set:
            metric.set_evaluation(self.evaluation)

    def reset_vol(self, vol):
        self.vol = vol

    def reset_spot(self, spot):
        self.spot = spot

    @abstractmethod
    def delta_neutral(self):
        return

    @abstractmethod
    def pnl(self, spot=None, implied_vol=None):
        return

    @abstractmethod
    def value(self, spot=None, implied_vol=None):
        return

    @abstractmethod
    def delta(self):
        return

    def pnls_senario_spot(self, spot_list):
        pnls = []
        for spot in spot_list:
            y = self.pnl(spot)
            pnls.append(y)
        return pnls


class PortfolioStraddle(Portfolio):

    def __init__(self, evalDate, option_call, option_put, spot, strike, vol, rf, df_theory=None):
        Portfolio.__init__(self, evalDate, rf,df_theory)
        self.vol = vol
        self.spot = spot
        self.option_call = option_call
        self.option_put = option_put
        self.metrics_call = OptionMetrics(option_call,rf,self.engineType).set_evaluation(self.evaluation)
        self.metrics_put = OptionMetrics(option_put,rf,self.engineType).set_evaluation(self.evaluation)
        self.metrics_set = [self.metrics_call,self.metrics_put]
        self.delta_neutral()

    # def reset_option(self, option_call, option_put):
    #     self.option_call = option_call
    #     self.option_put = option_put
    #     self.metrics_call = OptionMetrics(option_call)
    #     self.metrics_put = OptionMetrics(option_put)
    #     self.delta_neutral()

    def delta_neutral(self):
        iv_call = self.metrics_call.implied_vol(self.spot,self.metrics_call.Option.init_price)
        iv_put = self.metrics_put.implied_vol(self.spot,self.metrics_put.Option.init_price)
        self.metrics_call.Option.init_iv = iv_call
        self.metrics_put.Option.init_iv = iv_put
        delta_call = self.metrics_call.delta(self.spot, iv_call)
        delta_put = self.metrics_put.delta(self.spot, iv_put)
        rc = 1.0
        rp = -delta_call / delta_put
        theorymargin_c = self.df_theory.loc[self.metrics_call.Option.strike,'call']-self.metrics_call.Option.init_price
        theorymargin_p = self.df_theory.loc[self.metrics_put.Option.strike,'put']-self.metrics_put.Option.init_price
        n = self.init_theory_margin/(rc*theorymargin_c+rp*theorymargin_p)
        self.ratio_call = rc*n
        self.ratio_put = rp*n
        p_put = self.metrics_put.option_price(self.spot, iv_put)
        p_call = self.metrics_call.option_price(self.spot, iv_call)
        self.init_value = self.ratio_call*p_call + self.ratio_put*p_put

    def pnl(self, spot=None,implied_vol=None):
        if spot == None: spot = self.spot
        if implied_vol == None:
            p_call = self.metrics_call.option_price(spot, self.metrics_call.Option.init_iv)
            p_put = self.metrics_put.option_price(spot, self.metrics_put.Option.init_iv)
        else:
            p_call = self.metrics_call.option_price(spot, implied_vol)
            p_put = self.metrics_put.option_price(spot, implied_vol)
        pnl = self.ratio_call * p_call + self.ratio_put * p_put - self.init_value
        return pnl

    def value(self, spot=None,implied_vol=None):
        if spot == None: spot = self.spot
        if implied_vol == None:
            p_call = self.metrics_call.option_price(spot, self.metrics_call.Option.init_iv)
            p_put = self.metrics_put.option_price(spot, self.metrics_put.Option.init_iv)
        else:
            p_call = self.metrics_call.option_price(spot, implied_vol)
            p_put = self.metrics_put.option_price(spot, implied_vol)
        port_value = self.ratio_call * p_call + self.ratio_put * p_put
        return port_value

    def delta(self,implied_vol=None):
        if implied_vol == None:
            iv_call = self.metrics_call.implied_vol(self.spot, self.metrics_call.Option.init_price)
            iv_put = self.metrics_put.implied_vol(self.spot, self.metrics_put.Option.init_price)
        else:
            iv_call = iv_put = implied_vol
        delta_call = self.metrics_call.delta(self.spot, iv_call)
        delta_put = self.metrics_put.delta( self.spot, iv_put)
        port_delta = self.ratio_call * delta_call + self.ratio_put * delta_put
        return port_delta


class PortfolioBackspread(Portfolio):

    def __init__(self, evalDate, option_long, option_short, spot, strike, vol, rf, df_theory=None):
        Portfolio.__init__(self, evalDate, rf,df_theory)
        self.vol = vol
        self.spot = spot
        self.option_long = option_long
        self.option_short = option_short
        self.metrics_long = OptionMetrics(option_long,rf,self.engineType).set_evaluation(self.evaluation)
        self.metrics_short = OptionMetrics(option_short,rf,self.engineType).set_evaluation(self.evaluation)
        self.metrics_set = [self.metrics_long,self.metrics_short]
        self.delta_neutral()

    # def reset_option(self, option_long, option_short):
    #     self.option_long = option_long
    #     self.option_short = option_short
    #     self.metrics_long = OptionMetrics(option_long,rf,engineType).set_evaluation(self.evaluation)
    #     self.metrics_short = OptionMetrics(option_short,rf,engineType).set_evaluation(self.evaluation)
    #     self.delta_neutral()

    def delta_neutral(self):
        iv_long = self.metrics_long.implied_vol(self.spot,self.metrics_long.Option.init_price)
        iv_short = self.metrics_short.implied_vol(self.spot,self.metrics_short.Option.init_price)
        self.metrics_long.Option.init_iv = iv_long
        self.metrics_short.Option.init_iv = iv_short
        delta_long = self.metrics_long.delta(self.spot,  iv_long)
        delta_short = self.metrics_short.delta(self.spot, iv_short)
        rl = 1.0
        rs = -delta_long / delta_short
        theorymargin_l = self.df_theory.loc[self.metrics_long.Option.strike,'call']-self.metrics_long.Option.init_price
        theorymargin_s = self.df_theory.loc[self.metrics_short.Option.strike,'call']-self.metrics_short.Option.init_price
        n = self.init_theory_margin/(rl*theorymargin_l+rs*theorymargin_s)
        self.ratio_long = rl*n
        self.ratio_short = rs*n
        p_long = self.metrics_long.option_price(self.spot, iv_long)
        p_short = self.metrics_short.option_price(self.spot, iv_short)
        self.init_value_long = self.ratio_long * p_long
        self.init_value_short = -self.ratio_short * p_short
        self.init_earning = self.init_value_short - self.init_value_long

    def pnl(self, spot=None,implied_vol=None):
        if spot == None: spot = self.spot
        if implied_vol == None:
            p_l = self.metrics_long.option_price(spot, self.metrics_long.Option.init_iv)
            p_s = self.metrics_short.option_price(spot, self.metrics_short.Option.init_iv)
        else:
            p_l = self.metrics_long.option_price(spot, implied_vol)
            p_s = self.metrics_short.option_price(spot, implied_vol)
        if self.evaluation.evalDate == self.metrics_long.Option.maturitydt:
            p_l = max(0.0,spot-self.metrics_long.Option.strike)
            p_s = max(0.0,spot-self.metrics_short.Option.strike)
        v_long = p_l * self.ratio_long
        v_short = -p_s * self.ratio_short
        pnl = v_long - self.init_value_long + self.init_value_short - v_short
        return pnl

    def value(self, spot=None,implied_vol=None):
        return self.pnl(spot,implied_vol) + self.init_earning


    def delta(self,implied_vol=None):
        if implied_vol == None:
            iv_long = self.metrics_long.implied_vol(self.spot, self.metrics_long.Option.init_price)
            iv_short = self.metrics_short.implied_vol(self.spot, self.metrics_short.Option.init_price)
        else:
            iv_long = iv_short = implied_vol
        delta_l = self.metrics_long.delta(self.spot, iv_long)
        delta_s = self.metrics_short.delta( self.spot,iv_short)
        port_delta = self.ratio_long*delta_l - self.ratio_short*delta_s
        return port_delta


# pu = PlotUtil()
# eval_date = datetime.date(2018, 3, 1)
# mdt = datetime.date(2018, 3, 28)
# # spot = 2.8840  # 2018-3-1 50etf close price
# spot = 2.9
# strike = 2.9
# strike2 = 3.0
# strike3 = 2.8
# vol = 0.3
# rf = 0.03
# mkt_call = {3.1: 0.0148, 3.0: 0.0317, 2.95: 0.0461, 2.9: 0.0641, 2.85: 0.095, 2.8: 0.127, 2.75: 0.1622}
# mkt_put = {3.1: 0.2247, 3.0: 0.1424, 2.95: 0.1076, 2.9: 0.0751, 2.85: 0.0533, 2.8: 0.037, 2.75: 0.0249}
#
# engineType = 'AnalyticEuropeanEngine'
# calendar = ql.China()
# daycounter = ql.ActualActual()
# evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
# maturitydt = ql.Date(mdt.day, mdt.month, mdt.year)
# evaluation = Evaluation(evalDate, daycounter, calendar)
#
# strikes = []
# thoery_call = []
# thoery_put = []
# for k in mkt_call.keys():
#     option = OptionPlainEuropean(k, maturitydt, ql.Option.Call)
#     metrics = OptionMetrics(option,rf,engineType).set_evaluation(evaluation)
#     p = metrics.option_price(spot, vol)
#     thoery_call.append(p)
#     strikes.append(k)
#     option = OptionPlainEuropean(k, maturitydt, ql.Option.Put)
#     metrics = OptionMetrics(option,rf,engineType).set_evaluation(evaluation)
#     p = metrics.option_price(spot, vol)
#     thoery_put.append(p)
#
# df_theory = pd.DataFrame({'k':strikes,'call':thoery_call,'put':thoery_put}).set_index('k')
# df_theory.to_csv('../save_results/df_theory.csv')
#
#
# evaluation = Evaluation(evalDate, daycounter, calendar)
#
# option_call = OptionPlainEuropean(strike, maturitydt, ql.Option.Call, mkt_call[strike])
# option_put = OptionPlainEuropean(strike, maturitydt, ql.Option.Put, mkt_put[strike])
# option_call2 = OptionPlainEuropean(strike2, maturitydt, ql.Option.Call, mkt_call[strike2]) #虚值
# option_call3 = OptionPlainEuropean(strike3,maturitydt,ql.Option.Call,mkt_call[strike3]) #实值
#
# # metricscall = OptionMetrics(option_call, rf, engineType).set_evaluation(evaluation)
# # iv_call = metricscall.implied_vol(spot, mkt_call[strike])
# # delta_call = metricscall.delta(spot,iv_call)
# # vega_call = metricscall.vega(spot, iv_call)
# # rho_call = metricscall.rho(spot,iv_call)
# # gamma_call = metricscall.gamma(spot,iv_call)
# #
# # metricsput = OptionMetrics(option_call2,rf,engineType).set_evaluation(evaluation)
# # iv_put = metricsput.implied_vol(spot, mkt_call[strike])
# # delta_put = metricsput.delta(spot,iv_put)
# # vega_put = metricsput.vega(spot, iv_put)
# # rho_put = metricsput.rho(spot,iv_put)
# # gamma_put = metricsput.gamma(spot,iv_put)
# #
# # print('iv ', iv_call,iv_put)
# # print('delta ', delta_call,delta_put)
# # print('vega ', vega_call,vega_put)
# # print('rho ',rho_call,rho_put)
# # print('gamma ',gamma_call,gamma_put)
# #
# # metricscall2 = OptionMetrics(option_call2, rf, engineType).set_evaluation(evaluation)
# # metricscall3 = OptionMetrics(option_call3, rf, engineType).set_evaluation(evaluation)
# # iv_call2 = metricscall2.implied_vol(spot, mkt_call[strike2])
# # iv_call3 = metricscall3.implied_vol(spot, mkt_call[strike3])
# #
# # prices = []
# # prices2 = []
# # prices3 = []
# # x = []
# # t = 0
# # while evalDate < maturitydt:
# #     p = metricscall.theta(spot,iv_call)
# #     p2 = metricscall2.theta(spot,iv_call2)
# #     p3 = metricscall3.theta(spot,iv_call3)
# #     prices.append(p)
# #     prices2.append(p2)
# #     prices3.append(p3)
# #     x.append(t)
# #     evalDate = calendar.advance(evalDate, ql.Period(1, ql.Days))
# #     evaluation = Evaluation(evalDate, daycounter, calendar)
# #     metricscall.set_evaluation(evaluation)
# #     metricscall2.set_evaluation(evaluation)
# #     metricscall3.set_evaluation(evaluation)
# #     t += 1
# # df_thetas = pd.DataFrame({'t':x,'theta-atm':prices,'theta-otm':prices2,'theta-itm':prices3}).sort_values(by='t')
# # df_thetas.to_csv('../save_results/df_thetas.csv')
# """组合价值随标的变化"""
# evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
# # port = PortfolioStraddle(evalDate, option_call, option_put, spot, strike, vol, rf,df_theory)
# # print('ratio ',port.ratio_call,port.ratio_put)
# port = PortfolioBackspread(evalDate, option_call2, option_call, spot, strike, vol, rf, df_theory)
# print('ratio ',port.ratio_long,port.ratio_short)
#
# spots = np.arange(spot - 0.3, spot + 0.3, 0.005)
# npvs = port.pnls_senario_spot(spots)
#
# df_pnl_spots = pd.DataFrame({'spots':spots,'pnl':npvs}).sort_values(by='spots')
# df_pnl_spots.to_csv('../save_results/df_pnl_spots-straddle.csv')
# # df_pnl_spots.to_csv('../save_results/df_pnl_spots-backspread.csv')
#
# f1 = pu.plot_line_chart(spots, [npvs], ['straddle'], 'spot', 'pnl')
#
# """组合时间价值衰减"""
# evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
# port = PortfolioStraddle(evalDate, option_call, option_put, spot, strike, vol, rf , df_theory)
# # port = PortfolioBackspread(evalDate, option_call2, option_call, spot, strike, vol, rf, df_theory)
#
# pnls_theta = []
# x = []
# t = 0
# while evalDate < maturitydt:
#     pnl = port.pnl()
#     pnls_theta.append(pnl)
#     x.append(t)
#     evalDate = calendar.advance(evalDate, ql.Period(1, ql.Days))
#     port.reset_evaluation(evalDate)
#     t += 1
#
# df_pnl_ts = pd.DataFrame({'t':x,'pnl':pnls_theta}).sort_values(by='t')
# df_pnl_ts.to_csv('../save_results/df_pnl_ts-straddle.csv')
# # df_pnl_ts.to_csv('../save_results/df_pnl_ts-backspread.csv')
#
#
# f2 = pu.plot_line_chart(x, [pnls_theta], ['straddle'], 't', 'npv')
#
# """组合波动率变化"""
# evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
# port = PortfolioStraddle(evalDate, option_call, option_put, spot, strike, vol, rf, df_theory)
# # port = PortfolioBackspread(evalDate, option_call2, option_call, spot, strike, vol, rf, df_theory)
#
# npv0 = port.pnl()
# pnl_vols = []
# vols = np.arange(15.0, 35.0, 0.1)
# for vol in vols:
#     npv = port.pnl(implied_vol=vol/100.0)
#     pnl_vols.append(npv)
#
# df_pnl_vols = pd.DataFrame({'vol':vols,'pnl':pnl_vols}).sort_values(by='vol')
# df_pnl_vols.to_csv('../save_results/df_pnl_vols-straddle.csv')
# # df_pnl_vols.to_csv('../save_results/df_pnl_vols-backspread.csv')
#
#
# f3 = pu.plot_line_chart(vols, [pnl_vols], ['straddle'], 'vol', 'pnl')
#
# plt.show()
