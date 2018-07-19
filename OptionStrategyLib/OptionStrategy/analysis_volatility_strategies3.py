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
from back_test.analysis_volatility_strategies import PortfolioBackspread


pu = PlotUtil()
eval_date = datetime.date(2018, 3, 1)
mdt = datetime.date(2018, 3, 28)
spot = 2.8840  # 2018-3-1 50etf close price
# spot = 2.9
strike = 2.75
strike2 = 2.85
# strike = 2.85
# strike2 = 2.95
strike3 = 2.8
vol = 0.3
rf = 0.03
mkt_call = {3.1: 0.0148, 3.0: 0.0317, 2.95: 0.0461, 2.9: 0.0641, 2.85: 0.095, 2.8: 0.127, 2.75: 0.1622}
mkt_put = {3.1: 0.2247, 3.0: 0.1424, 2.95: 0.1076, 2.9: 0.0751, 2.85: 0.0533, 2.8: 0.037, 2.75: 0.0249}

engineType = 'AnalyticEuropeanEngine'
calendar = ql.China()
daycounter = ql.ActualActual()
evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
maturitydt = ql.Date(mdt.day, mdt.month, mdt.year)
evaluation = Evaluation(evalDate, daycounter, calendar)

strikes = []
thoery_call = []
thoery_put = []
for k in mkt_call.keys():
    option = OptionPlainEuropean(k, maturitydt, ql.Option.Call)
    metrics = OptionMetrics(option,rf,engineType).set_evaluation(evaluation)
    p = metrics.option_price(spot, vol)
    thoery_call.append(p)
    strikes.append(k)
    option = OptionPlainEuropean(k, maturitydt, ql.Option.Put)
    metrics = OptionMetrics(option,rf,engineType).set_evaluation(evaluation)
    p = metrics.option_price(spot, vol)
    thoery_put.append(p)

df_theory = pd.DataFrame({'k':strikes,'call':thoery_call,'put':thoery_put}).set_index('k')
df_theory.to_csv('../save_results/df_theory.csv')


evaluation = Evaluation(evalDate, daycounter, calendar)

option_call = OptionPlainEuropean(strike, maturitydt, ql.Option.Call, mkt_call[strike])
option_call2 = OptionPlainEuropean(strike2, maturitydt, ql.Option.Call, mkt_call[strike2]) #虚值
option_call3 = OptionPlainEuropean(strike3,maturitydt,ql.Option.Call,mkt_call[strike3]) #实值

# metricscall = OptionMetrics(option_call, rf, engineType).set_evaluation(evaluation)
# iv_call = metricscall.implied_vol(spot, mkt_call[strike])
# delta_call = metricscall.delta(spot,iv_call)
# vega_call = metricscall.vega(spot, iv_call)
# rho_call = metricscall.rho(spot,iv_call)
# gamma_call = metricscall.gamma(spot,iv_call)
#
# metricsput = OptionMetrics(option_call2,rf,engineType).set_evaluation(evaluation)
# iv_put = metricsput.implied_vol(spot, mkt_call[strike])
# delta_put = metricsput.delta(spot,iv_put)
# vega_put = metricsput.vega(spot, iv_put)
# rho_put = metricsput.rho(spot,iv_put)
# gamma_put = metricsput.gamma(spot,iv_put)
#
# print('iv ', iv_call,iv_put)
# print('delta ', delta_call,delta_put)
# print('vega ', vega_call,vega_put)
# print('rho ',rho_call,rho_put)
# print('gamma ',gamma_call,gamma_put)
#
# metricscall2 = OptionMetrics(option_call2, rf, engineType).set_evaluation(evaluation)
# metricscall3 = OptionMetrics(option_call3, rf, engineType).set_evaluation(evaluation)
# iv_call2 = metricscall2.implied_vol(spot, mkt_call[strike2])
# iv_call3 = metricscall3.implied_vol(spot, mkt_call[strike3])
#
# prices = []
# prices2 = []
# prices3 = []
# x = []
# t = 0
# while evalDate < maturitydt:
#     p = metricscall.theta(spot,iv_call)
#     p2 = metricscall2.theta(spot,iv_call2)
#     p3 = metricscall3.theta(spot,iv_call3)
#     prices.append(p)
#     prices2.append(p2)
#     prices3.append(p3)
#     x.append(t)
#     evalDate = calendar.advance(evalDate, ql.Period(1, ql.Days))
#     evaluation = Evaluation(evalDate, daycounter, calendar)
#     metricscall.set_evaluation(evaluation)
#     metricscall2.set_evaluation(evaluation)
#     metricscall3.set_evaluation(evaluation)
#     t += 1
# df_thetas = pd.DataFrame({'t':x,'theta-atm':prices,'theta-otm':prices2,'theta-itm':prices3}).sort_values(by='t')
# df_thetas.to_csv('../save_results/df_thetas.csv')
"""组合价值随标的变化"""
evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
port = PortfolioBackspread(evalDate, option_call2, option_call, spot, strike, vol, rf, df_theory)
# port = PortfolioBackspread(evalDate, option_call, option_call3, spot, strike, vol, rf, df_theory)

print('ratio ',port.ratio_long,port.ratio_short)
print('init value ',port.init_earning)
spots = np.arange(spot - 0.3, spot + 0.3, 0.005)
npvs = port.pnls_senario_spot(spots)

df_pnl_spots = pd.DataFrame({'spots':spots,'pnl':npvs}).sort_values(by='spots')
df_pnl_spots.to_csv('../save_results/df_pnl_spots-backspread.csv')

f1 = pu.plot_line_chart(spots, [npvs], ['straddle'], 'spot', 'pnl')

"""组合时间价值衰减"""
evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
port = PortfolioBackspread(evalDate, option_call2, option_call, spot, strike, vol, rf, df_theory)
# port = PortfolioBackspread(evalDate, option_call, option_call3, spot, strike, vol, rf, df_theory)

pnls_theta = []
x = []
t = 0
while evalDate <= maturitydt:
    pnl = port.pnl()
    pnls_theta.append(pnl)
    x.append(t)
    evalDate = calendar.advance(evalDate, ql.Period(1, ql.Days))
    port.reset_evaluation(evalDate)
    t += 1

df_pnl_ts = pd.DataFrame({'t':x,'pnl':pnls_theta}).sort_values(by='t')
df_pnl_ts.to_csv('../save_results/df_pnl_ts-backspread.csv')


f2 = pu.plot_line_chart(x, [pnls_theta], ['straddle'], 't', 'npv')

"""组合波动率变化"""
evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
port = PortfolioBackspread(evalDate, option_call2, option_call, spot, strike, vol, rf, df_theory)
# port = PortfolioBackspread(evalDate, option_call, option_call3, spot, strike, vol, rf, df_theory)

npv0 = port.pnl()
pnl_vols = []
vols = np.arange(15.0, 35.0, 0.1)
for vol in vols:
    npv = port.pnl(implied_vol=vol/100.0)
    pnl_vols.append(npv)

df_pnl_vols = pd.DataFrame({'vol':vols,'pnl':pnl_vols}).sort_values(by='vol')
df_pnl_vols.to_csv('../save_results/df_pnl_vols-backspread.csv')


f3 = pu.plot_line_chart(vols, [pnl_vols], ['straddle'], 'vol', 'pnl')

plt.show()
