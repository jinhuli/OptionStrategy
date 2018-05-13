from abc import ABCMeta, abstractmethod
import QuantLib as ql
import numpy as np
import datetime
from OptionStrategyLib.OptionPricing.Options import OptionPlainEuropean
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
from OptionStrategyLib.OptionPricing import OptionPricingUtil as util
import math


class Portfolio(object):
    __metaclass__ = ABCMeta

    def __init__(self, evalDate, spot, strike, vol, rf):
        calendar = ql.China()
        daycounter = ql.ActualActual()
        self.evaluation = Evaluation(evalDate, daycounter, calendar)
        self.spot = spot
        self.strike = strike
        self.vol = vol
        self.rf = rf
        self.engineType = 'AnalyticEuropeanEngine'
        self.init_v = 100.0

    def reset_evaluation(self, evalDate):
        self.evaluation = Evaluation(evalDate, daycounter, calendar)

    def reset_vol(self, vol):
        self.vol = vol

    def reset_spot(self, spot):
        self.spot = spot

    @abstractmethod
    def delta_neutral(self):
        return

    @abstractmethod
    def pnl(self, spot=None):
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

    def __init__(self, evalDate, option_call, option_put, spot, strike, vol, rf):
        Portfolio.__init__(self, evalDate, spot, strike, vol, rf)
        self.option_call = option_call
        self.option_put = option_put
        self.metrics_call = OptionMetrics(option_call)
        self.metrics_put = OptionMetrics(option_put)
        self.delta_neutral()

    def reset_option(self, option_call, option_put):
        self.option_call = option_call
        self.option_put = option_put
        self.metrics_call = OptionMetrics(option_call)
        self.metrics_put = OptionMetrics(option_put)
        self.delta_neutral()

    def delta_neutral(self):
        delta_call = self.metrics_call.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        delta_put = self.metrics_put.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        self.ratio_call = 100.0
        # self.ratio_put = -np.floor(100.0*delta_call / delta_put)
        self.ratio_put = -100.0 * delta_call / delta_put
        p_put = self.metrics_put.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        p_call = self.metrics_call.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        self.init_value = self.ratio_call * p_call + self.ratio_put * p_put

    def pnl(self, spot=None):
        if spot == None: spot = self.spot
        p_put = self.metrics_put.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        p_call = self.metrics_call.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        pnl = self.ratio_call * p_call + self.ratio_put * p_put - self.init_value
        return pnl

    def delta(self):
        delta_call = self.metrics_call.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        delta_put = self.metrics_put.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        port_delta = self.ratio_call * delta_call + self.ratio_put * delta_put
        return port_delta


class PortfolioBackspread(Portfolio):

    def __init__(self, evalDate, option_long, option_short, spot, strike, vol, rf):
        Portfolio.__init__(self, evalDate, spot, strike, vol, rf)
        self.option_long = option_long
        self.option_short = option_short
        self.metrics_long = OptionMetrics(option_long)
        self.metrics_short = OptionMetrics(option_short)
        self.delta_neutral()

    def reset_option(self, option_long, option_short):
        self.option_long = option_long
        self.option_short = option_short
        self.metrics_long = OptionMetrics(option_long)
        self.metrics_short = OptionMetrics(option_short)
        self.delta_neutral()

    def delta_neutral(self):
        delta_long = self.metrics_long.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        delta_short = self.metrics_short.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        self.ratio_long = 100.0
        self.ratio_short = 100.0 * delta_long / delta_short
        p_long = self.metrics_long.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        p_short = self.metrics_short.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        self.init_value_long = self.ratio_long * p_long
        self.init_value_short = self.ratio_short * p_short
        self.init_pnl = self.init_value_short - self.init_value_long
        print('init pnl : ', self.init_pnl)

    def pnl(self, spot=None):
        if spot == None: spot = self.spot
        p_long = self.metrics_long.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        p_short = self.metrics_short.option_price(self.evaluation, self.rf, spot, self.vol, self.engineType)
        v_long = p_long * self.ratio_long
        v_short = p_short * self.ratio_short
        pnl = v_long - self.init_value_long + self.init_value_short - v_short + self.init_pnl
        print(p_long, p_short)
        return pnl

    def delta(self):
        delta_call = self.metrics_long.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        delta_put = self.metrics_short.delta(self.evaluation, self.rf, self.spot, self.engineType, self.vol)
        port_delta = self.ratio_long * delta_call - self.ratio_short * delta_put
        return port_delta


pu = PlotUtil()
eval_date = datetime.date(2018, 5, 11)
mdt = datetime.date(2018, 5, 27)
spot = 2.716  # 2018-3-1 50etf close price
strike = 2.7
strike2 = 3.0
option_price = 0.0546
vol = 0.21
rf = 0.03

engineType = 'AnalyticEuropeanEngine'
calendar = ql.China()
daycounter = ql.ActualActual()
evalDate = ql.Date(eval_date.day, eval_date.month, eval_date.year)
maturitydt = ql.Date(mdt.day, mdt.month, mdt.year)
evaluation = Evaluation(evalDate, daycounter, calendar)

option_call = OptionPlainEuropean(strike, maturitydt, ql.Option.Call)

option_put = OptionPlainEuropean(strike, maturitydt, ql.Option.Put)
option_call2 = OptionPlainEuropean(strike2, maturitydt, ql.Option.Call)
metrics = OptionMetrics(option_call2, rf, engineType).set_evaluation(evaluation)
iv = metrics.implied_vol(spot, option_price)
vega = metrics.vega(spot, vol)
vega2 = metrics.vega_effective(spot,vol)
rho = metrics.rho(spot,vol)
print(vega,vega2,rho)
theta = metrics.theta(spot, iv)

yield_curve = ql.FlatForward(evalDate,
                             rf,
                             daycounter)
T = yield_curve.dayCounter().yearFraction(evalDate,
                                          maturitydt)
discount = yield_curve.discount(T)
strikepayoff = ql.PlainVanillaPayoff(ql.Option.Call, strike)
stddev = vol * math.sqrt(T)
black = ql.BlackCalculator(strikepayoff, spot, stddev, discount)
theta2 = black.theta(spot,T)/252
print(iv, vega, theta,theta2)
