import math
import datetime
from scipy.stats import norm
from OptionStrategyLib.Util import PricingUtil
from OptionStrategyLib.OptionPricing.Options import EuropeanOption
from back_test.model.constant import Util


class BlackCalculator(object):
    """ European Option Pricing and Metrics """

    def __init__(self, dt_date: datetime.date, spot: float, Option: EuropeanOption,
                 rf: float = 0.03, vol: float = 0.0):
        self.vol = vol
        self.rf = rf
        self.Option = Option
        self.run(dt_date, spot)

    def run(self, dt_date, spot):
        stdDev = PricingUtil.get_std(dt_date, self.Option.dt_maturity, self.vol)
        discount = PricingUtil.get_discount(dt_date, self.Option.dt_maturity, self.rf)
        self.strike = self.Option.strike
        self.forward = spot / discount
        self.stdDev = stdDev
        self.discount = discount
        if self.Option.option_type == Util.TYPE_PUT:
            self.iscall = False
        else:
            self.iscall = True
        if stdDev > 0.0:
            if self.strike == 0.0:
                n_d1 = 0.0
                n_d2 = 0.0
                cum_d1 = 1.0
                cum_d2 = 1.0
            else:
                D1 = math.log(self.forward / self.strike, math.e) / stdDev + 0.5 * stdDev
                D2 = D1 - stdDev
                cum_d1 = norm.cdf(D1)
                cum_d2 = norm.cdf(D2)
                n_d1 = norm.pdf(D1)
                n_d2 = norm.pdf(D2)
        else:
            if self.forward > self.strike:
                cum_d1 = 1.0
                cum_d2 = 1.0
            else:
                cum_d1 = 0.0
                cum_d2 = 0.0
            n_d1 = 0.0
            n_d2 = 0.0

        if self.iscall:
            alpha = cum_d1  ## N(d1)
            dAlpha_dD1 = n_d1  ## n(d1)
            beta = -cum_d2  ## -N(d2)
            dBeta_dD2 = -n_d2  ## -n(d2)
        else:
            alpha = -1.0 + cum_d1  ## -N(-d1)
            dAlpha_dD1 = n_d1  ## n( d1)
            beta = 1.0 - cum_d2  ## N(-d2)
            dBeta_dD2 = -n_d2  ## -n( d2)
        self.alpha = alpha
        self.dAlpha_dD1 = dAlpha_dD1
        self.beta = beta
        self.dBeta_dD2 = dBeta_dD2
        self.x = self.strike
        self.dX_dS = 0.0

    def NPV(self):
        return self.discount * (self.forward * self.alpha + self.x * self.beta)

    def Alpha(self):
        # Replicate portfolio -- component shares of stock,
        # N(d1) for call / -N(-d1) for put
        return self.alpha

    def Beta(self):
        # Replicate portfolio -- component shares of borrowing/lending,
        # -N(d2) for call / N(-d2) for put
        return self.beta

    def Cash(self):
        return self.beta * self.strike * self.discount

    def Delta(self, spot):
        if spot <= 0.0:
            return
        else:
            DforwardDs = self.forward / spot
            temp = self.stdDev * spot
            DalphaDs = self.dAlpha_dD1 / temp
            DbetaDs = self.dBeta_dD2 / temp
            temp2 = DalphaDs * self.forward + self.alpha * DforwardDs + DbetaDs * self.x \
                    + self.beta * self.dX_dS
            delta = self.discount * temp2
            return delta

    def implied_vol(self):

        return

    # 全Delta: dOption/dS = dOption/dS + dOption/dSigma * dSigma/dK
    # 根据SVI模型校准得到的隐含波动率的参数表达式，计算隐含波动率对行权价的一阶倒数（dSigma_dK）
    def delta_total(self, spot, dSigma_dK):
        delta = self.Delta(spot)
        return delta + delta * dSigma_dK

option = EuropeanOption(3000.0,datetime.date(2018,3,1),'put')
black = BlackCalculator(datetime.date(2018,1,1),3000.0,option)