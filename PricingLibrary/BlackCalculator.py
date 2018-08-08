import math
import datetime
from scipy.stats import norm
from back_test.model.constant import OptionType,PricingUtil

""" European Option Pricing and Metrics """


class BlackCalculator(object):

    def __init__(self,
                 dt_eval: datetime.date,
                 dt_maturity: datetime.date,
                 strike: float,
                 type: OptionType,
                 spot: float,
                 vol: float,
                 rf: float = 0.03):
        if type == OptionType.CALL:
            self.iscall = True
        else:
            self.iscall = False
        stdDev = PricingUtil.get_std(dt_eval, dt_maturity, vol)
        discount = PricingUtil.get_discount(dt_eval, dt_maturity, rf)
        self.dt_eval = dt_eval
        self.dt_maturity = dt_maturity
        self.strike = strike
        self.forward = spot / discount
        self.stdDev = stdDev
        self.discount = discount
        self.spot = spot
        if stdDev > 0.0:
            if self.strike == 0.0:
                n_d1 = 0.0
                n_d2 = 0.0
                cum_d1 = 1.0
                cum_d2 = 1.0
                D1 = None
                D2 = None
            else:
                D1 = math.log(self.forward / self.strike, math.e) / stdDev + 0.5 * stdDev
                D2 = D1 - stdDev
                cum_d1 = norm.cdf(D1)
                cum_d2 = norm.cdf(D2)
                n_d1 = norm.pdf(D1)
                n_d2 = norm.pdf(D2)
            self.D1 = D1
            self.D2 = D2
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

    def Delta(self):
        if self.spot <= 0.0:
            return
        elif self.dt_eval == self.dt_maturity:
            if self.iscall:
                if self.strike < self.spot:
                    delta = 1.0
                elif self.strike > self.spot:
                    delta = 0.0
                else:
                    delta = 0.5
            else:
                if self.strike > self.spot:
                    delta = -1.0
                elif self.strike < self.spot:
                    delta = 0.0
                else:
                    delta = -0.5
        else:
            DforwardDs = self.forward / self.spot
            temp = self.stdDev * self.spot
            DalphaDs = self.dAlpha_dD1 / temp
            DbetaDs = self.dBeta_dD2 / temp
            temp2 = DalphaDs * self.forward + self.alpha * DforwardDs + DbetaDs * self.x \
                    + self.beta * self.dX_dS
            delta = self.discount * temp2
        return delta

    def Gamma(self):
        spot = self.spot
        if spot <= 0.0:
            return
        if self.dt_eval == self.dt_maturity:
            return 0.0
        DforwardDs = self.forward / spot
        temp = self.stdDev * spot
        DalphaDs = self.dAlpha_dD1 / temp
        DbetaDs = self.dBeta_dD2 / temp
        D2alphaDs2 = -DalphaDs / spot * (1 + self.D1 / self.stdDev)
        D2betaDs2 = -DbetaDs / spot * (1 + self.D2 / self.stdDev)
        temp2 = D2alphaDs2 * self.forward + 2.0 * DalphaDs * DforwardDs + D2betaDs2 * self.x \
                + 2.0 * DbetaDs * self.dX_dS
        gamma = self.discount * temp2
        return gamma

    # 全Delta: dOption/dS = dOption/dS + dOption/dSigma * dSigma/dK
    # 根据SVI模型校准得到的隐含波动率的参数表达式，计算隐含波动率对行权价的一阶倒数（dSigma_dK）
    # def delta_total(self, dSigma_dK):
    #     delta = self.Delta()
    #     return delta + delta * dSigma_dK


# mdt = datetime.date.today() + datetime.timedelta(days=30*3)
# p = BlackCalculator(datetime.date.today(),mdt,1820,OptionType.PUT,1837,0.13)
# c = BlackCalculator(datetime.date.today(),mdt,1920,OptionType.CALL,1837,0.13)
#
# print(p.NPV())
# print(c.NPV())
