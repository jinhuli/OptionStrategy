import math
from scipy.stats import norm
from PricingLibrary.Util import PricingUtil
from back_test.model.constant import *
from PricingLibrary.BlackCalculator import BlackCalculator

"""  
     Approximated Black 1976 implied standard deviation, i.e.
     volatility*sqrt(timeToMaturity).
     It is calculated using Brenner and Subrahmanyan (1988) and Feinstein
     (1988) approximation for at-the-money forward option, with the extended
     moneyness approximation by Corrado and Miller (1996)
"""


class BlackFormula(object):

    def __init__(self,
                 dt_eval: datetime.date,
                 dt_maturity: datetime.date,
                 strike: float,
                 type: OptionType,
                 spot: float,
                 black_price: float,
                 rf: float = 0.03,
                 displacement: float = 0.0):
        discount = PricingUtil.get_discount(dt_eval, dt_maturity, rf)
        self.dt_eval = dt_eval
        self.dt_maturity = dt_maturity
        self.option_type = type
        self.strike = strike + displacement
        self.forward = spot / discount + displacement
        self.discount = discount
        self.spot = spot
        self.displacement = displacement
        self.black_price = black_price

        if strike == self.forward:
            stddev = black_price / discount * math.sqrt(2.0 * math.pi) / self.forward
        else:
            moneynessDelta = self.option_type.value * (self.forward - strike)
            moneynessDelta_2 = moneynessDelta / 2.0
            temp = black_price / discount - moneynessDelta_2
            moneynessDelta_PI = moneynessDelta * moneynessDelta / math.pi
            temp2 = temp * temp - moneynessDelta_PI
            if temp2 < 0.0:
                # approximation breaks down, 2 alternatives:
                # 1. zero it
                temp2 = 0.0
            # 2. Manaster-Koehler (1982) efficient Newton-Raphson seed
            # return std::fabs(std::log(forward/strike))*std::sqrt(2.0); -- commented out in original C++
            temp2 = math.sqrt(temp2)
            temp += temp2
            temp *= math.sqrt(2.0 * math.pi)
            stddev = temp / (self.forward + strike)
        self.stddev = stddev

    def ImpliedVolApproximation(self):
        return self.stddev / math.sqrt(PricingUtil.get_ttm(self.dt_eval, self.dt_maturity))

    def ImpliedVol(self):
        return

""" Black 1976 implied standard deviation, 
    i.e. volatility*sqrt(timeToMaturity) """


class BlackFormulaImpliedStdDev(object):

    def __init__(self,
                 dt_eval: datetime.date,
                 dt_maturity: datetime.date,
                 strike: float,
                 type: OptionType,
                 spot: float,
                 black_price: float,
                 guess: float,
                 accuracy: float,
                 rf: float = 0.03,
                 displacement: float = 0.0):
        discount = PricingUtil.get_discount(dt_eval, dt_maturity, rf)
        self.dt_eval = dt_eval
        self.dt_maturity = dt_maturity
        self.option_type = type
        self.strike = strike + displacement
        self.forward = spot / discount + displacement
        self.discount = discount
        self.spot = spot
        self.black_price = black_price
        self.guess = guess
        self.accuracy = accuracy
        self.displacement = displacement

        # TODO: SOLVE

    def ImpliedVol(self):
        return

# a = [
# -0.0527 ,
# -0.0238 ,
# -0.0268 ,
# -0.0253 ,
# -0.0426 ,
# -0.0699 ,
# -0.0566 ,
# -0.0525 ,
# -0.1019 ,
# -0.0379 ,
# -0.0302 ,
# -0.0270 ,
# -0.0338 ,
# -0.0064 ,
# -0.0313 ,
# -0.0360 ,
# -0.0376 ,
# -0.0188 ,
# -0.0243 ,
# -0.0230 ,
# -0.0265 ,
# -0.0154 ,
# -0.0150 ,
# -0.0215 ,
# -0.0130 ,
# -0.0170 ,
# -0.0219 ,
# -0.0151 ,
# -0.0229 ,
# -0.0166 ,
# -0.0220 ,
# -0.0187 ,
# -0.0166 ,
# -0.0123 ,
# -0.0170 ,
# -0.0152 ,
# -0.0103 ,
# -0.0158 ,
# -0.0410 ,
# -0.0266 ,
# -0.0276
# ]
# vols = []
# dt_eval = datetime.date(2018, 7, 6)
# dt_maturity = dt_eval + datetime.timedelta(days=30)
# strike = 4000
# spot = 4000
# for i in a:
#     black_price = spot*(-i)
#     type = OptionType.CALL
#     black = BlackFormula(dt_eval, dt_maturity, strike, type, spot, black_price)
#     std = black.stddev
#     vol = std / math.sqrt(PricingUtil.get_ttm(dt_eval, dt_maturity))
#     # print(vol * 100, '%')
#     # black = BlackCalculator(dt_eval, dt_maturity, strike, type, spot, vol)
#     # print(black.NPV())
#     vols.append(vol)
#
# df = pd.DataFrame()
# df['vol'] = vols
# df.to_csv('../vols.csv')
# print(df)


