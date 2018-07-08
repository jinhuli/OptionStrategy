import math
from scipy.stats import norm
from OptionStrategyLib.Util import PricingUtil
from back_test.model.constant import *
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator

"""  
     Approximated Black 1976 implied standard deviation, i.e.
     volatility*sqrt(timeToMaturity).
     It is calculated using Brenner and Subrahmanyan (1988) and Feinstein
     (1988) approximation for at-the-money forward option, with the extended
     moneyness approximation by Corrado and Miller (1996)
"""


class BlackFormulaImpliedStdDevApproximation(object):

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

    def ImpliedVol(self):
        return self.stddev / math.sqrt(PricingUtil.get_ttm(self.dt_eval, self.dt_maturity))


"""   Black 1976 implied standard deviation, 
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

# dt_eval = datetime.date(2018, 7, 6)
# dt_maturity = datetime.date(2018, 7, 25)
# strike = 2.45
# spot = 2.425
# black_price = 0.04
# type = OptionType.CALL
# blackImplied = BlackFormulaImpliedStdDevApproximation(dt_eval, dt_maturity, strike, type, spot, black_price)
# std = blackImplied.stddev
# vol = std / math.sqrt(PricingUtil.get_ttm(dt_eval, dt_maturity))
# print(vol * 100, '%')
# black = BlackCalculator(dt_eval, dt_maturity, strike, type, spot, vol)
# print(black.NPV())
