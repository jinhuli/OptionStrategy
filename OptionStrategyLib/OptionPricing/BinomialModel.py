import math
from OptionStrategyLib.Util import PricingUtil
from back_test.model.constant import *


class BinomialModel(object):

    def __init__(self,
                 dt_eval: datetime.date,
                 dt_maturity: datetime.date,
                 strike: float,
                 type: OptionType,
                 spot: float,
                 black_price: float,
                 rf: float = 0.03,
                 ):
        self.dt_eval = dt_eval
        self.dt_maturity = dt_maturity
        self.option_type = type
        self.strike = strike
        self.spot = spot
        self.black_price = black_price