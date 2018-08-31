import math
import datetime
from PricingLibrary.BlackFormular import BlackFormula
from PricingLibrary.EngineQuantlib import QlBlackFormula
from PricingLibrary.BlackCalculator import BlackCalculator
import back_test.model.constant as c

dt_eval = datetime.date(2017, 1, 1)
dt_maturity = datetime.date(2017, 4, 1)
spot_price = 120
strike_price = 100
volatility = 0.3  # the historical vols or implied vols
dividend_rate = 0
risk_free_rate = 0.03
option_price = 30.0

black_formula = BlackFormula(dt_eval, dt_maturity, c.OptionType.CALL,spot_price,strike_price,option_price)
iv1 = black_formula.ImpliedVolApproximation()
estimated_price1 = BlackCalculator(dt_eval,dt_maturity,strike_price,c.OptionType.CALL,spot_price,iv1).NPV()
ql_black =  QlBlackFormula(dt_eval, dt_maturity, c.OptionType.CALL,spot_price,strike_price)
iv2, estimated_price2 = ql_black.estimate_vol(option_price)

print(iv1,estimated_price1)
print(iv2,estimated_price2)