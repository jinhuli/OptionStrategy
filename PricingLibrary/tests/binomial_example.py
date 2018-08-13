from unittest import TestCase
from PricingLibrary.BinomialModel import BinomialTree
from PricingLibrary.BlackCalculator import BlackCalculator
from back_test.model.constant import OptionType, OptionExerciseType, QuantlibUtil
import datetime
import QuantLib as ql
import numpy as np
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt


pu = PlotUtil()
dt_eval = datetime.date(2017, 1, 1)
dt_maturity = datetime.date(2017, 4, 1)
spot_price = 120
strike_price = 120
volatility = 0.3  # the historical vols or implied vols
dividend_rate = 0
risk_free_rate = 0.03
# steps = 800
maturity_date = QuantlibUtil.to_ql_date(dt_maturity)

option_type = ql.Option.Put

day_count = ql.ActualActual()
calendar = ql.NullCalendar()

calculation_date = QuantlibUtil.to_ql_date(dt_eval)
# print(day_count.yearFraction(calculation_date, maturity_date))
ql.Settings.instance().evaluationDate = calculation_date
payoff = ql.PlainVanillaPayoff(option_type, strike_price)
settlement = calculation_date

am_exercise = ql.AmericanExercise(settlement, maturity_date)
american_option = ql.VanillaOption(payoff, am_exercise)
eu_exercise = ql.EuropeanExercise(maturity_date)
european_option = ql.VanillaOption(payoff, eu_exercise)

spot_handle = ql.QuoteHandle(
    ql.SimpleQuote(spot_price)
)
flat_ts = ql.YieldTermStructureHandle(
    ql.FlatForward(calculation_date, risk_free_rate, day_count)
)
dividend_yield = ql.YieldTermStructureHandle(
    ql.FlatForward(calculation_date, dividend_rate, day_count)
)
flat_vol_ts = ql.BlackVolTermStructureHandle(
    ql.BlackConstantVol(calculation_date, calendar, volatility, day_count)
)
bsm_process = ql.BlackScholesMertonProcess(spot_handle,
                                           dividend_yield,
                                           flat_ts,
                                           flat_vol_ts)


list_bm_euro = []
list_ql_euro = []
list_bm_ame = []
list_ql_ame = []
x = list(np.arange(5,800,10))
for steps in x:
    steps = int(steps)
    print(steps)
    american_binomial_tree = BinomialTree(steps, dt_eval, dt_maturity,
                                          OptionType.PUT, OptionExerciseType.AMERICAN, spot_price, strike_price, volatility)
    american_binomial_tree.initialize()
    american_binomial_tree.step_back(0)
    list_bm_ame.append(american_binomial_tree.NPV())
    # print(american_binomial_tree.T)
    # print("american binomial_tree price", american_binomial_tree.NPV())
    european_binomial_tree = BinomialTree(steps, dt_eval, dt_maturity,
                                          OptionType.PUT, OptionExerciseType.EUROPEAN, spot_price, strike_price, volatility)
    european_binomial_tree.initialize()
    european_binomial_tree.step_back(0)
    # print("european binomial_tree price", european_binomial_tree.NPV())
    list_bm_euro.append(european_binomial_tree.NPV())
    # black = BlackCalculator(dt_eval, dt_maturity, strike_price, OptionType.PUT, spot_price, volatility)
    # print("european blackcalculator price", black.NPV())


    binomial_engine = ql.BinomialVanillaEngine(bsm_process, "crr", steps)
    black_engine = ql.AnalyticEuropeanEngine(bsm_process)
    american_option.setPricingEngine(binomial_engine)
    # print("american quantlib price(BinomialVanillaEngine)", american_option.NPV())
    european_option.setPricingEngine(binomial_engine)
    # print("european quantlib price(BinomialVanillaEngine)", european_option.NPV())
    # european_option.setPricingEngine(black_engine)
    # print("european quantlib price(blackcalculator)", european_option.NPV())
    list_ql_euro.append(european_option.NPV())
    list_ql_ame.append(american_option.NPV())


pu.plot_line_chart(x,[list_bm_euro,list_ql_euro],['bimonial euro','quantpib euro'],'steps')
pu.plot_line_chart(x,[list_bm_ame,list_ql_ame],['bimonial ame','quantpib ame'],'steps')
plt.show()