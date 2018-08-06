from unittest import TestCase
from PricingLibrary.BinomialModel import BinomialTree
from PricingLibrary.BlackCalculator import BlackCalculator
from back_test.model.constant import OptionType, OptionExerciseType
import datetime
import QuantLib as ql


class TestBinomialTree(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.size =800
        cls.max_index = 9
        cls.strike = 20

    def test_step_back(self):
        american_binomial_tree = BinomialTree(self.size, datetime.date(2017, 1, 1), datetime.date(2017, 4, 1),
                                              OptionType.PUT, OptionExerciseType.AMERICAN, 120, 120, 0.3)
        american_binomial_tree.initialize()
        american_binomial_tree.step_back(0)
        print("american binomial_tree price", american_binomial_tree.NPV())
        european_binomial_tree = BinomialTree(self.size, datetime.date(2017, 1, 1), datetime.date(2017, 4, 1),
                                              OptionType.PUT, OptionExerciseType.EUROPEAN, 120, 120, 0.3)
        european_binomial_tree.initialize()
        european_binomial_tree.step_back(0)
        print("european binomial_tree price", european_binomial_tree.NPV())

        black = BlackCalculator(datetime.date(2017, 1, 1), datetime.date(2017, 4, 1),120,OptionType.PUT,120,0.3)
        print("european blackcalculator price", black.NPV())

        maturity_date = ql.Date(1, 4, 2017)
        spot_price = 120
        strike_price = 120
        volatility = 0.3  # the historical vols or implied vols
        dividend_rate = 0
        option_type = ql.Option.Put

        risk_free_rate = 0.03
        day_count = ql.ActualActual()
        calendar = ql.NullCalendar()

        calculation_date = ql.Date(1, 1, 2017)
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
        steps = 100
        binomial_engine = ql.BinomialVanillaEngine(bsm_process, "crr", steps)
        black_engine = ql.AnalyticEuropeanEngine(bsm_process)
        american_option.setPricingEngine(binomial_engine)
        print("american quantlib price(BinomialVanillaEngine)", american_option.NPV())
        european_option.setPricingEngine(binomial_engine)
        print("european quantlib price(BinomialVanillaEngine)", european_option.NPV())
        european_option.setPricingEngine(black_engine)
        print("european quantlib price(blackcalculator)", european_option.NPV())


