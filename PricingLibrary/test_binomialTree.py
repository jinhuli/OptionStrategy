from unittest import TestCase
from PricingLibrary.BinomialModel import BinomialTree
from PricingLibrary.BlackCalculator import BlackCalculator
from back_test.model.constant import OptionType, OptionExerciseType, QuantlibUtil
import datetime
import QuantLib as ql
from PricingLibrary.EngineQuantlib import QlBinomial

class TestBinomialTree(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.size = 800
        cls.max_index = 9
        cls.strike = 20

    def test_step_back(self):
        dt_eval = datetime.date(2017, 1, 1)
        dt_maturity = datetime.date(2017, 4, 1)
        spot_price = 120
        strike_price = 120
        volatility = 0.3  # the historical vols or implied vols
        dividend_rate = 0
        risk_free_rate = 0.03
        steps = self.size

        american_binomial_tree = BinomialTree(steps, dt_eval, dt_maturity,
                                              OptionType.PUT, OptionExerciseType.AMERICAN, spot_price, strike_price, volatility)
        american_binomial_tree.initialize()
        american_binomial_tree.step_back(0)
        print(american_binomial_tree.T)
        print("american binomial_tree price", american_binomial_tree.NPV())
        european_binomial_tree = BinomialTree(steps, dt_eval, dt_maturity,
                                              OptionType.PUT, OptionExerciseType.EUROPEAN, spot_price, strike_price, volatility)
        european_binomial_tree.initialize()
        european_binomial_tree.step_back(0)
        print("european binomial_tree price", european_binomial_tree.NPV())

        black = BlackCalculator(dt_eval, dt_maturity,strike_price,OptionType.PUT,spot_price,volatility)
        print("european blackcalculator price", black.NPV())

        maturity_date = QuantlibUtil.to_ql_date(dt_maturity)

        option_type = ql.Option.Put

        day_count = ql.ActualActual()
        calendar = ql.NullCalendar()

        calculation_date = QuantlibUtil.to_ql_date(dt_eval)
        print(day_count.yearFraction(calculation_date,maturity_date))
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
        binomial_engine = ql.BinomialVanillaEngine(bsm_process, "crr", steps)
        black_engine = ql.AnalyticEuropeanEngine(bsm_process)
        american_option.setPricingEngine(binomial_engine)
        print("american quantlib price(BinomialVanillaEngine)", american_option.NPV())
        european_option.setPricingEngine(binomial_engine)
        print("european quantlib price(BinomialVanillaEngine)", european_option.NPV())
        european_option.setPricingEngine(black_engine)
        print("european quantlib price(blackcalculator)", european_option.NPV())

    def test_estimate_vol(self):
        dt_eval = datetime.date(2018, 8, 7)
        dt_maturity = datetime.date(2018, 12, 7)
        spot_price = 3224
        strike_price = 3200
        init_vol = 0.1755  # the historical vols or implied vols
        risk_free_rate = 0.03
        steps = self.size
        american_binomial_tree = BinomialTree(steps,dt_eval,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,spot_price,strike_price,init_vol,risk_free_rate)
        american_binomial_tree.initialize()
        vol, price = american_binomial_tree.estimate_vol(105.5)
        print(vol)
        print(price)
        american_ql = QlBinomial(steps,dt_eval,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,spot_price,strike_price,init_vol,risk_free_rate)
        vol, price = american_ql.estimate_vol(105.5)
        print(vol)
        print(price)
        american_binomial_tree = BinomialTree(steps,dt_eval,dt_maturity,OptionType.CALL,OptionExerciseType.AMERICAN,spot_price,strike_price,init_vol,risk_free_rate)
        american_binomial_tree.initialize()
        vol, price = american_binomial_tree.estimate_vol(139.5)
        print(vol)
        print(price)
        american_ql = QlBinomial(steps,dt_eval,dt_maturity,OptionType.CALL,OptionExerciseType.AMERICAN,spot_price,strike_price,init_vol,risk_free_rate)
        vol, price = american_ql.estimate_vol(139.5)
        print(vol)
        print(price)
