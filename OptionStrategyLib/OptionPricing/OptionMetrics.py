import QuantLib as ql
from OptionStrategyLib.OptionPricing import OptionPricingUtil as util


class OptionMetrics:
    def __init__(self, option, rf, engineType):
        self.Option = option
        self.rf = rf
        self.engineType = engineType
        # self.implied_vol = -1.0

    def reset_option(self, option):
        self.Option = option

    def set_evaluation(self, evaluation):
        self.evaluation = evaluation
        return self

    def option_price(self, spot_price, vol):
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, vol)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        p = option.NPV()
        return p

    def implied_vol(self, spot_price, option_price):
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, 0.0)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        # print('pricing metrics eval date : ',self.evaluation.evalDate)
        try:
            implied_vol = option.impliedVolatility(option_price, process, 1.0e-3, 300, 0.05, 5.0)
        except:
            implied_vol = 0.0
        # self.implied_vol = implied_vol
        return implied_vol

    def delta(self, spot_price, implied_vol):
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, implied_vol)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        delta = option.delta()
        return delta

    def effective_delta(self, spot_price, implied_vol, dS=0.001):
        option_ql = self.Option.option_ql
        process1 = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price + dS, implied_vol)
        process2 = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price - dS, implied_vol)
        engine1 = util.get_engine(process1, self.engineType)
        engine2 = util.get_engine(process2, self.engineType)
        option_ql.setPricingEngine(engine1)
        option_price1 = option_ql.NPV()
        option_ql.setPricingEngine(engine2)
        option_price2 = option_ql.NPV()
        delta_eff = (option_price1 - option_price2) / (2 * dS)
        return delta_eff

    def theta(self, spot_price,implied_vol):
        """The rate of change in the fair value of the option per one day decrease
        in the option time when other variables remain constant.
        This is the negative of the derivative of the option price
        with respect to the option time (in years), divided by 365."""
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, implied_vol)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        theta = option.theta()/365.0
        return theta

    def vega(self, spot_price, implied_vol):
        """The rate of change in the fair value of the option per 1% change in volatility
         when other variables remain constant.
         This is the derivative of the option price with respect to the volatility,
         divided by 100."""
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, implied_vol)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        vega = option.vega()/100.0
        # price1 = self.option_price(spot_price, implied_vol)
        # price2 = self.option_price(spot_price, implied_vol + 0.01)
        # vega = price2 - price1
        return vega

    def vega_effective(self, spot_price, implied_vol):
        # option = self.Option.option_ql
        # process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, implied_vol)
        # engine = util.get_engine(process, self.engineType)
        # option.setPricingEngine(engine)
        # vega = option.vega()
        price1 = self.option_price(spot_price, implied_vol)
        price2 = self.option_price(spot_price, implied_vol + 0.01)
        vega = price2 - price1
        return vega
    def rho(self, spot_price, implied_vol):
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, implied_vol)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        rho = option.rho()
        return rho

    def gamma(self, spot_price, implied_vol):
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, implied_vol)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        gamma = option.gamma()
        return gamma

    def vomma(self, spot_price, implied_vol):
        option = self.Option.option_ql
        process = self.evaluation.get_bsmprocess_cnstvol(self.rf, spot_price, implied_vol)
        engine = util.get_engine(process, self.engineType)
        option.setPricingEngine(engine)
        vomma = option.vomma()
        return vomma
