from back_test.bkt_util import BktUtil
import numpy as np

class OptionPortfolio(object):

    def __init__(self,open_date):
        self.util = BktUtil()
        self.open_date = open_date
        self.portfolio_combinations = {}
        self.optionset = []
        self.unit_portfolio = None
        self.delta = None
        self.theta = None
        self.vega = None


class Straddle(OptionPortfolio):

    def __init__(self,open_date,call,put):
        OptionPortfolio.__init__(self,open_date)
        self.option_call = call
        self.option_put = put
        self.optionset = [call,put]
        self.invest_ratio_call = None
        self.unit_call = None
        self.invest_ratio_put = None
        self.unit_put = None
        self.delta_neutral_rebalancing()

    def delta_neutral_rebalancing(self):
        delta_call = self.option_call.get_delta()
        delta_put = self.option_put.get_delta()
        self.invest_ratio_call = 1.0
        if np.isnan(delta_put) or np.isnan(delta_call) \
                or delta_call == None or delta_put == None or delta_put ==0.0 or delta_call == 0.0:
            if self.invest_ratio_put == None:
                self.invest_ratio_put = 1.0
        else:
            self.invest_ratio_put = -delta_call / delta_put


class BackSpread(OptionPortfolio):

    def __init__(self,open_date,long,short,option_type):
        OptionPortfolio.__init__(self,open_date)
        self.option_long = long
        self.option_short = short
        self.optionset = [long,short]
        self.invest_ratio_long = None
        self.unit_long = None
        self.invest_ratio_short = None
        self.unit_short = None
        self.option_type = option_type
        self.delta_neutral_rebalancing()

    def delta_neutral_rebalancing(self):
        # if self.option_long.implied_vol == None: self.option_long.update_implied_vol()
        # if self.option_short.implied_vol == None: self.option_short.update_implied_vol()
        # iv = (self.option_long.implied_vol + self.option_short.implied_vol)/2
        # delta_long = self.option_long.get_delta(iv)
        # delta_short = self.option_short.get_delta(iv)
        delta_long = self.option_long.get_delta()
        delta_short = self.option_short.get_delta()
        self.invest_ratio_long = 1.0
        if np.isnan(delta_short) or np.isnan(delta_long) \
                or delta_short == None or delta_long == None or delta_short ==0.0 or delta_long == 0.0:
            if self.invest_ratio_short == None:
                self.invest_ratio_short = 1.0
        else:
            self.invest_ratio_short = delta_long / delta_short


class Calls(OptionPortfolio):

    def __init__(self,open_date,callset,cd_weighted='equal_unit'):
        OptionPortfolio.__init__(self,open_date)
        self.optionset = callset
        self.cd_weighted = cd_weighted


class Puts(OptionPortfolio):

    def __init__(self,open_date,callset,cd_weighted='equal_unit'):
        OptionPortfolio.__init__(self,open_date)
        self.optionset = callset
        self.cd_weighted = cd_weighted


class CalandarSpread(OptionPortfolio):

    def __init__(self,open_date,option_mdt1,option_mdt2,option_type):
        OptionPortfolio.__init__(self,open_date)
        self.option_set = [option_mdt1,option_mdt2]
        self.option_mdt1 = option_mdt1
        self.option_mdt2 = option_mdt2
        self.option_type = option_type