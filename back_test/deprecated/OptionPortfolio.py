import copy

import numpy as np

from back_test.deprecated.BktUtil import BktUtil


class OptionPortfolio(object):

    def __init__(self,open_date):
        self.util = BktUtil()
        self.open_date = open_date
        self.portfolio_combinations = {}
        self.optionset = []
        self.unit_portfolio = None
        self.underlying = None
        self.delta = None
        self.theta = None
        self.vega = None


class Straddle(OptionPortfolio):

    def __init__(self,open_date,call,put,delta_exposure,long_short):
        OptionPortfolio.__init__(self,open_date)
        self.option_call = call
        self.option_put = put
        self.optionset = [call,put]
        self.invest_ratio_call = None
        self.unit_call = None
        self.invest_ratio_put = None
        self.unit_put = None
        self.long_short = long_short
        self.rebalancing(delta_exposure)

    "标的价格大幅运动的情况下，调整行权价组和"
    def update_portfolio(self,option_call,option_put,long_short):
        self.liquidate_put = copy.copy(self.option_put)
        self.liquidate_call = copy.copy(self.option_call)
        self.option_put = option_put
        self.option_call = option_call
        self.long_short = long_short
        self.optionset = [self.option_call,self.option_put]
        self.unit_long = None
        self.unit_short = None


    def rebalancing(self,delta_exposure):
        if delta_exposure == 0.0:
            self.delta_neutral_rebalancing()
        else:
            self.delta_exposure_rebalancing(delta_exposure)

    def delta_neutral_rebalancing(self):
        if self.option_call == None or self.option_put == None:
            return
        delta_call = self.option_call.get_delta()
        delta_put = self.option_put.get_delta()
        self.invest_ratio_call = 1.0
        if np.isnan(delta_put) or np.isnan(delta_call) \
                or delta_call == None or delta_put == None or delta_put ==0.0 or delta_call == 0.0:
            if self.invest_ratio_put == None:
                self.invest_ratio_put = 1.0
        else:
            self.invest_ratio_put = -delta_call / delta_put

    def delta_exposure_rebalancing(self,delta_exposure):
        if self.option_call == None or self.option_put == None:
            return
        delta_call = self.option_call.get_delta()
        delta_put = self.option_put.get_delta()
        if delta_exposure >= delta_call:
            self.invest_ratio_put = 0.0
            self.invest_ratio_call = 1.0

        elif delta_exposure <= delta_put:
            self.invest_ratio_put = 1.0
            self.invest_ratio_call = 0.0
        else:
            self.invest_ratio_call = 1.0
            self.invest_ratio_put = (delta_exposure-delta_call)/delta_put


class Calls(OptionPortfolio):

    def __init__(self,open_date,callset,cd_long_short,cd_weighted='equal_unit'):
        OptionPortfolio.__init__(self,open_date)
        self.optionset = callset
        self.cd_weighted = cd_weighted
        self.long_short = cd_long_short


class Puts(OptionPortfolio):

    def __init__(self,open_date,callset,cd_long_short,cd_weighted='equal_unit'):
        OptionPortfolio.__init__(self,open_date)
        self.optionset = callset
        self.cd_weighted = cd_weighted
        self.long_short = cd_long_short


class CalandarSpread(OptionPortfolio):

    def __init__(self,open_date,option_mdt1,option_mdt2,option_type):
        OptionPortfolio.__init__(self,open_date)
        self.option_set = [option_mdt1,option_mdt2]
        self.option_mdt1 = option_mdt1
        self.option_mdt2 = option_mdt2
        self.option_type = option_type


class BackSpread(OptionPortfolio):

    def __init__(self,open_date,option_buy,option_write,option_type,delta_exposure=0.0):
        OptionPortfolio.__init__(self,open_date)
        self.option_long = option_buy
        self.option_short = option_write
        self.optionset = [option_buy,option_write]
        self.invest_ratio_long = None
        self.unit_long = None
        self.invest_ratio_short = None
        self.unit_short = None
        self.option_type = option_type
        self.rebalancing(delta_exposure)

    def rebalancing(self,delta_exposure=0.0):
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


class Collar(OptionPortfolio):

    def __init__(self,open_date,buy_put,write_call,underlying):
        OptionPortfolio.__init__(self,open_date)
        self.buy_put = buy_put
        self.write_call = write_call
        self.underlying = underlying
        self.optionset = [self.buy_put,self.write_call]
        self.unit_call = None
        self.unit_put = None
        self.unit_underlying = None
        self.liquidate_put = None
        self.liquidate_call = None

    "标的价格大幅运动的情况下，调整buy write的期权合约"
    def update_portfolio(self,buy_put,write_call):
        self.liquidate_put = copy.copy(self.buy_put)
        self.liquidate_call = copy.copy(self.write_call)
        self.buy_put = buy_put
        self.write_call = write_call
        self.optionset = [self.buy_put,self.write_call]
        self.unit_long = None
        self.unit_short = None

    def liquidate(self):
        self.buy_put = None
        self.write_call = None
        self.underlying = None
        self.optionset = []
        self.unit_long = None
        self.unit_short = None






