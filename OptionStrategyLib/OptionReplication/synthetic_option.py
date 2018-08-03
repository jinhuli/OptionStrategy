import numpy as np
from copy import copy
from PricingLibrary.BlackCalculator import BlackCalculator
from PricingLibrary.Options import EuropeanOption
from back_test.model.base_future_coutinuous import BaseFutureCoutinuous
from back_test.model.constant import Util, FrequentType, DeltaBound, BuyWrite
from PricingLibrary.Util import PricingUtil
import math
"""
基于期货构造合成期权，行权价可根据现货指数点位设定从而包含基差的影响。
"""


class SytheticOption(BaseFutureCoutinuous):

    def __init__(self, df_c1_minute,
                 df_c1_daily=None,
                 df_futures_all_daily=None,
                 df_index_daily=None,
                 rf=0.03,
                 frequency=FrequentType.MINUTE
                 ):
        super().__init__(df_future_c1=df_c1_minute, df_future_c1_daily=df_c1_daily,
                         df_futures_all_daily=df_futures_all_daily, df_underlying_index_daily=df_index_daily,
                         rf=rf, frequency=frequency)
        self.synthetic_ratio = 0.0
        self.synthetic_unit: int = 0
        self.amt_option = 0
    def get_c1_with_start_dates(self):
        df = self.df_daily_data.drop_duplicates(Util.ID_INSTRUMENT)[[Util.DT_DATE, Util.ID_INSTRUMENT]]
        return df

    def get_c1_with_end_dates(self):
        df = self.df_daily_data.drop_duplicates(Util.ID_INSTRUMENT, 'last')[[Util.DT_DATE, Util.ID_INSTRUMENT]]
        return df

    def get_black_delta(self, option: EuropeanOption, vol: float, spot: float=None):
        if spot is None:
            spot = self.mktprice_close()
        black = BlackCalculator(self.eval_date, option.dt_maturity, option.strike,
                                option.option_type, spot, vol, self.rf)
        delta = black.Delta()
        return delta

    # Get synthetic position in trade unit
    def get_synthetic_unit(self, delta, buywrite=BuyWrite.BUY) -> int:
        # hedge_scale : total notional amt to hedge in RMB
        # amt_position = delta * self.amt_option
        # trade_unit = np.floor(amt_position / (self.mktprice_close() * self.multiplier()))
        trade_unit = np.floor(buywrite.value * delta * self.amt_option / self.multiplier())
        # self.synthetic_ratio = delta
        self.synthetic_unit = trade_unit
        return trade_unit

    # Get hedge position in trade unit
    def get_hedge_position(self, delta, buywrite=BuyWrite.BUY):
        # hedge_scale : total notional amt to hedge in RMB
        return - self.get_synthetic_unit(delta, buywrite)

    def get_synthetic_option_rebalancing_unit(self, delta: float,
                                              option: EuropeanOption,
                                              vol:float,
                                              spot:float,
                                              delta_bound: DeltaBound,
                                              buywrite:BuyWrite=BuyWrite.BUY) -> int:
        hold_unit = self.synthetic_unit
        synthetic_unit = self.get_synthetic_unit(delta,buywrite)
        d_unit = synthetic_unit - hold_unit
        if delta_bound == DeltaBound.WHALLEY_WILLMOTT:
            bound = self.whalley_wilmott(self.eval_date,option,vol,spot)*self.amt_option / self.multiplier()
            if abs(d_unit)>bound:
                self.synthetic_unit = synthetic_unit
                return d_unit
            else:
                # print(self.eval_datetime,' within hedge bound ',d_unit,bound)
                return 0
        else:
            return d_unit

    def whalley_wilmott(self, eval_date, option, vol, spot=None, rho=0.5, fee=5.0 / 10000.0):
        if spot is None:
            spot = self.mktprice_close()
        black = BlackCalculator(self.eval_date, option.dt_maturity, option.strike,
                                option.option_type, spot, vol, self.rf)
        gamma = black.Gamma()
        ttm = PricingUtil.get_ttm(eval_date, option.dt_maturity)
        H = (1.5 * math.exp(-self.rf * ttm) * fee * spot * (gamma ** 2) / rho) ** (1 / 3)
        return H

    def portfolio_exposure(self, hedge_holding_unit):
        # TODO
        return

    def replicated_option_value(self, option: EuropeanOption, vol):
        spot = self.mktprice_close()
        black = BlackCalculator(self.eval_date, option.dt_maturity, option.strike,
                                option.option_type, spot, vol, self.rf)
        npv = black.NPV()
        return npv


