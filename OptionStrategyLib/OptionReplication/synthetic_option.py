import numpy as np
from copy import copy
from PricingLibrary.BlackCalculator import BlackCalculator
from PricingLibrary.Options import EuropeanOption
from back_test.model.base_future_coutinuous import BaseFutureCoutinuous
from back_test.model.constant import Util, FrequentType, DeltaBound

"""
基于期货构造合成期权，行权价可根据现货指数点位设定从而包含基差的影响。
"""


class SytheticOption(BaseFutureCoutinuous):

    def __init__(self, df_c1_minute,
                 df_c1_daily=None,
                 df_futures_all_daily=None,
                 df_index_daily=None,
                 rf=0.03,
                 frequency=FrequentType.MINUTE,
                 amt_option = 1
                 ):
        super().__init__(df_future_c1=df_c1_minute, df_future_c1_daily=df_c1_daily,
                         df_futures_all_daily=df_futures_all_daily, df_underlying_index_daily=df_index_daily,
                         rf=rf, frequency=frequency)
        self.synthetic_ratio = 0.0
        self.synthetic_unit: int = 0
        self.amt_option = amt_option

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
    def get_synthetic_unit(self, delta) -> int:
        # hedge_scale : total notional amt to hedge in RMB
        amt_position = delta * self.amt_option
        # trade_unit = np.floor(amt_position / (self.mktprice_close() * self.multiplier()))
        trade_unit = np.floor(amt_position / self.multiplier())
        # self.synthetic_ratio = delta
        self.synthetic_unit = trade_unit
        return trade_unit

    # Get hedge position in trade unit
    def get_hedge_position(self, delta):
        # hedge_scale : total notional amt to hedge in RMB
        return - self.get_synthetic_unit(delta)

    def get_synthetic_option_rebalancing_unit(self, delta: float, delta_bound: DeltaBound = DeltaBound.NONE) -> int:
        # d_delta = delta - self.synthetic_ratio
        # # Apply delta bound filter
        # if delta_bound == DeltaBound.WHALLEY_WILLMOTT:
        #     if abs(d_delta) > delta_bound:
        #         trade_unit = np.floor(d_delta * self.amt_option / self.multiplier())
        #     else:
        #         return 0
        # else:
        #     trade_unit = np.floor(d_delta * self.amt_option / self.multiplier())
        # self.synthetic_ratio = delta
        hold_unit = self.synthetic_unit
        synthetic_unit = self.get_synthetic_unit(delta)
        d_unit = synthetic_unit - hold_unit
        self.synthetic_unit = synthetic_unit
        return d_unit

    def portfolio_exposure(self, hedge_holding_unit):
        # TODO
        return

    def replicated_option_value(self, option: EuropeanOption, vol):
        spot = self.mktprice_close()
        black = BlackCalculator(self.eval_date, option.dt_maturity, option.strike,
                                option.option_type, spot, vol, self.rf)
        npv = black.NPV()
        return npv


