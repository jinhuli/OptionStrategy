from PricingLibrary.BlackCalculator import BlackCalculator
from PricingLibrary.Options import EuropeanOption
from back_test.model.constant import Util, FrequentType, OptionType, DeltaBound
from back_test.model.base_future_coutinuous import BaseFutureCoutinuous
import numpy as np

"""
基于期货构造合成期权，行权价可根据现货指数点位设定从而包含基差的影响。
"""


class SytheticOption(BaseFutureCoutinuous):

    def __init__(self, df_c1_minute,
                 df_c1_daily,
                 df_futures_daily=None,
                 df_index_daily=None,
                 rf=0.03,
                 frequency=FrequentType.MINUTE,
                 hedge_scale=Util.BILLION):
        super().__init__(df_data=df_c1_minute, df_daily_data=df_c1_daily, rf=rf, frequency=frequency)
        self.df_futures_daily = df_futures_daily
        self.df_underying_index = df_index_daily
        self.hedge_ratio = 0.0
        self.hedge_scale = hedge_scale

    def get_c1_with_start_dates(self):
        df = self.df_daily_data.drop_duplicates(Util.ID_INSTRUMENT)[[Util.DT_DATE, Util.ID_INSTRUMENT]]
        return df

    def get_c1_with_end_dates(self):
        df = self.df_daily_data.drop_duplicates(Util.ID_INSTRUMENT, 'last')[[Util.DT_DATE, Util.ID_INSTRUMENT]]
        return df

    def get_black_delta(self, option: EuropeanOption, vol: float):
        spot = self.mktprice_close()
        black = BlackCalculator(self.eval_date, option.dt_maturity, option.strike,
                                option.option_type, spot, vol, self.rf)
        delta = black.Delta()
        return delta

    # Get hedge position in trade unit
    def create_hedge_position(self, delta):
        # hedge_scale : total notional amt to hedge in RMB
        amt_position = - delta * self.hedge_scale
        trade_unit = np.floor(amt_position / self.multiplier())
        self.hedge_ratio = delta
        return trade_unit

    def rebalancing(self, delta: float, delta_bound: DeltaBound = DeltaBound.NONE) -> float:
        d_delta = delta - self.hedge_ratio
        # Apply delta bound filter
        if delta_bound == DeltaBound.WHALLEY_WILLMOTT:
            if abs(d_delta) > delta_bound:
                trade_unit = np.floor(d_delta * self.hedge_scale / self.multiplier())
            else:
                return 0.0
        else:
            trade_unit = np.floor(d_delta * self.hedge_scale / self.multiplier())
        self.hedge_ratio = delta
        return trade_unit

    def portfolio_exposure(self, hedge_holding_unit):
        # TODO
        return

    def replicated_option_value(self,option:EuropeanOption, vol):
        spot = self.mktprice_close()
        black = BlackCalculator(self.eval_date, option.dt_maturity, option.strike,
                                option.option_type, spot, vol, self.rf)
        npv = black.NPV()
        return npv

