from OptionStrategyLib.OptionPricing.Options import OptionPlainEuropean

from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from back_test.bkt_util import BktUtil

import datetime
import QuantLib as ql
import numpy as np
import pandas as pd


class BktInstrument(object):
    """
    Contain metrics and trading position info as attributes

    """

    def __init__(self, cd_frequency, df_daily_metrics, df_intraday_metrics=None,rf = 0.03):
        self.util = BktUtil()
        self.rf = rf
        self.frequency = cd_frequency
        self.df_daily_metrics = df_daily_metrics  # Sorted ascending by date/datetime
        if self.frequency in self.util.cd_frequency_low:
            self.df_metrics = df_daily_metrics
        else:
            self.df_metrics = df_intraday_metrics
        self.start_index = 0
        self.nbr_index = len(df_daily_metrics)
        self.last_index = len(df_daily_metrics) - 1
        self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        # Start
        self.current_index = self.start_index
        self.last_state = pd.Series()
        self.pricing_metrics = None
        self.trade_long_short = None
        self.trade_flag_open = False
        self.trade_unit = None
        self.trade_dt_open = None
        self.trade_long_short = None
        self.premium = None
        self.trade_open_price = None
        self.trade_margin_capital = None
        self.transaction_fee = None
        self.open_price = None
        self.update_current_state()
        self.set_instrument_basics()
        self.update_instrument_price()

    def next(self):
        self.last_state = self.current_state
        self.current_index = self.current_index + 1
        self.update_current_state()
        self.update_instrument_price()

    def update_current_state(self):
        self.current_state = self.df_metrics.loc[self.current_index]

        if self.frequency in self.util.cd_frequency_low:
            self.current_daily_state = self.current_state
            self.dt_date = self.current_state[self.util.col_date]
        else:
            self.update_current_datetime()
            # Remove datetime data before 09:30
            dt = datetime.datetime(self.dt_datetime.year, self.dt_datetime.month, self.dt_datetime.day, 9, 30, 00)
            while self.dt_datetime < dt:
                self.current_index += 1
                self.update_current_state()
                self.update_current_datetime()
            # set evaluation date
            dt_today = self.dt_datetime.date()
            if dt_today != self.dt_date:
                self.dt_date = dt_today
                idx_today = self.dt_list.index(dt_today)
                self.current_daily_state = self.df_daily_metrics.loc[idx_today]


    def update_current_datetime(self):
        try:
            dt_datetime = self.current_state[self.util.col_datetime]
        except Exception:
            dt_datetime = None
        self.dt_datetime = dt_datetime

    def set_instrument_basics(self):
        try:
            id_instrument = self.current_daily_state[self.util.id_instrument]
        except Exception as e:
            print(e)
            id_instrument = None
        self.id_instrument = id_instrument
        try:
            code_instrument = self.current_daily_state[self.util.col_code_instrument]
        except Exception as e:
            print(e)
            code_instrument = None
        self.code_instrument = code_instrument


    def update_instrument_price(self):
        try:
            # settle = self.current_state[self.util.col_settlement]
            close = self.current_state[self.util.col_close]
            amt_open = self.current_state[self.util.col_open]
        except Exception as e:
            print(e)
            settle = None
            close = None
            amt_open = None

        if close == -999.0 or close == None:
            print(self.id_instrument, ' : amt_close is null!')
            return
        # elif settle == -999.0:
        #     print(self.id_instrument, ' : amt_settlement is null!')
        # self.mktprice_settle = settle
        self.mktprice_close = close
        self.mktprice_open = amt_open

    def get_holding_volume(self):
        try:
            holding_volume = self.current_daily_state[self.util.col_holding_volume]
        except Exception as e:
            print(e)
            holding_volume = None
        return holding_volume

    def get_trading_volume(self):
        try:
            trading_volume = self.current_daily_state[self.util.col_trading_volume]
        except Exception as e:
            print(e)
            trading_volume = None
        return trading_volume

    def get_last_settlement(self):
        try:
            amt_pre_settle = self.current_daily_state[self.util.col_last_settlement]
        except Exception as e:
            print(e)
            amt_pre_settle = None
        if amt_pre_settle == None:
            idx_date = self.dt_list.index(self.dt_date)
            if idx_date == 0: return amt_pre_settle
            dt_last = self.dt_list[self.dt_list.index(self.dt_date) - 1]
            df_last_state = self.df_daily_metrics.loc[dt_last]
            amt_pre_settle = df_last_state[self.util.col_last_settlement]
        return amt_pre_settle

    def get_last_close(self):
        try:
            amt_pre_close = self.current_daily_state[self.util.col_last_close]
        except Exception as e:
            print(e)
            amt_pre_close = None
        if amt_pre_close == None:
            idx_date = self.dt_list.index(self.dt_date)
            if idx_date == 0: return amt_pre_close
            dt_last = self.dt_list[self.dt_list.index(self.dt_date) - 1]
            df_last_state = self.df_daily_metrics.loc[dt_last]
            amt_pre_close = df_last_state[self.util.col_last_close]
        return amt_pre_close

    def get_init_margin(self):
        return

    def get_maintain_margin(self):
        return

    def price_limit(self):
        # 认购期权最大涨幅＝max｛合约标的前收盘价×0.5 %，min[（2×合约标的前收盘价－行权价格），合约标的前收盘价]×10％｝
        # 认购期权最大跌幅＝合约标的前收盘价×10％
        # 认沽期权最大涨幅＝max｛行权价格×0.5 %，min[（2×行权价格－合约标的前收盘价），合约标的前收盘价]×10％｝
        # 认沽期权最大跌幅＝合约标的前收盘价×10％
        return None
