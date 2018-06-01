from back_test.bkt_util import BktUtil
import datetime
import numpy as np


class BktInstrument(object):
    """ Contain metrics and trading position info as attributes """

    def __init__(self, cd_frequency, df_daily_metrics, df_intraday_metrics=None, rf = 0.03):
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
        self.current_index = self.start_index
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

    def next(self):
        self.current_index = self.current_index + 1
        self.update_current_state()

    def update_current_state(self):
        self.current_state = self.df_metrics.loc[self.current_index]
        if self.frequency in self.util.cd_frequency_low:
            self.current_daily_state = self.current_state
            self.eval_date = self.current_state[self.util.col_date]
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
            if dt_today != self.eval_date:
                self.eval_date = dt_today
                idx_today = self.dt_list.index(dt_today)
                self.current_daily_state = self.df_daily_metrics.loc[idx_today]

    def update_current_datetime(self):
        try:
            dt_datetime = self.current_state[self.util.col_datetime]
        except:
            dt_datetime = None
        self.dt_datetime = dt_datetime

    def id_instrument(self):
        try:
            id_instrument = self.current_daily_state[self.util.id_instrument]
        except:
            id_instrument = None
        return id_instrument

    def code_instrument(self):
        try:
            code_instrument = self.current_daily_state[self.util.col_code_instrument]
        except:
            code_instrument = None
        return code_instrument

    def mktprice_close(self):
        try:
            close = self.current_state[self.util.col_close]
            if close == self.util.nan_value:return
        except Exception as e:
            print(e)
            close = None
        return close

    def mktprice_open(self):
        try:
            amt_open = self.current_state[self.util.col_open]
            if amt_open == self.util.nan_value:return
        except:
            amt_open = None
        return amt_open

    def mktprice_settlement(self):
        try:
            settle = self.current_state[self.util.col_settlement]
            if settle == self.util.nan_value: settle = None
        except:
            settle = None
        return settle

    def mktprice_morning_open_15min(self):
        try:
            morning_open_15min = self.current_state[self.util.col_morning_open_15min]
            if morning_open_15min == self.util.nan_value: return
        except:
            morning_open_15min = None
        return morning_open_15min

    def mktprice_morning_close_15min(self):
        try:
            morning_close_15min = self.current_state[self.util.col_morning_close_15min]
            if morning_close_15min == self.util.nan_value: return None
        except:
            morning_close_15min = None
        return morning_close_15min

    def mktprice_afternoon_open_15min(self):
        try:
            afternoon_open_15min = self.current_state[self.util.col_afternoon_open_15min]
            if afternoon_open_15min == self.util.nan_value: return None
        except:
            afternoon_open_15min = None
        return afternoon_open_15min

    def mktprice_afternoon_close_15min(self):
        try:
            option_afternoon_close_15min = self.current_state[self.util.col_afternoon_close_15min]
            if option_afternoon_close_15min == self.util.nan_value: return None
        except:
            option_afternoon_close_15min = None
        return option_afternoon_close_15min

    def mktprice_morning_avg(self):
        try:
            morning_avg = self.current_state[self.util.col_morning_avg]
            if morning_avg == self.util.nan_value: return None
        except:
            morning_avg = None
        return morning_avg

    def mktprice_afternoon_avg(self):
        try:
            afternoon_avg = self.current_state[self.util.col_afternoon_avg]
            if afternoon_avg == self.util.nan_value: return None
        except:
            afternoon_avg = None
        return afternoon_avg

    def mktprice_daily_avg(self):
        try:
            daily_avg = self.current_state[self.util.col_daily_avg]
            if daily_avg == self.util.nan_value: return None
        except:
            daily_avg = None
        return daily_avg

    def holding_volume(self):
        try:
            holding_volume = self.current_daily_state[self.util.col_holding_volume]
        except:
            holding_volume = None
        return holding_volume

    def trading_volume(self):
        try:
            trading_volume = self.current_daily_state[self.util.col_trading_volume]
        except:
            trading_volume = None
        return trading_volume

    """ last settlement, daily"""
    def mktprice_last_settlement(self):
        amt = None
        if self.util.col_last_settlement in self.current_daily_state.columns:
            amt = self.current_daily_state[self.util.col_last_settlement]
        if amt == None or amt == np.nan:
            try:
                idx_date = self.dt_list.index(self.eval_date)
                if idx_date == 0:
                    return
                dt_last = self.dt_list[self.dt_list.index(self.eval_date) - 1]
                df_last_state = self.df_daily_metrics.loc[dt_last]
                amt = df_last_state[self.util.col_settlement]
            except Exception as e:
                print(e)
        return amt

    """ last bar/state, not necessarily daily"""
    def mktprice_last_close(self):
        amt = None
        if self.util.col_last_close in self.current_daily_state.columns:
            amt = self.current_daily_state[self.util.col_last_close]
        if amt == None or amt == np.nan:
            try:
                if self.current_index == 0: return
                df_last_state = self.df_metrics.loc[self.current_index-1]
                amt = df_last_state[self.util.col_close]
            except Exception as e:
                print(e)
        return amt

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
