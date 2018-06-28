import datetime
from pandas import DataFrame, Series
from .abstract_base_product import AbstractBaseProduct
from .constant import FrequentType
from .BktUtil import BktUtil


class BaseInstrument(AbstractBaseProduct):
    """
    BaseInstrument: base class for financial product like instrument.
    """

    def __init__(self, df_metrics: DataFrame, frequency: FrequentType = FrequentType.DAILY):
        self.util = BktUtil()
        self.frequency: FrequentType = frequency
        self.df_metrics: DataFrame = df_metrics
        self.nbr_index: int = df_metrics.shape[0]
        self.current_index: int = 0
        self.current_state: Series = self.df_metrics.loc[self.current_index]
        # TODO why this property?
        self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        self.eval_date: datetime.date = None
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

    def next(self) -> None:
        self.update_current_state()

    def update_current_state(self) -> None:
        if self.frequency in self.util.frequent_type_low:
            self.current_index += 1
            self.current_state = self.df_metrics.loc[self.current_index]
            self.eval_date = self.current_state[self.util.col_date]
        else:
            cur_date = self.current_state[self.util.col_date]
            cur_datetime = self.current_state[self.util.col_datetime]
            # Remove data with date before 09:30 or after 15:00
            if cur_datetime < datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 9, 30, 00) or \
                    cur_date > datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 15, 00, 00):
                self.update_current_state()
            cur_datetime = self.current_state[self.util.col_datetime]
            cur_date = cur_datetime.date()
            if cur_datetime < datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 9, 30, 00) or \
                    cur_date > datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 15, 00, 00):
                self.update_current_state()
            # while self.dt_datetime < lo_dt_datetime:
            #     self.current_index += 1
            #     self.update_current_state()
            #     self.update_current_datetime()
            # set evaluation date
            # dt_today = self.dt_datetime.date()
            # if dt_today != self.eval_date:
            #     self.eval_date = dt_today
            #     idx_today = self.dt_list.index(dt_today)
            #     self.current_daily_state = self.df_daily_metrics.loc[idx_today]

    def update_current_datetime(self):
        try:
            dt_datetime = self.current_state[self.util.col_datetime]
        except:
            dt_datetime = None
        self.dt_datetime = dt_datetime
