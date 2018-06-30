import datetime
from pandas import DataFrame, Series
from .abstract_base_product import AbstractBaseProduct
from .constant import FrequentType
from .constant import Util


class BaseInstrument(AbstractBaseProduct):
    """
    BaseInstrument: base class for financial product like instrument.
    """

    def __init__(self, df_data: DataFrame, id_instrument: str, frequency: FrequentType = FrequentType.DAILY):
        super().__init__()
        self.frequency: FrequentType = frequency
        self.df_data: DataFrame = df_data
        self.id_instrument = id_instrument
        # TODO maybe use enum is better
        self.name_code = id_instrument.split('_')[0]
        self.nbr_index: int = df_data.shape[0]
        self.current_index: int = -1
        self.current_state: Series = None
        # TODO why this property?
        # self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        self.eval_date: datetime.date = None
        self.pre_process()
        self.update_current_state()
        # self.pricing_metrics = None
        # self.trade_long_short = None
        # self.trade_flag_open = False
        # self.trade_unit = None
        # self.trade_dt_open = None
        # self.trade_long_short = None
        # self.premium = None
        # self.trade_open_price = None
        # self.trade_margin_capital = None
        # self.transaction_fee = None
        # self.open_price = None

    def pre_process(self) -> None:
        # filter function to filter out ivalid data from dataframe
        def filter_invalid_data(x):
            cur_date = x[Util.DT_DATE]
            if x[Util.DT_DATETIME] >= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 9, 30, 00) and \
                    x[
                        Util.DT_DATETIME] <= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 11, 30,
                                                               00):
                return True
            if x[Util.DT_DATETIME] >= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 13, 00, 00) and \
                    x[
                        Util.DT_DATETIME] <= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 15, 00,
                                                               00):
                return True
            return False

        if self.frequency not in Util.LOW_FREQUENT:
            # overwrite date col based on data in datetime col.
            self.df_data[Util.DT_DATE] = self.df_data[Util.DT_DATETIME].apply(lambda x: x.date())
            mask = self.df_data.apply(filter_invalid_data, axis=1)
            self.df_data = self.df_data[mask].reset_index(drop=True)

    def next(self) -> None:
        self.update_current_state()

    def update_current_state(self) -> None:
        self.current_index += 1
        self.current_state = self.df_data.loc[self.current_index]
        self.eval_date = self.current_state[Util.DT_DATE]

    def get_current_state(self) -> Series:
        return self.current_state

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},current_index: {2},frequency: {3})' \
            .format(self.id_instrument, self.eval_date, self.current_index, self.frequency)
