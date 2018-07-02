import datetime
import numpy as np
from pandas import DataFrame, Series
from back_test.model.abstract_base_product import AbstractBaseProduct
from back_test.model.constant import FrequentType, Util


class BaseProduct(AbstractBaseProduct):
    """
    BaseProduct: base class for financial product.
    """

    def __init__(self, df_data: DataFrame, df_daily_data: DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__()
        self.frequency: FrequentType = frequency
        self.df_data: DataFrame = df_data
        # Used in high frequency data
        self.df_daily_data: DataFrame = df_daily_data
        # TODO maybe use enum is better
        self.nbr_index: int = df_data.shape[0]
        self.current_index: int = -1
        self.current_daily_index: int = -1
        self.current_state: Series = None
        self.current_daily_state: Series = None
        self.rf = rf
        # TODO why this property?
        # self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        self.eval_date: datetime.date = None
        self.pre_process()
        self.next()

    def pre_process(self) -> None:
        # TODO: move to a file as static method
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
            # TODO: preprocess df_daily
        self.generate_required_columns_if_missing()

    def next(self) -> None:
        self.update_current_state()
        self.update_current_daily_state()

    def update_current_state(self) -> None:
        self.current_index += 1
        self.current_state = self.df_data.loc[self.current_index]
        self.eval_date = self.current_state[Util.DT_DATE]

    def update_current_daily_state(self) -> None:
        if self.df_daily_data is None:
            return
        if self.current_daily_state[Util.DT_DATE] == self.current_state[Util.DT_DATE]:
            return
        self.current_daily_index += 1
        self.current_daily_state = self.df_daily_data.loc[self.current_daily_index]

    def get_current_state(self) -> Series:
        return self.current_state

    def get_current_dayli_state(self) -> Series:
        return self.current_daily_state

    def generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.PRODUCT_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if not columns.contains(column):
                print("{} missing column {}", self.__repr__(), column)
                self.df_data[column] = None

    def __repr__(self) -> str:
        return 'BaseProduct(id_instrument: {0},eval_date: {1},current_index: {2},frequency: {3})' \
            .format(self.id_instrument(), self.eval_date, self.current_index, self.frequency)

    """
    getters
    """

    def name_code(self) -> str:
        return self.id_instrument().split('_')[0]

    def id_instrument(self) -> str:
        return self.current_state[Util.ID_INSTRUMENT]

    def code_instrument(self) -> str:
        return self.current_state[Util.CODE_INSTRUMENT]

    def mktprice_close(self) -> float:
        ret = self.current_state[Util.AMT_CLOSE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_open(self) -> float:
        ret = self.current_state[Util.AMT_OPEN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_settlement(self) -> float:
        ret = self.current_state[Util.AMT_SETTLEMENT]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_morning_open_15min(self) -> float:
        ret = self.current_state[Util.AMT_MORNING_OPEN_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_morning_close_15min(self) -> float:
        ret = self.current_state[Util.AMT_MORNING_CLOSE_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_afternoon_open_15min(self) -> float:
        ret = self.current_state[Util.AMT_AFTERNOON_OPEN_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_afternoon_close_15min(self) -> float:
        ret = self.current_state[Util.AMT_AFTERNOON_CLOSE_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_morning_avg(self) -> float:
        ret = self.current_state[Util.AMT_MORNING_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_afternoon_avg(self) -> float:
        ret = self.current_state[Util.AMT_AFTERNOON_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_daily_avg(self) -> float:
        ret = self.current_state[Util.AMT_DAILY_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def holding_volume(self) -> float:
        ret = self.current_state[Util.AMT_HOLDING_VOLUME]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def trading_volume(self) -> float:
        ret = self.current_state[Util.AMT_TRADING_VOLUME]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    """ last settlement, daily"""

    def mktprice_last_settlement(self) -> float:
        if self.frequency in Util.LOW_FREQUENT:
            ret = self.current_state[Util.AMT_LAST_SETTLEMENT]
            if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
                return None if self.current_index == 0 \
                    else self.df_data.loc[self.current_index - 1][Util.AMT_SETTLEMENT]
        else:
            ret = self.current_daily_state[Util.AMT_LAST_SETTLEMENT]
            if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
                return None if self.current_daily_index == 0 \
                    else self.df_daily_data.loc[self.current_daily_index - 1][Util.AMT_SETTLEMENT]
        return ret

    """ last bar/state, not necessarily daily"""

    def mktprice_last_close(self) -> float:
        ret = self.current_state[Util.AMT_LAST_CLOSE]
        if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
            if self.current_index == 0:
                return self.current_state[Util.AMT_OPEN]
            return self.df_data.loc[self.current_index - 1][Util.AMT_CLOSE]
        return ret
