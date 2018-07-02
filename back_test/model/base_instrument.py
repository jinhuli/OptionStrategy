import datetime
import numpy as np
from pandas import DataFrame, Series
from back_test.model.abstract_base_product import AbstractBaseProduct
from back_test.model.constant import FrequentType, Util


class BaseInstrument(AbstractBaseProduct):
    """
    BaseInstrument: base class for financial product like instrument.
    """

    def __init__(self, df_data: DataFrame, frequency: FrequentType = FrequentType.DAILY):
        super().__init__()
        self.frequency: FrequentType = frequency
        self.df_data: DataFrame = df_data
        # TODO maybe use enum is better
        self.nbr_index: int = df_data.shape[0]
        self.current_index: int = -1
        self.current_state: Series = None
        # TODO why this property?
        # self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        self.eval_date: datetime.date = None
        self.pre_process()
        self.update_current_state()

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

    def generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.INSTRUMENT_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if not columns.contains(column):
                print("{} missing column {}", self.__repr__(), column)
                self.df_data[column] = None

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},current_index: {2},frequency: {3})' \
            .format(self.id_instrument(), self.eval_date, self.current_index, self.frequency)

    """
    getters
    """
    def name_code(self):
        return self.id_instrument().split('_')[0]

    def id_instrument(self):
        return self.current_state[Util.ID_INSTRUMENT]

    def code_instrument(self):
        return self.current_state[Util.CODE_INSTRUMENT]

    def mktprice_close(self):
        ret = self.current_state[Util.AMT_CLOSE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_open(self):
        ret = self.current_state[Util.AMT_OPEN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_settlement(self):
        ret = self.current_state[Util.AMT_SETTLEMENT]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_morning_open_15min(self):
        ret = self.current_state[Util.AMT_MORNING_OPEN_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_morning_close_15min(self):
        ret = self.current_state[Util.AMT_MORNING_CLOSE_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_afternoon_open_15min(self):
        ret = self.current_state[Util.AMT_AFTERNOON_OPEN_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_afternoon_close_15min(self):
        ret = self.current_state[Util.AMT_AFTERNOON_CLOSE_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    """tmp"""
    def mktprice_morning_avg(self):
        ret = self.current_state[Util.AMT_MORNING_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_afternoon_avg(self):
        ret = self.current_state[Util.AMT_AFTERNOON_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def mktprice_daily_avg(self):
        ret = self.current_state[Util.AMT_DAILY_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def holding_volume(self):
        ret = self.current_state[Util.AMT_HOLDING_VOLUME]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def trading_volume(self):
        ret = self.current_state[Util.AMT_TRADING_VOLUME]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    """ last settlement, daily"""

    def mktprice_last_settlement(self):
        ret = self.current_state[Util.AMT_LAST_SETTLEMENT]
        if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
            if self.current_index == 0:
                return None
            # TODO: add support for high frequency data later.
            if self.frequency not in Util.LOW_FREQUENT:
                return None
            return self.df_data.loc[self.current_index - 1][Util.AMT_SETTLEMENT]

        # amt = None
        # # tmp = pd.DataFrame(self.current_daily_state)
        # if self.util.col_last_settlement in self.current_daily_state.index.values:
        #     amt = self.current_daily_state.loc[self.util.col_last_settlement]
        # if amt == None or amt == np.nan:
        #     try:
        #         idx_date = self.dt_list.index(self.eval_date)
        #         if idx_date == 0:
        #             return
        #         dt_last = self.dt_list[self.dt_list.index(self.eval_date) - 1]
        #         df_last_state = self.df_daily_metrics.loc[dt_last]
        #         amt = df_last_state[self.util.col_settlement]
        #     except Exception as e:
        #         print(e)
        # return amt


    """ last bar/state, not necessarily daily"""

    def mktprice_last_close(self):
        ret = self.current_state[Util.AMT_LAST_CLOSE]
        if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
            if self.current_index == 0:
                return None
        amt = None
        if self.util.col_last_close in self.current_daily_state.index.values:
            amt = self.current_daily_state.loc[self.util.col_last_close]
        if amt == None or amt == np.nan:
            try:
                if self.current_index == 0: return
                df_last_state = self.df_metrics.loc[self.current_index - 1]
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
