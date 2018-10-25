from abc import ABC, abstractmethod
import datetime
import numpy as np
import pandas as pd
from back_test.model.abstract_base_product import AbstractBaseProduct
from back_test.model.constant import FrequentType, Util, TradeType, ExecuteType, LongShort
from back_test.model.trade import Order
from typing import Union


class BaseProduct(AbstractBaseProduct):
    """
    BaseProduct: base class for financial product.
    """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__()
        self.frequency: FrequentType = frequency
        self.df_data: pd.DataFrame = df_data
        self.df_daily_data: pd.DataFrame = df_daily_data  # Used in high frequency data
        self.nbr_index: int = df_data.shape[0]
        self._id_instrument: str = self.df_data.loc[0][Util.ID_INSTRUMENT]
        self._name_code: str = self._id_instrument.split('_')[0].lower()
        self.current_index: int = -1
        self.current_daily_index: int = -1
        self.eval_date: datetime.date = None
        self.eval_datetime: datetime.datetime = None
        self.current_state: pd.Series = None
        self.current_daily_state: pd.Series = None
        self.rf = rf

    def init(self) -> None:
        self.validate_data()
        self.pre_process()
        self.next()

    def next(self) -> None:
        if not self.has_next():
            return None
        self.update_current_state()
        self.update_current_daily_state()

    def last_date(self) -> datetime.date:
        return self.df_data.loc[self.nbr_index - 1, Util.DT_DATE]

    def go_to(self, dt: datetime.date) -> None:
        """
        Set current date of base product.
        :param dt: required datetime.
        :return:
        """
        if self.df_data is not None:
            df_query_index = self.df_data.dt_date == dt
            if df_query_index.sum() == 0:
                raise ValueError("Input date {} does not exist in df_data on base product object {}".format(dt, self))
            self.current_index = df_query_index.idxmax()
            self.current_state = self.df_data.loc[self.current_index]
            self.eval_date = self.current_state[Util.DT_DATE]
        if self.df_daily_data is not None:
            df_daily_query_index = self.df_daily_data.dt_date == dt
            if df_daily_query_index.sum() == 0:
                raise ValueError(
                    "Input date {} does not exist in df_daily_data on base product object {}".format(dt, self))
            self.current_daily_index = df_daily_query_index.idxmax()
            self.current_daily_state = self.df_daily_data.loc[self.current_daily_index]
        if self.frequency not in Util.LOW_FREQUENT:
            self.eval_datetime = self.current_state[Util.DT_DATETIME]
        else:
            self.eval_datetime: datetime.datetime = datetime.datetime(self.eval_date.year,
                                                                      self.eval_date.month,
                                                                      self.eval_date.day,
                                                                      0, 0, 0)

    def validate_data(self) -> None:
        # Basic validation appliable for all instruments
        if self.frequency not in Util.LOW_FREQUENT:
            # High Frequency Data:
            # overwrite date col based on data in datetime col.
            self.df_data[Util.DT_DATE] = self.df_data[Util.DT_DATETIME].apply(lambda x: x.date())
            self.eval_date: datetime.date = self.df_data.loc[0][Util.DT_DATE]
            self.eval_datetime: datetime.datetime = self.df_data.loc[0][Util.DT_DATETIME]
            mask = self.df_data.apply(Util.filter_invalid_data, axis=1)
            self.df_data = self.df_data[mask].reset_index(drop=True)
            self.nbr_index: int = self.df_data.shape[0]
        else:
            self.eval_date: datetime.date = self.df_data.loc[0][Util.DT_DATE]
            self.eval_datetime: datetime.datetime = datetime.datetime(self.eval_date.year,
                                                                      self.eval_date.month,
                                                                      self.eval_date.day,
                                                                      0, 0, 0)
        # Product specific validation to be override
        self._generate_required_columns_if_missing()
        # Product specific pre_process to be override
        self.pre_process()

    def pre_process(self) -> None:
        return

    def _generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.PRODUCT_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if column not in columns:
                self.df_data[column] = None
        if self.df_daily_data is None or self.df_daily_data.empty:
            return
            # for column in required_column_list:
            #     self.df_daily_data[column] = None
        else:
            columns2 = self.df_daily_data.columns
            for column in required_column_list:
                if column not in columns2:
                    self.df_daily_data[column] = None

    # TODO: ADD NEXT DAY METHOD
    def get_next_state_date(self):
        if self.has_next():
            next_date = self.df_data.loc[self.current_index + 1, Util.DT_DATE]
            return next_date

    def is_last_minute(self) -> bool:
        if self.has_next():
            next_date = self.df_data.loc[self.current_index + 1, Util.DT_DATE]
            if self.eval_date == next_date:
                return False
            else:
                return True
        else:
            return True

    def has_next_minute(self) -> bool:
        if self.frequency in Util.LOW_FREQUENT or self.current_index == self.nbr_index:
            return False
        else:
            dt_next_bar = self.df_data.loc[self.current_index + 1, Util.DT_DATE]
            if self.eval_date == dt_next_bar:
                return True
            else:
                return False

    def has_next(self) -> bool:
        return self.current_index < self.nbr_index - 1

    def is_last(self) -> bool:
        return self.current_index == self.nbr_index -1

    def update_current_state(self) -> None:
        self.current_index += 1
        self.current_state = self.df_data.loc[self.current_index]
        self.eval_date = self.current_state[Util.DT_DATE]
        if self.frequency not in Util.LOW_FREQUENT:
            self.eval_datetime = self.current_state[Util.DT_DATETIME]
        else:
            self.eval_datetime: datetime.datetime = datetime.datetime(self.eval_date.year,
                                                                      self.eval_date.month,
                                                                      self.eval_date.day,
                                                                      0, 0, 0)

    # TODO: Is daily state necessary in high freq data?
    def update_current_daily_state(self) -> None:
        if self.df_daily_data is None:
            return
        if self.current_daily_state is not None and self.current_daily_state[Util.DT_DATE] == self.current_state[
            Util.DT_DATE]:
            return
        self.current_daily_index += 1
        self.current_daily_state = self.df_daily_data.loc[self.current_daily_index]

    def get_current_state(self) -> pd.Series:
        return self.current_state

    def get_current_dayly_state(self) -> pd.Series:
        return self.current_daily_state

    def __repr__(self) -> str:
        return 'BaseProduct(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    """
    open/close position
    """

    def open_long(self, order: Order) -> bool:  # 多开
        order.trade_type = TradeType.OPEN_LONG
        return self.execute_order(order)

    def open_short(self, order: Order) -> bool:  # 空开
        order.trade_type = TradeType.OPEN_SHORT
        return self.execute_order(order)

    def close_out(self, dt_trade, id_instrument, trade_price, time_signal=None):
        # Find current aggregated open interest on id_instrument

        return True

    """ Close not all open interests on 'id_instrument' """

    def close_partial(self, dt_trade, id_instrument, trade_price, trade_unit, time_signal=None):
        # TODO
        return True

    # def execute_order(self,order: Order, slippage=0, execute_type: ExecuteType = ExecuteType.EXECUTE_ALL_UNITS):
    #     raise NotImplementedError("Child class not implement method execute_order.")

    """
    getters
    """

    # """ 保证金交易当前价值为零/基础证券交易不包含保证金current value为当前价格 """
    # def get_current_value(self, long_short):
    #     raise NotImplementedError("Child class not implement method execute_order.")
    #
    # """ 标记是否为保证金交易 """
    # def is_margin_trade(self, long_short):
    #     raise NotImplementedError("Child class not implement method execute_order.")
    #
    # """ 标记该证券是否逐日盯市（期货是的，期权不是） """
    # def is_mtm(self):
    #     raise NotImplementedError("Child class not implement method execute_order.")

    @abstractmethod
    def execute_order(self, order: Order, slippage: int = 0,
                      execute_type: ExecuteType = ExecuteType.EXECUTE_ALL_UNITS) -> bool:
        # 执行交易指令
        pass

    @abstractmethod
    def get_current_value(self, long_short: LongShort) -> float:
        # 保证金交易当前价值为零/基础证券交易不包含保证金current value为当前价格
        pass

    @abstractmethod
    def is_margin_trade(self, long_short: LongShort) -> bool:
        # 标记是否为保证金交易
        pass

    @abstractmethod
    def is_mtm(self) -> bool:
        # 标记该证券是否逐日盯市
        pass

    def multiplier(self) -> int:
        return 1

    def name_code(self) -> str:
        return self._name_code

    def id_instrument(self) -> str:
        return self._id_instrument

    def code_instrument(self) -> str:
        return self.current_state[Util.CODE_INSTRUMENT]

    def mktprice_close(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_CLOSE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_open(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_OPEN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_settlement(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_SETTLEMENT]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_volume_weighted(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_TRADING_VALUE]/self.current_state[Util.AMT_TRADING_VOLUME]/self.multiplier()
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_morning_open_15min(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_MORNING_OPEN_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_morning_close_15min(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_MORNING_CLOSE_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_afternoon_open_15min(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_AFTERNOON_OPEN_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_afternoon_close_15min(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_AFTERNOON_CLOSE_15MIN]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_morning_avg(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_MORNING_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_afternoon_avg(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_AFTERNOON_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def mktprice_daily_avg(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_DAILY_AVG]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def holding_volume(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_HOLDING_VOLUME]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def trading_volume(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_TRADING_VOLUME]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def trading_value(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_TRADING_VALUE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret


    """ last settlement, daily"""

    def mktprice_last_settlement(self) -> float:
        if self.frequency in Util.LOW_FREQUENT:
            ret = self.current_state[Util.AMT_LAST_SETTLEMENT]
            if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
                return self.mktprice_close() if self.current_index == 0 \
                    else self.df_data.loc[self.current_index - 1][Util.AMT_SETTLEMENT]
        else:
            ret = self.current_daily_state[Util.AMT_LAST_SETTLEMENT]
            # if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
            #     return self.mktprice_close() if self.current_daily_index == 0 \
            #         else self.df_daily_data.loc[self.current_daily_index - 1][Util.AMT_SETTLEMENT]
            if (ret is None or np.isnan(ret) or ret == Util.NAN_VALUE) and self.current_daily_index != 0:
                ret = self.df_daily_data.loc[self.current_daily_index - 1][Util.AMT_SETTLEMENT]
            if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
                return self.mktprice_close()
        return ret

    """ last bar/state, not necessarily daily"""

    def mktprice_last_close(self) -> float:
        ret = self.current_state[Util.AMT_LAST_CLOSE]
        if ret is None or np.isnan(ret) or ret == Util.NAN_VALUE:
            if self.current_index == 0:
                return self.current_state[Util.AMT_OPEN]
            return self.df_data.loc[self.current_index - 1][Util.AMT_CLOSE]
        return ret

    def get_initial_margin(self, long_short: LongShort) -> float:
        return 0.0

    def get_maintain_margin(self, long_short: LongShort) -> float:
        return 0.0


    def calulate_volume_weighted_price_start(self):
        self.total_trading_value = 0
        self.total_trading_volume = 0

    def calulate_volume_weighted_price(self):
        if self.trading_volume() is not None and self.trading_value() is not None:
            self.total_trading_value += self.trading_value()
            self.total_trading_volume += self.trading_volume()

    def calulate_volume_weighted_price_stop(self):
        return self.total_trading_value/self.total_trading_volume
