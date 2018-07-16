import datetime
import uuid
import pandas as pd
from enum import Enum
from back_test.model.constant import TradeType


class OrderStatus(Enum):
    INITIAL = 0
    PROCESSING = 1
    COMPLETE = 2


class OrderUtil:
    TICK_SIZE_DICT = {
        "50etf": 0.0001,
        "m": 1,
        "sr": 0.5,
    }


class Order(object):

    def __init__(self, dt_trade: datetime.date, id_instrument: str,
                 trade_type: TradeType, trade_unit: int, trade_price: float,
                 time_signal: datetime.datetime):
        super().__init__()
        self._dt_trade: datetime = dt_trade
        self._id_instrument = id_instrument
        self._trade_type = trade_type
        self._trade_unit = trade_unit
        self._trade_price = trade_price
        self._time_signal = time_signal
        self._status = OrderStatus.INITIAL
        self._pending_unit = 0
        self._uuid = uuid.uuid4()

    @property
    def dt_trade(self) -> datetime.date:
        return self._dt_trade

    @dt_trade.setter
    def dt_trade(self, dt_trade: datetime.date) -> None:
        self._dt_trade = dt_trade

    @property
    def id_instrument(self):
        return self._id_instrument

    @id_instrument.setter
    def id_instrument(self, id_instrument: str) -> None:
        self._id_instrument = id_instrument

    @property
    def trade_type(self) -> TradeType:
        return self._trade_type

    @trade_type.setter
    def trade_type(self, trade_type: TradeType) -> None:
        self.trade_type = trade_type

    @property
    def trade_unit(self) -> int:
        return self._trade_unit

    @trade_unit.setter
    def trade_unit(self, trade_unit: int) -> None:
        # TODO might check trade_unit? kitten
        self._trade_unit = trade_unit

    @property
    def trade_price(self) -> float:
        return self._trade_price

    @trade_price.setter
    def trade_price(self, trade_price: int) -> None:
        self._trade_price = trade_price

    @property
    def time_signal(self) -> datetime.datetime:
        return self._time_signal

    @time_signal.setter
    def time_signal(self, time_signale: datetime.datetime) -> None:
        self._time_signal = time_signale

    @property
    def status(self) -> OrderStatus:
        return self._status

    @status.setter
    def status(self, status: OrderStatus) -> None:
        self._status = status

    @property
    def pending_unit(self) -> int:
        return self._pending_unit

    @pending_unit.setter
    def pending_unit(self, unit: int) -> None:
        self._pending_unit = unit

    @property
    def uuid(self) -> uuid:
        return self._uuid

    def trade_with_current_volume(self, max_volume: float, slippage: int = 1) -> pd.Series:
        if self.trade_unit < max_volume:
            trading_volume = self.trade_unit
            self.status = OrderStatus.COMPLETE
        else:
            self.pending_unit = self.trade_unit - max_volume
            trading_volume = max_volume
            self.status = OrderStatus.PROCESSING
        name_code = self.id_instrument.split("_")[0]
        price = self.trade_price + slippage * OrderUtil.TICK_SIZE_DICT[name_code]
        return pd.Series(
            {
                'uuid': uuid.uuid4(),
                'id_instrumet': self.id_instrument,
                'unit': trading_volume,
                'price': price,
                'type': self.trade_type,
                'date': self.dt_trade,
                'signal': self.time_signal,
            }
        )
