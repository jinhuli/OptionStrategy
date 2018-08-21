from pandas import Series
from abc import ABC, abstractmethod
from back_test.model.trade import Order
from back_test.model.constant import LongShort,ExecuteType
"""
    AbstractBaseProduct: an abstract class of base product.
"""


class AbstractBaseProduct(ABC):
    """
    initialize the AbstractBaseProduct class
    """

    def __init__(self) -> None:
        super().__init__()

    """
    init: initialize product after object is constructed.
    """
    @abstractmethod
    def init(self) -> None:
        pass

    """
    pre_process: pre process data to filter out invalid data or doing other required preprocess job.
    """
    @abstractmethod
    def pre_process(self) -> None:
        pass

    """
    next: move forward to next tick of data
    """
    @abstractmethod
    def next(self) -> None:
        pass

    """
    has_next: return whether has next iter
    """
    @abstractmethod
    def has_next(self) -> bool:
        pass

    """
    update_current_state: update df_metrics with current_index.
    """

    @abstractmethod
    def update_current_state(self) -> None:
        pass

    """
    get_current_state: method to get current tick data based on index from df_metric as DataFrame(Series).
    """

    @abstractmethod
    def get_current_state(self) -> Series:
        pass



    @abstractmethod
    def validate_data(self):
        pass


    @abstractmethod
    def execute_order(self, order: Order, slippage:int=0,execute_type:ExecuteType=ExecuteType.EXECUTE_ALL_UNITS) -> bool:
        # 执行交易指令
        pass


    @abstractmethod
    def get_current_value(self, long_short:LongShort) -> float:
        # 保证金交易当前价值为零/基础证券交易不包含保证金current value为当前价格
        pass

    @abstractmethod
    def is_margin_trade(self, long_short:LongShort) -> bool:
        # 标记是否为保证金交易
        pass

    @abstractmethod
    def is_mtm(self) -> bool:
        # 标记该证券是否逐日盯市
        pass
