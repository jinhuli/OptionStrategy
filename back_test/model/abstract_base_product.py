from pandas import Series
from abc import ABC, abstractmethod

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

    """
    open/close position: 返回开平仓是否成功的信号
    """

