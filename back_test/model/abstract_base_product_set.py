from abc import ABC, abstractmethod
from pandas import Series

"""
    AbstractBaseProductSet: an abstract class of base product set.
"""


class AbstractBaseProductSet(ABC):
    """
    initialize the AbstractBaseProduct class
    """

    def __init__(self) -> None:
        super().__init__()

    """
    pre_process: pre process data to populate option set
    """
    @abstractmethod
    def pre_process(self) -> None:
        pass

    """
    next: move forward to next tick for each element in product set
    """
    @abstractmethod
    def next(self) -> None:
        pass
