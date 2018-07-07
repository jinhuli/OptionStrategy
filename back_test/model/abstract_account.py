from pandas import Series
from abc import ABC, abstractmethod

"""
    AbstractBaseProduct: an abstract class of base product.
"""


class AbstractAccount(ABC):
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
    open_position: 
    """
    @abstractmethod
    def add_record(self) -> bool:
        pass