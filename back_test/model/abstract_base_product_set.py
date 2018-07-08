from abc import ABC, abstractmethod

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
    init: initialize product set after object is constructed.
    """
    @abstractmethod
    def init(self) -> None:
        pass


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

    """
    has_next: return whether has next iter
    """
    @abstractmethod
    def has_next(self) -> bool:
        pass