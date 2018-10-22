from abc import ABC, abstractmethod

class AbstractOptionPricingEngine(ABC):

    def __init__(self) -> None:
        super().__init__()


    @abstractmethod
    def NPV(self) -> float:
        pass

    @abstractmethod
    def reset_vol(self, vol):
        pass

    @abstractmethod
    def estimate_vol(self, price: float, presion: float = 0.00001, max_vol: float = 2.0):
        pass

    @abstractmethod
    def Delta(self,implied_vol:float) -> float:
        pass

    @abstractmethod
    def Gamma(self,implied_vol:float) -> float:
        pass