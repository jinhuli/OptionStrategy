import datetime
import pandas as pd
import numpy as np
from typing import Union
from back_test.model.constant import FrequentType, Util, PricingType, EngineType, Option50ETF, OptionFilter, OptionType
from back_test.model.base_product import BaseProduct
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator
from OptionStrategyLib.OptionPricing.BlackFormular import BlackFormula

""" Designed for underlyig class of index/ETF based options."""


class BaseIndex(BaseProduct):
    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 frequency: FrequentType = FrequentType.DAILY,
                 rf: float = 0.03):
        super().__init__(df_data, df_daily_data, rf, frequency)

    def __repr__(self) -> str:
        return 'BaseOption(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)
