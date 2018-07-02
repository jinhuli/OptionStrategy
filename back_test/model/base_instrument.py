import datetime
import numpy as np
from pandas import DataFrame, Series
from back_test.model.constant import FrequentType, Util
from back_test.model.base_product import BaseProduct


class BaseInstrument(BaseProduct):
    """
    BaseInstrument: base class for financial product like instrument.
    """

    def __init__(self, df_data: DataFrame, df_daily_data: DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__(df_data, df_daily_data, rf, frequency)

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},current_index: {2},frequency: {3})' \
            .format(self.id_instrument(), self.eval_date, self.current_index, self.frequency)

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
