import pandas as pd
from back_test.model.constant import FrequentType, Util
from back_test.model.base_product import BaseProduct
from back_test.model.trade import Order


class BaseInstrument(BaseProduct):
    """
    BaseInstrument: STOCK/ETF/INDEX
    """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__(df_data, df_daily_data, rf, frequency)
        self.multiplier = 1.0

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    def execute_order(self, order: Order):
        return True

    """ 用于计算杠杆率 ：基础证券交易不包含保证金current value为当前价格 """
    def get_current_value(self):
        return self.mktprice_close()
