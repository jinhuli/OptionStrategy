import pandas as pd
from back_test.model.constant import FrequentType, Util
from back_test.model.base_product import BaseProduct


class BaseFuture(BaseProduct):
    """
    BaseInstrument: base class for financial product like instrument.
    """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__(df_data, df_daily_data, rf, frequency)
        self.multiplier = Util.DICT_FUTURE_CONTRACT_MULTIPLIER[self.name_code()]

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    def get_margin(self):
        margin_rate = Util.DICT_FUTURE_MARGIN_RATE[self.name_code()]
        pre_settle_price = self.mktprice_last_settlement()
        margin = pre_settle_price * margin_rate * self.multiplier
        return margin

