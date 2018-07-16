import pandas as pd
from typing import Union
import datetime
from back_test.model.constant import FrequentType, Util
from back_test.model.base_product import BaseProduct


class BaseFuture(BaseProduct):
    """
    BaseFuture: For Independent Future or Future Continuous.
    """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__(df_data, df_daily_data, rf, frequency)
        self.multiplier = Util.DICT_FUTURE_CONTRACT_MULTIPLIER[self.name_code()]

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    """ getters """

    def contract_month(self) -> Union[str, None]:
        return self.current_state[Util.NAME_CONTRACT_MONTH]

    def get_margin(self) -> Union[float,None]:
        margin_rate = Util.DICT_FUTURE_MARGIN_RATE[self.name_code()]
        pre_settle_price = self.mktprice_last_settlement()
        margin = pre_settle_price * margin_rate * self.multiplier
        return margin

    def maturitydt(self) -> Union[datetime.date,None]:
        return self.current_state[Util.DT_MATURITY]

    def multiplier(self) -> Union[int,None]:
        return self.multiplier

    # 与base_product里不同，主力连续价格系列中id_instrument会变
    def id_instrument(self) -> str:
        return self.current_state[Util.ID_INSTRUMENT]

    def is_core(self) -> Union[bool,None]:
        core_months = Util.DICT_FUTURE_CORE_CONTRACT[self.name_code()]
        if core_months == Util.STR_ALL:
            return True
        else:
            month = int(self.contract_month()[-2:])
            if month in core_months:
                return True
            else:
                return False

    #TODO: 主力连续的仓换月周/日；移仓换月成本

