from typing import Union

import pandas as pd

from back_test.model.base_product import BaseProduct
from back_test.model.constant import FrequentType, Util, ExecuteType
from back_test.model.trade import Order


class BaseFutureCoutinuous(BaseProduct):
    """
    BaseFuture: For Independent Future or Future Continuous.
    """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__(df_data, df_daily_data, rf, frequency)
        self._multiplier = Util.DICT_CONTRACT_MULTIPLIER[self.name_code()]
        self.fee_rate = Util.DICT_TRANSACTION_FEE_RATE[self.name_code()]
        self.fee_per_unit = Util.DICT_TRANSACTION_FEE[self.name_code()]
        # self.fee_per_unit = 0
        # self.fee_rate = 0
        self._margin_rate = Util.DICT_FUTURE_MARGIN_RATE[self.name_code()]

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    """ getters """

    def margin_rate(self) -> Union[float, None]:
        return self._margin_rate

    def get_initial_margin(self) -> Union[float, None]:
        pre_settle_price = self.mktprice_last_settlement()
        margin = pre_settle_price * self._margin_rate * self._multiplier
        return margin

    # TODO: USE SETTLEMENT PRICE
    def get_maintain_margin(self) -> Union[float, None]:
        margin = self.mktprice_close() * self._margin_rate * self._multiplier
        return margin

    """ 期货合约既定name_code的multiplier为固定值,不需要到current state里找 """

    def multiplier(self) -> Union[int, None]:
        return self._multiplier

    """ 与base_product里不同，主力连续价格系列中id_instrument会变 """

    def id_instrument(self) -> Union[str, None]:
        return self.current_state[Util.ID_INSTRUMENT]

    """ 用于计算杠杆率 ：保证金交易，current value为零 """

    def get_current_value(self, long_short):
        return 0.0

    """ Intraday Weighted Average Price """

    def volume_weigted_average_price(self) -> Union[float, None]:
        if self.frequency in Util.LOW_FREQUENT:
            return self.mktprice_close()
        else:
            df_today = self.df_data[self.df_data[Util.DT_DATE] == self.eval_date]
            df_today.loc[:, 'volume_price'] = df_today[Util.AMT_TRADING_VOLUME] * df_today[Util.AMT_CLOSE]
            vwap = df_today['volume_price'].sum() / df_today[Util.AMT_TRADING_VOLUME].sum()
            return vwap

    # TODO: 主力连续的仓换月周/日；移仓换月成本

    def execute_order(self, order: Order, slippage=1, execute_type: ExecuteType = ExecuteType.EXECUTE_ALL_UNITS):
        if order is None: return
        if execute_type == ExecuteType.EXECUTE_ALL_UNITS:
            order.trade_all_unit(slippage)
        elif execute_type == ExecuteType.EXECUTE_WITH_MAX_VOLUME:
            order.trade_with_current_volume(int(self.trading_volume()), slippage)
        else:
            return
        execution_record: pd.Series = order.execution_res
        # calculate margin requirement
        margin_requirement = self.get_initial_margin() * execution_record[Util.TRADE_UNIT]
        if self.fee_per_unit is None:
            # 百分比手续费
            transaction_fee = execution_record[Util.TRADE_PRICE] * self.fee_rate * execution_record[
                Util.TRADE_UNIT] * self._multiplier
        else:
            # 每手手续费
            transaction_fee = self.fee_per_unit * execution_record[Util.TRADE_UNIT]
        execution_record[Util.TRANSACTION_COST] += transaction_fee
        transaction_fee_add_to_price = transaction_fee / (execution_record[Util.TRADE_UNIT] * self._multiplier)
        execution_record[Util.TRADE_PRICE] += execution_record[
                                                  Util.TRADE_LONG_SHORT].value * transaction_fee_add_to_price
        position_size = order.long_short.value * execution_record[Util.TRADE_PRICE] * execution_record[
            Util.TRADE_UNIT] * self._multiplier
        execution_record[
            Util.TRADE_BOOK_VALUE] = position_size  # 头寸规模（含多空符号），例如，空一手豆粕（3000点，乘数10）得到头寸规模为-30000，而建仓时点头寸市值为0。
        execution_record[Util.TRADE_MARGIN_CAPITAL] = margin_requirement
        execution_record[
            Util.TRADE_MARKET_VALUE] = 0.0  # Init value of a future trade is ZERO, except for transaction cost.
        return execution_record
