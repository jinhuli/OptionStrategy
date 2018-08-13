import pandas as pd

from back_test.model.base_product import BaseProduct
from back_test.model.constant import FrequentType, Util, ExecuteType, LongShort
from back_test.model.trade import Order


class BaseInstrument(BaseProduct):
    """
    BaseInstrument: STOCK/ETF/INDEX
    """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.DAILY):
        super().__init__(df_data, df_daily_data, rf, frequency)
        self._multiplier = 1.0
        self.fee_rate = 0.0
        self.fee_per_unit = 0.0

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    """ Long position only in base instrument. """

    def execute_order(self, order: Order, slippage=0, execute_type: ExecuteType = ExecuteType.EXECUTE_ALL_UNITS):
        if order is None: return
        if execute_type == ExecuteType.EXECUTE_ALL_UNITS:
            order.trade_all_unit(slippage)
        elif execute_type == ExecuteType.EXECUTE_WITH_MAX_VOLUME:
            order.trade_with_current_volume(int(self.trading_volume()), slippage)
        else:
            return
        execution_record: pd.Series = order.execution_res
        # calculate margin requirement
        margin_requirement = 0.0
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
        # execution_record[
        #     Util.TRADE_MARKET_VALUE] = position_size  # Init value of a future trade is ZERO, except for transaction cost.
        return execution_record

    """ 用于计算杠杆率 ：基础证券交易不包含保证金current value为当前价格 """

    def get_current_value(self, long_short):
        if long_short == LongShort.LONG:
            return self.mktprice_close()

    def multiplier(self):
        return self._multiplier
