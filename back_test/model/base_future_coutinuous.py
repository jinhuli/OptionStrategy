from typing import Union

import pandas as pd

from back_test.model.base_product import BaseProduct
from back_test.model.constant import FrequentType, Util, ExecuteType, LongShort
from back_test.model.trade import Order
import datetime

class BaseFutureCoutinuous(BaseProduct):
    """
    BaseFuture: For Independent Future or Future Continuous.
    """

    def __init__(self, df_future_c1: pd.DataFrame,  # future c1
                 df_future_c1_daily: pd.DataFrame = None,  # future daily c1
                 df_future_c2: pd.DataFrame = None,  # future c2
                 df_futures_all_daily: pd.DataFrame = None,
                 df_underlying_index_daily: pd.DataFrame = None,
                 rf: float = 0.03, frequency: FrequentType = FrequentType.MINUTE):
        super().__init__(df_future_c1, df_future_c1_daily, rf, frequency)
        self._multiplier = Util.DICT_CONTRACT_MULTIPLIER[self.name_code()]
        self.fee_rate = Util.DICT_TRANSACTION_FEE_RATE[self.name_code()]
        self.fee_per_unit = Util.DICT_TRANSACTION_FEE[self.name_code()]
        self._margin_rate = Util.DICT_FUTURE_MARGIN_RATE[self.name_code()]
        self.df_future_c2 = df_future_c2
        self.df_underlying_index_daily = df_underlying_index_daily
        self.df_all_futures_daily = df_futures_all_daily
        self.idx_underlying_index = -1
        self.underlying_state_daily = None

    def __repr__(self) -> str:
        return 'BaseInstrument(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    def next(self):
        super().next()
        if self.df_underlying_index_daily is None: return
        if self.underlying_state_daily is None or self.eval_date != self.eval_datetime.date():
            self.idx_underlying_index += 1
            self.underlying_state_daily = self.df_underlying_index_daily.loc[self.idx_underlying_index]

    """ getters """

    def margin_rate(self) -> Union[float, None]:
        return self._margin_rate

    def get_initial_margin(self,long_short:LongShort) -> Union[float, None]:
        # pre_settle_price = self.mktprice_last_settlement()
        margin = self.mktprice_close() * self._margin_rate * self._multiplier
        return margin

    # TODO: USE SETTLEMENT PRICE
    def get_maintain_margin(self,long_short:LongShort) -> Union[float, None]:
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

    def is_margin_trade(self, long_short):
        return True

    def is_mtm(self):
        return True

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

    def execute_order(self, order: Order, slippage=0, execute_type: ExecuteType = ExecuteType.EXECUTE_ALL_UNITS):
        if order is None or order.trade_unit == 0: return
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

    # """ 高频数据下按照当日成交量加权均价开仓，结束后时间点移动到下一个交易日第一个时间点。 """
    def execute_order_by_VWAP(self, order: Order, slippage=0,
                              execute_type: ExecuteType = ExecuteType.EXECUTE_ALL_UNITS):
        if self.frequency in Util.LOW_FREQUENT:
            return
        else:
            total_trade_value = 0.0
            total_volume_value = 0.0
            while not self.is_last_minute():
                total_trade_value += self.mktprice_close() * self.trading_volume()
                total_volume_value += self.trading_volume()
                self.next()
            total_trade_value += self.mktprice_close() * self.trading_volume()
            total_volume_value += self.trading_volume()
            volume_weighted_price = total_trade_value / total_volume_value
            order.trade_price = volume_weighted_price
            execution_record = self.execute_order(order, slippage, execute_type)
            return execution_record

    def shift_contract_by_VWAP(self, id_c1: str, id_c2: str, hold_unit: int, open_unit: int,
                               long_short: LongShort, slippage, execute_type):
        if long_short == LongShort.LONG:
            close_order_long_short = LongShort.SHORT
        else:
            close_order_long_short = LongShort.LONG
        close_order = Order(dt_trade=self.eval_date, id_instrument=id_c1, trade_unit=hold_unit,
                            trade_price=None, time_signal=self.eval_datetime, long_short=close_order_long_short)
        # TODO: OPEN ORDER UNIT SHOULD BE RECALCULATED BY DELTA.
        open_order = Order(dt_trade=self.eval_date, id_instrument=id_c2, trade_unit=open_unit,
                           trade_price=None, time_signal=self.eval_datetime, long_short=long_short)
        if self.frequency in Util.LOW_FREQUENT:
            return
        else:
            df_c1_today = self.df_all_futures_daily[(self.df_all_futures_daily[Util.DT_DATE] == self.eval_date) & (
                self.df_all_futures_daily[Util.ID_INSTRUMENT] == id_c1)]
            total_trade_value_c1 = df_c1_today[Util.AMT_TRADING_VALUE].values[0]
            total_volume_c1 = df_c1_today[Util.AMT_TRADING_VOLUME].values[0]
            # volume_weighted_price_c1 = total_trade_value_c1 / (total_volume_c1 * self.multiplier())
            price_c1 = (df_c1_today[Util.AMT_CLOSE].values[0]+df_c1_today[Util.AMT_OPEN].values[0])/2.0
            total_trade_value_c2 = 0.0
            total_volume_c2 = 0.0
            while not self.is_last_minute():
                total_trade_value_c2 += self.mktprice_close() * self.trading_volume() * self.multiplier()
                total_volume_c2 += self.trading_volume()
                self.next()
            total_trade_value_c2 += self.mktprice_close() * self.trading_volume() * self.multiplier()
            total_volume_c2 += self.trading_volume()
            volume_weighted_price_c2 = total_trade_value_c2 / (total_volume_c2 * self.multiplier())
            # close_order.trade_price = volume_weighted_price_c1
            close_order.trade_price = price_c1
            open_order.trade_price = volume_weighted_price_c2
            close_execution_record = self.execute_order(close_order, slippage, execute_type)
            open_execution_record = self.execute_order(open_order, slippage, execute_type)
            return close_execution_record, open_execution_record
