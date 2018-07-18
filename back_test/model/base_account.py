import pandas as pd
import numpy as np
import datetime
from typing import Union
from back_test.model.abstract_account import AbstractAccount
from back_test.model.base_product import BaseProduct
from back_test.model.constant import Util, TradeType, LongShort
from back_test.model.trade import Order


class BaseAccount():
    def __init__(self, init_fund, leverage=1.0, fee_rate=3.0 / 10000.0, rf=0.03):
        # super().__init__()
        self.df_records = pd.DataFrame()
        self.list_records = []
        self.trade_book = pd.DataFrame(columns=Util.TRADE_BOOK_COLUMN_LIST)
        self.dict_holding = {}  # id_instrument -> Product
        self.account = pd.DataFrame()
        self.init_fund = init_fund
        self.max_leverage = leverage
        self.fee_rate = fee_rate
        self.rf = rf
        self.cash = init_fund  # 现金账户：初始资金为现金
        self.actual_leverage = 0.0
        self.total_mtm_position = 0.0  # 总多空持仓市值加总

        # self.total_asset = init_fund # 投资组合总市值：初始状态只有现金; 每日收盘更新浮盈浮亏；
        # self.total_invest_position_long = 0.0 # 总多头持仓市值：每日收盘计算
        # self.total_invest_posiion_short = 0.0 # 总空头持仓市值：每日收盘计算

    # def add_record_1(self, dt_trade, id_instrument, trade_type, trade_price, trade_cost, trade_unit,
    #                  trade_margin_capital=0.0):
    #     record = pd.DataFrame(data={Util.ID_INSTRUMENT: [id_instrument],
    #                                 Util.DT_TRADE: [dt_trade],
    #                                 Util.TRADE_TYPE: [trade_type],
    #                                 Util.TRADE_PRICE: [trade_price],
    #                                 Util.TRANSACTION_COST: [trade_cost],
    #                                 Util.TRADE_UNIT: [trade_unit],  # 多空体现在unit正负号
    #                                 Util.TRADE_MARGIN_CAPITAL: [trade_margin_capital]
    #                                 })
    #     self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)

    def add_holding(self, base_product: BaseProduct):
        if base_product.id_instrument() not in self.dict_holding.keys():
            self.dict_holding.update({base_product.id_instrument():base_product})


    # TODO : 及时计算账户资产、现金、保证金、杠杆等基本要素
    def add_record(self, execution_record: pd.Series, base_product: BaseProduct):
        if execution_record is None: return
        self.list_records.append(execution_record)
        self.add_holding(base_product)
        id_instrument = execution_record[Util.ID_INSTRUMENT]
        if not self.trade_book.empty and id_instrument in self.trade_book.index:
            book_series = self.trade_book.loc[id_instrument]
            if book_series[Util.TRADE_LONG_SHORT] != execution_record[Util.TRADE_LONG_SHORT]:
                # """ close out """
                if book_series[Util.TRADE_UNIT] == execution_record[Util.TRADE_UNIT]:
                    self.dict_holding.pop(id_instrument, None)
                    book_series[Util.TRADE_UNIT] = 0
                    book_series[Util.TRADE_REALIZED_PNL] += \
                        execution_record[Util.TRADE_UNIT] * execution_record[Util.TRADE_LONG_SHORT] * \
                        (execution_record[Util.TRADE_PRICE] - book_series[Util.AVERAGE_POSITION_COST])
                    book_series[Util.TRADE_BOOK_VALUE] = 0.0
                    book_series[Util.TRADE_MARGIN_CAPITAL] = 0.0
                    self.cash += execution_record[Util.TRADE_MARGIN_CAPITAL]
                    self.cash += book_series[Util.TRADE_REALIZED_PNL]
                # """ close partial """
                elif book_series[Util.TRADE_UNIT] > execution_record[Util.TRADE_UNIT]:
                    closed_ratio = (book_series[Util.TRADE_UNIT] - execution_record[Util.TRADE_UNIT]) / book_series[
                        Util.TRADE_UNIT]
                    book_series[Util.TRADE_UNIT] -= execution_record[Util.TRADE_UNIT]
                    book_series[Util.LAST_PRICE] = execution_record[Util.TRADE_PRICE]
                    margin_released = book_series[Util.TRADE_MARGIN_CAPITAL] * closed_ratio
                    book_series[Util.TRADE_MARGIN_CAPITAL] -= margin_released
                    book_series[Util.TRADE_BOOK_VALUE] = book_series[Util.TRADE_BOOK_VALUE] * closed_ratio
                    realized_pnl = execution_record[Util.TRADE_UNIT] * execution_record[Util.TRADE_LONG_SHORT].value * \
                                   (execution_record[Util.TRADE_PRICE] - book_series[Util.AVERAGE_POSITION_COST])
                    book_series[Util.TRADE_REALIZED_PNL] += realized_pnl
                    self.cash += realized_pnl + margin_released
                # """ open opposite :平仓加反向开仓 """
                else:
                    book_series[Util.TRADE_LONG_SHORT] = execution_record[Util.TRADE_LONG_SHORT]
                    book_series[Util.TRADE_UNIT] = execution_record[Util.TRADE_UNIT] - book_series[Util.TRADE_UNIT]
                    book_series[Util.LAST_PRICE] = execution_record[Util.TRADE_PRICE]
                    actual_margin_required = execution_record[Util.TRADE_MARGIN_CAPITAL] * \
                                             (
                                                 book_series[Util.TRADE_UNIT] / execution_record[
                                                     Util.TRADE_MARGIN_CAPITAL])
                    margin_released = book_series[Util.TRADE_MARGIN_CAPITAL]
                    book_series[Util.TRADE_MARGIN_CAPITAL] = actual_margin_required
                    book_series[Util.TRADE_BOOK_VALUE] = execution_record[Util.TRADE_LONG_SHORT] * book_series[
                        Util.TRADE_UNIT] * execution_record[Util.TRADE_PRICE]
                    book_series[Util.AVERAGE_POSITION_COST] = execution_record[Util.TRADE_PRICE]
                    realized_pnl = book_series[Util.TRADE_UNIT] * book_series[Util.TRADE_LONG_SHORT] * \
                                   (execution_record[Util.TRADE_PRICE] - book_series[Util.AVERAGE_POSITION_COST])
                    book_series[Util.TRADE_REALIZED_PNL] += realized_pnl
                    self.cash += realized_pnl - actual_margin_required + margin_released
            # """ add position """"
            else:
                book_series[Util.TRADE_UNIT] += execution_record[Util.TRADE_UNIT]
                book_series[Util.LAST_PRICE] = execution_record[Util.TRADE_PRICE]
                book_series[Util.TRADE_MARGIN_CAPITAL] += execution_record[Util.TRADE_MARGIN_CAPITAL]
                book_series[Util.TRADE_BOOK_VALUE] += execution_record[Util.TRADE_BOOK_VALUE]
                book_series[Util.AVERAGE_POSITION_COST] = abs(book_series[Util.TRADE_BOOK_VALUE]) / book_series[
                    Util.TRADE_UNIT]
                self.cash -= execution_record[Util.TRADE_MARGIN_CAPITAL]
        else:
            book_series = pd.Series({
                Util.TRADE_UNIT: execution_record[Util.TRADE_UNIT],
                Util.LAST_PRICE: execution_record[Util.TRADE_PRICE],
                Util.TRADE_MARGIN_CAPITAL: execution_record[Util.TRADE_MARGIN_CAPITAL],
                Util.TRADE_BOOK_VALUE: execution_record[Util.TRADE_BOOK_VALUE],
                Util.TRADE_LONG_SHORT: execution_record[Util.TRADE_LONG_SHORT],
                Util.AVERAGE_POSITION_COST: execution_record[Util.TRADE_PRICE],
                Util.TRADE_REALIZED_PNL: 0.0
            })
            self.trade_book.loc[id_instrument] = book_series
            self.cash -= execution_record[Util.TRADE_MARGIN_CAPITAL]
        self.update_account_status()

    def update_account_status(self):
        total_mtm_position = (self.trade_book.loc[:, Util.TRADE_UNIT] * self.trade_book.loc[:, Util.LAST_PRICE]).sum()
        total_margin = self.trade_book[Util.TRADE_MARGIN_CAPITAL].sum()
        self.actual_leverage = total_mtm_position / (self.cash + total_margin)
        self.total_mtm_position = total_mtm_position

    def create_trade_order(self, dt_trade: datetime.date, id_instrument: str,
                          trade_type: TradeType, trade_price: Union[float, None],
                          time_signal: Union[datetime.datetime, None],
                          trade_unit: int = None, long_short=None):
        if trade_type == TradeType.CLOSE_SHORT or trade_type == TradeType.CLOSE_LONG:
            book_series = self.trade_book.loc[id_instrument]
            trade_unit = book_series[Util.TRADE_UNIT]
            if trade_type == TradeType.CLOSE_SHORT and book_series[Util.TRADE_LONG_SHORT] == LongShort.LONG:
                return
            elif trade_type == TradeType.CLOSE_LONG and book_series[Util.TRADE_LONG_SHORT] == LongShort.SHORT:
                return
            order = Order(dt_trade, id_instrument, trade_type, trade_unit, trade_price,
                          time_signal, long_short)
            return order
        max_unit = np.floor(self.get_investable_market_value() / trade_price)
        if max_unit < 1:
            return
        else:
            trade_unit = min(max_unit, trade_unit)
            order = Order(dt_trade, id_instrument, trade_type, trade_unit, trade_price,
                          time_signal, long_short)
            return order

    def get_investable_market_value(self):
        return self.cash * self.max_leverage

    def daily_accounting(self):
        # TODO : recalculate margin requirements in a daily basis.

        return self.account

        # def open_long(self, dt_trade, id_instrument, trade_price, trade_unit, name_code):
        #     trade_type = TradeType.OPEN_LONG
        #     trade_cost = trade_price * trade_unit * self.fee_rate
        #     # TODO: get trade margin capital by product code.
        #     trade_margin_capital = 0.0
        #     self.add_record(dt_trade, id_instrument, trade_type, trade_price, trade_cost, trade_unit, trade_margin_capital)
