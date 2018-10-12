import numpy as np
import pandas as pd

from back_test.model.base_product import BaseProduct
from back_test.model.constant import Util, TradeType, LongShort, CdTradePrice
from back_test.model.trade import Order
from back_test.model.base_option import BaseOption


class BaseAccount():
    def __init__(self, init_fund, leverage=1.0, rf=0.03):
        # super().__init__()
        # self.df_records = pd.DataFrame()
        # self.list_records = []
        self.trade_records = pd.DataFrame()
        self.trade_book = pd.DataFrame(columns=Util.TRADE_BOOK_COLUMN_LIST)
        self.dict_holding = {}  # id_instrument -> Product
        self.account = pd.DataFrame(columns=Util.ACCOUNT_COLUMNS)
        self.init_fund = init_fund
        self.max_leverage = leverage
        # self.fee_rate = fee_rate
        self.rf = rf
        self.cash = init_fund  # 现金账户：初始资金为现金
        self.actual_leverage = 0.0
        self.realized_pnl = 0.0
        self.trade_book_daily = pd.DataFrame(columns=Util.TRADE_BOOK_COLUMN_LIST)
        self.portfolio_total_value = init_fund
        self.realized_pnl_from_closed_out_positions = 0.0
        # self.cashed_unrealized_pnl_from_mtm_positions = 0.0
        # self.total_mtm_position = 0.0  # 总多空持仓市值(abs value)加总
        # self.total_margin_trade_mtm_position = 0.0

    def add_holding(self, base_product: BaseProduct):
        if base_product.id_instrument() not in self.dict_holding.keys():
            self.dict_holding.update({base_product.id_instrument(): base_product})

    def add_record(self, execution_record: pd.Series, base_product: BaseProduct):
        if execution_record is None: return
        self.add_holding(base_product)
        id_instrument = execution_record[Util.ID_INSTRUMENT]
        if not self.trade_book.empty and id_instrument in self.trade_book.index:
            book_series = self.trade_book.loc[id_instrument]
            # """ New record has opposite direction """
            if book_series[Util.TRADE_LONG_SHORT] != execution_record[Util.TRADE_LONG_SHORT]:
                # """ close out """
                if book_series[Util.TRADE_UNIT] == execution_record[Util.TRADE_UNIT]:
                    trade_long_short = book_series[Util.TRADE_LONG_SHORT]
                    trade_unit = 0
                    last_price = execution_record[Util.TRADE_PRICE]
                    trade_margin_capital = 0.0
                    trade_book_value = 0.0
                    average_position_cost = 0.0
                    # execution_record realized pnl.
                    realized_pnl = book_series[Util.NBR_MULTIPLIER] * book_series[Util.TRADE_UNIT] \
                                   * book_series[Util.TRADE_LONG_SHORT].value * \
                                   (execution_record[Util.TRADE_PRICE] -
                                    book_series[Util.AVERAGE_POSITION_COST])
                    # This position total realized pnl.
                    trade_realized_pnl = book_series[Util.TRADE_REALIZED_PNL] + realized_pnl
                    if base_product.is_margin_trade(trade_long_short):
                        # 开仓为保证金交易，将已实现损益加入现金账户
                        self.cash += book_series[Util.TRADE_MARGIN_CAPITAL] + realized_pnl
                    else: # 无保证金交易（期权买方、股票等）最终平仓收入（- execution_record[Util.TRADE_BOOK_VALUE]）加入现金账户
                        self.cash += - execution_record[Util.TRADE_BOOK_VALUE]
                    position_current_value = 0.0
                    # self.dict_holding.pop(id_instrument, None)
                # """ close partial """
                elif book_series[Util.TRADE_UNIT] > execution_record[Util.TRADE_UNIT]:
                    ratio = execution_record[Util.TRADE_UNIT] / book_series[Util.TRADE_UNIT]
                    trade_long_short = book_series[Util.TRADE_LONG_SHORT]
                    trade_unit = book_series[Util.TRADE_UNIT] - execution_record[Util.TRADE_UNIT]
                    last_price = execution_record[Util.TRADE_PRICE]
                    margin_released = book_series[Util.TRADE_MARGIN_CAPITAL] * ratio
                    trade_margin_capital = book_series[Util.TRADE_MARGIN_CAPITAL] - margin_released
                    trade_book_value = book_series[Util.TRADE_BOOK_VALUE] * (1 - ratio)
                    average_position_cost = book_series[Util.AVERAGE_POSITION_COST]
                    # execution_record realized pnl.
                    realized_pnl = book_series[Util.NBR_MULTIPLIER] * \
                                   execution_record[Util.TRADE_UNIT] * book_series[Util.TRADE_LONG_SHORT].value * \
                                   (execution_record[Util.TRADE_PRICE] - book_series[Util.AVERAGE_POSITION_COST])
                    # This position total realized pnl.
                    trade_realized_pnl = book_series[Util.TRADE_REALIZED_PNL] + realized_pnl
                    if base_product.is_margin_trade(trade_long_short):
                        # 开仓为保证金交易，将已实现损益加入现金账户
                        self.cash += realized_pnl + margin_released
                    else: # 无保证金交易（期权买方、股票等）最终平仓收入（- execution_record[Util.TRADE_BOOK_VALUE]）加入现金账户。
                        self.cash += - execution_record[Util.TRADE_BOOK_VALUE]
                    position_current_value = self.get_position_value(id_instrument, trade_unit,
                                                                     trade_long_short
                                                                     )
                # """ open opposite :平仓加反向开仓 """
                else:
                    trade_long_short = execution_record[Util.TRADE_LONG_SHORT]
                    trade_unit = execution_record[Util.TRADE_UNIT] - book_series[Util.TRADE_UNIT]
                    last_price = execution_record[Util.TRADE_PRICE]
                    trade_margin_capital = execution_record[Util.TRADE_MARGIN_CAPITAL] * \
                                           (1 - book_series[Util.TRADE_UNIT] / execution_record[Util.TRADE_UNIT])
                    margin_released = book_series[Util.TRADE_MARGIN_CAPITAL]
                    trade_book_value = book_series[Util.NBR_MULTIPLIER] * execution_record[Util.TRADE_LONG_SHORT].value \
                                       * trade_unit * last_price
                    average_position_cost = last_price
                    realized_pnl = book_series[Util.NBR_MULTIPLIER] * book_series[Util.TRADE_UNIT] * \
                                   book_series[Util.TRADE_LONG_SHORT].value * \
                                   (execution_record[Util.TRADE_PRICE] - book_series[Util.AVERAGE_POSITION_COST])
                    trade_realized_pnl = book_series[Util.TRADE_REALIZED_PNL] + realized_pnl
                    #
                    if trade_long_short == LongShort.LONG:
                        open_long_short = LongShort.SHORT
                    else:
                        open_long_short = LongShort.LONG
                    if base_product.is_margin_trade(open_long_short):
                        # 开仓为保证金交易，将已实现损益加入现金账户。如果初始开仓为卖出期权，当前反向买入更多的期权无需保证金，
                        # 因而账户需支出权利金（-trade_book_value），返还之前的保证金（margin_released），
                        # 收入之前保证金卖出期权的实际损益（realized_pnl）。
                        if isinstance(base_product, BaseOption):
                            self.cash += realized_pnl + trade_book_value + margin_released
                        else:
                            self.cash += realized_pnl - trade_margin_capital + margin_released
                    else:
                        # 无保证金交易,且仅用于期权，股票不具有反向开仓功能。卖出更多的期权，将一部分之前的买权平仓，对应的权利金为平仓收入全部加入账户。
                        # 现金账户收到权利金（trade_book_value）、支出保证金（trade_margin_capital）
                        self.cash += trade_book_value - trade_margin_capital
                    position_current_value = self.get_position_value(id_instrument, trade_unit,
                                                                     trade_long_short
                                                                     )

            # """ add position : New record has the same direction """"
            else:
                trade_long_short = execution_record[Util.TRADE_LONG_SHORT]
                trade_unit = book_series[Util.TRADE_UNIT] + execution_record[Util.TRADE_UNIT]
                last_price = execution_record[Util.TRADE_PRICE]
                margin_add = execution_record[Util.TRADE_MARGIN_CAPITAL]
                trade_margin_capital = book_series[Util.TRADE_MARGIN_CAPITAL] + margin_add
                trade_book_value = book_series[Util.TRADE_BOOK_VALUE] + execution_record[Util.TRADE_BOOK_VALUE]
                # average_position_cost = abs(book_series[Util.TRADE_BOOK_VALUE]) / (
                #         book_series[Util.TRADE_UNIT] * base_product.multiplier())
                average_position_cost = (book_series[Util.TRADE_UNIT] * book_series[Util.AVERAGE_POSITION_COST]
                                         + execution_record[Util.TRADE_UNIT] * execution_record[Util.TRADE_PRICE]) / \
                                        (execution_record[Util.TRADE_UNIT] + book_series[Util.TRADE_UNIT])
                realized_pnl = 0.0
                trade_realized_pnl = book_series[Util.TRADE_REALIZED_PNL]  # No added realized pnl
                if base_product.is_margin_trade(trade_long_short):
                    # 开仓为保证金交易: 加仓仅需要加保证金
                    self.cash -= margin_add
                else:
                    # 无保证金交易（期权买方、股票等）加仓价值（为正，execution_record[Util.TRADE_BOOK_VALUE]）从现金账户扣除。
                    self.cash -= execution_record[Util.TRADE_BOOK_VALUE]
                position_current_value = self.get_position_value(id_instrument, trade_unit,
                                                                 trade_long_short)
            self.trade_book.loc[id_instrument, Util.TRADE_LONG_SHORT] = trade_long_short
            self.trade_book.loc[id_instrument, Util.LAST_PRICE] = last_price
            self.trade_book.loc[id_instrument, Util.TRADE_UNIT] = trade_unit
            self.trade_book.loc[id_instrument, Util.TRADE_REALIZED_PNL] = trade_realized_pnl
            self.trade_book.loc[id_instrument, Util.AVERAGE_POSITION_COST] = average_position_cost
            self.trade_book.loc[id_instrument, Util.TRADE_BOOK_VALUE] = trade_book_value
            self.trade_book.loc[id_instrument, Util.TRADE_MARGIN_CAPITAL] = trade_margin_capital
        else:
            # """ Open a new position """
            trade_unit = execution_record[Util.TRADE_UNIT]
            trade_long_short = execution_record[Util.TRADE_LONG_SHORT]
            average_position_cost = execution_record[Util.TRADE_PRICE]
            book_series = pd.Series({
                Util.TRADE_UNIT: trade_unit,
                Util.LAST_PRICE: execution_record[Util.TRADE_PRICE],
                Util.TRADE_MARGIN_CAPITAL: execution_record[Util.TRADE_MARGIN_CAPITAL],
                Util.TRADE_BOOK_VALUE: execution_record[Util.TRADE_BOOK_VALUE],
                Util.TRADE_LONG_SHORT: trade_long_short,
                Util.AVERAGE_POSITION_COST: average_position_cost,
                Util.TRADE_REALIZED_PNL: 0.0,
                'mtm_cashed_unrealized_pnl':0.0,
                Util.NBR_MULTIPLIER: base_product.multiplier()
            })
            realized_pnl = 0.0
            self.trade_book.loc[id_instrument] = book_series
            if base_product.is_margin_trade(trade_long_short):
                # 开仓为保证金交易: 加仓仅需要加保证金
                self.cash -= execution_record[Util.TRADE_MARGIN_CAPITAL]
            else:
                # 无保证金交易（期权买方、股票等）加仓价值（execution_record[Util.TRADE_BOOK_VALUE]）从现金账户扣除。
                self.cash -= execution_record[Util.TRADE_BOOK_VALUE]
            position_current_value = self.get_position_value(id_instrument, trade_unit, trade_long_short)
        self.trade_book.loc[id_instrument, Util.POSITION_CURRENT_VALUE] = position_current_value
        self.realized_pnl += realized_pnl
        execution_record[Util.TRADE_REALIZED_PNL] = realized_pnl
        execution_record[Util.CASH] = self.cash
        execution_record[Util.ABS_TRADE_BOOK_VALUE] = abs(execution_record[Util.TRADE_BOOK_VALUE])
        # self.list_records.append(execution_record)
        self.trade_records = self.trade_records.append(execution_record,ignore_index=True)


    # 用于衡量投资组合中非保证金交易部分的市值。
    def get_position_value(self, id_instrument, trade_unit, long_short):
        base_product = self.dict_holding[id_instrument]
        if base_product.is_margin_trade(long_short):
            # 对于保证金交易，头寸价值为未实现损益（unrealized pnl）
            position_current_value = 0.0
        else:
            # 对于非保证金交易：购买股票/期权买方等，头寸价值为当前市值（close price）
            position_current_value = base_product.get_current_value(long_short) * trade_unit * base_product.multiplier()
        return position_current_value

    # 用于杠杆率计算：总保证金交易的市值（按照last trade price，不考虑日内未实现损益），多空绝对市值相加。
    def get_portfolio_margin_trade_scale(self):
        portfolio_margin_trade_scale = 0.0
        for (id_instrument, row) in self.trade_book.iterrows():
            long_short = row[Util.TRADE_LONG_SHORT]
            base_product = self.dict_holding[id_instrument]
            if base_product.is_margin_trade(long_short):
                portfolio_margin_trade_scale += row[Util.TRADE_UNIT] * row[Util.NBR_MULTIPLIER] * row[
                    Util.LAST_PRICE]
        return portfolio_margin_trade_scale

    # For calculate MAX trade unit before execute order.
    def get_investable_cash(self):
        portfolio_margin_trade_scale = self.get_portfolio_margin_trade_scale()
        total_margin_capital = self.get_portfolio_margin_capital()
        investable_cash = max(0.0,
                              self.cash + total_margin_capital -
                              portfolio_margin_trade_scale / self.max_leverage)
        return investable_cash * self.max_leverage

    def create_trade_order_check_leverage(self, base_product,
                           # trade_type: TradeType,
                           long_short: LongShort,
                           trade_unit: int = None,
                           trade_price: float = None,
                           ):
        trade_unit = abs(trade_unit)  # unit的正负取绝对值，方向主要看trade type
        dt_trade = base_product.eval_date
        id_instrument = base_product.id_instrument()
        if trade_price is None:
            trade_price = base_product.mktprice_close()
        time_signal = base_product.eval_datetime
        multiplier = base_product.multiplier()
        # long_short = self.get_long_short(trade_type)
        # if trade_type == TradeType.CLOSE_OUT:
        #     trade_unit = book_series[Util.TRADE_UNIT]
        #     print("Close out all positions! ")
        # Close position时不检查保证金
        # if trade_type == TradeType.CLOSE_SHORT or trade_type == TradeType.CLOSE_LONG:
        if id_instrument in self.trade_book.index:
            book_series = self.trade_book.loc[id_instrument]
            if long_short != self.trade_book.loc[id_instrument, Util.TRADE_LONG_SHORT]:
                # if trade_type == TradeType.CLOSE_SHORT and book_series[Util.TRADE_LONG_SHORT] == LongShort.LONG:
                #     print('no short position to close')
                #     return
                # elif trade_type == TradeType.CLOSE_LONG and book_series[Util.TRADE_LONG_SHORT] == LongShort.SHORT:
                #     print('no short position to close')
                #     return
                order = Order(dt_trade, id_instrument, trade_unit, trade_price,
                              time_signal, long_short)
                return order
        if trade_unit is None:
            raise ValueError("trade_unit is None when opening position !")
        if base_product.is_margin_trade(long_short):
            investable_market_value = self.get_investable_cash()
        else:
            investable_market_value = self.get_investable_cash() * self.max_leverage
        max_unit = np.floor(investable_market_value / (trade_price * multiplier))
        if max_unit < 1:
            return
        else:
            trade_unit = min(max_unit, trade_unit)
            order = Order(dt_trade, id_instrument, trade_unit, trade_price,
                          time_signal, long_short)
            return order

    def get_trade_price(self, cd_trade_price:CdTradePrice, base_product:BaseProduct):
        if cd_trade_price == CdTradePrice.CLOSE:
            return base_product.mktprice_close()
        elif cd_trade_price == CdTradePrice.OPEN:
            return base_product.mktprice_open()
        elif cd_trade_price == CdTradePrice.VOLUME_WEIGHTED:
            return base_product.mktprice_volume_weighted()
        else:
            return

    def create_trade_order(self, base_product:BaseProduct,
                           long_short: LongShort,
                           trade_unit: int = None,
                           cd_trade_price: CdTradePrice = CdTradePrice.CLOSE
                           ):
        trade_unit = abs(trade_unit)  # unit的正负取绝对值，方向主要看trade type
        dt_trade = base_product.eval_date
        id_instrument = base_product.id_instrument()
        trade_price = self.get_trade_price(cd_trade_price,base_product)
        time_signal = base_product.eval_datetime
        order = Order(dt_trade, id_instrument, trade_unit, trade_price,
                          time_signal, long_short)
        return order

    def create_close_order(self, base_product:BaseProduct, cd_trade_price: CdTradePrice = CdTradePrice.CLOSE):
        id_instrument = base_product.id_instrument()
        if id_instrument not in self.trade_book.index:
            return
        else:
            trade_price = self.get_trade_price(cd_trade_price, base_product)
            trade_unit = self.trade_book.loc[id_instrument,Util.TRADE_UNIT]
            if trade_unit == 0: return
            if self.trade_book.loc[id_instrument,Util.TRADE_LONG_SHORT] == LongShort.LONG:
                long_short = LongShort.SHORT
            else:
                long_short = LongShort.LONG
            order = Order(base_product.eval_date, id_instrument, trade_unit, trade_price,
                          base_product.eval_datetime, long_short)
        return order

    def creat_close_out_order(self, cd_trade_price: CdTradePrice = CdTradePrice.CLOSE):
        if self.trade_book.empty:
            return []
        else:
            order_list = []
            for (id_instrument, book_series) in self.trade_book.iterrows():
                trade_unit = book_series[Util.TRADE_UNIT]
                if trade_unit == 0: continue
                if book_series[Util.TRADE_LONG_SHORT] == LongShort.LONG:
                    long_short = LongShort.SHORT
                else:
                    long_short = LongShort.LONG
                base_product = self.dict_holding[id_instrument]
                trade_price = self.get_trade_price(cd_trade_price, base_product)
                order = Order(base_product.eval_date, id_instrument, trade_unit,
                              trade_price=trade_price,
                              time_signal=base_product.eval_datetime, long_short=long_short)
                order_list.append(order)
            return order_list

    def daily_accounting(self, eval_date):
        margin_unrealized_pnl = 0.0
        total_short_scale = 0.0
        total_long_scale = 0.0
        nonmargin_unrealized_pnl = 0.0
        portfolio_delta = 0.0
        for (id_instrument, row) in self.trade_book.iterrows():
            base_product = self.dict_holding[id_instrument]
            trade_unit = row[Util.TRADE_UNIT]
            trade_long_short = row[Util.TRADE_LONG_SHORT]
            price = base_product.mktprice_close()
            if isinstance(base_product,BaseOption):
                iv = base_product.get_implied_vol()
                if iv is None: iv = 0.2
                delta = trade_unit*base_product.multiplier()*base_product.get_delta(iv)*trade_long_short.value
            else:
                delta = 0
            portfolio_delta += delta
            if trade_long_short == LongShort.SHORT:
                total_short_scale -= trade_unit*price*base_product.multiplier()
            else:
                total_long_scale += trade_unit*price*base_product.multiplier()
            unrealized_pnl = trade_long_short.value * (price - row[Util.AVERAGE_POSITION_COST]) * row[Util.TRADE_UNIT] * \
                             row[Util.NBR_MULTIPLIER]
            if base_product.is_margin_trade(trade_long_short):
                margin_unrealized_pnl += unrealized_pnl
            else:
                nonmargin_unrealized_pnl += unrealized_pnl
            trade_margin_capital_add = base_product.get_maintain_margin(trade_long_short) * row[Util.TRADE_UNIT] - \
                                       row[Util.TRADE_MARGIN_CAPITAL]
            self.trade_book.loc[id_instrument, Util.TRADE_MARGIN_CAPITAL] += trade_margin_capital_add
            self.cash -= trade_margin_capital_add
            # Calculate NPV
            position_current_value = self.get_position_value(id_instrument, trade_unit, trade_long_short)
            self.trade_book.loc[id_instrument, Util.POSITION_CURRENT_VALUE] = position_current_value
            self.trade_book.loc[id_instrument, Util.DT_DATE] = eval_date
            if row[Util.TRADE_UNIT] == 0.0: self.dict_holding.pop(id_instrument, None)


        self.trade_book_daily = self.trade_book_daily.append(self.trade_book)
        portfolio_margin_capital = self.get_portfolio_margin_capital()
        portfolio_trades_value = self.get_portfolio_trades_value()
        portfolio_total_value = self.cash + portfolio_margin_capital + \
                                portfolio_trades_value + margin_unrealized_pnl
        total_realized_pnl = self.trade_book[Util.TRADE_REALIZED_PNL].sum()
        self.realized_pnl_from_closed_out_positions += self.trade_book[self.trade_book[Util.TRADE_UNIT] == 0.0][Util.TRADE_REALIZED_PNL].sum()
        # self.cashed_unrealized_pnl_from_mtm_positions += self.trade_book[self.trade_book[Util.TRADE_UNIT] == 0.0]['mtm_cashed_unrealized_pnl'].sum()
        tmp = self.cash + portfolio_margin_capital
        portfolio_total_value2 = self.init_fund + margin_unrealized_pnl + nonmargin_unrealized_pnl + \
                                 total_realized_pnl+ self.realized_pnl_from_closed_out_positions

        portfolio_total_scale = self.get_portfolio_total_scale()
        npv = portfolio_total_value / self.init_fund
        npv2 = portfolio_total_value2 / self.init_fund
        # print('#############',npv,npv2,'#############')
        # print("\n")
        if self.trade_records.empty:
            daily_executed_amount = 0.0
            turnover = 0.0
        else:
            daily_executed_amount = self.trade_records[self.trade_records[Util.DT_TRADE]==eval_date][Util.ABS_TRADE_BOOK_VALUE].sum()
            turnover = daily_executed_amount/self.portfolio_total_value
        actual_leverage = portfolio_total_scale / portfolio_total_value
        self.cash = self.cash*(1+self.rf*(1.0/252.0))
        account_today = pd.Series({
            Util.DT_DATE: eval_date,
            Util.CASH: self.cash,
            Util.PORTFOLIO_MARGIN_CAPITAL: portfolio_margin_capital,
            Util.PORTFOLIO_TRADES_VALUE: portfolio_trades_value,
            Util.PORTFOLIO_VALUE: portfolio_total_value,
            Util.PORTFOLIO_NPV: npv,
            Util.PORTFOLIO_UNREALIZED_PNL: margin_unrealized_pnl+nonmargin_unrealized_pnl,
            Util.PORTFOLIO_LEVERAGE: actual_leverage,
            Util.TRADE_REALIZED_PNL: self.realized_pnl,
            Util.PORTFOLIO_LONG_POSITION_SCALE:total_long_scale,
            Util.PORTFOLIO_SHORT_POSITION_SCALE:total_short_scale,
            Util.MARGIN_UNREALIZED_PNL:margin_unrealized_pnl,
            Util.NONMARGIN_UNREALIZED_PNL:nonmargin_unrealized_pnl,
            Util.PORTFOLIO_DELTA:portfolio_delta,
            Util.DAILY_EXCECUTED_AMOUNT: daily_executed_amount,
            Util.TURNOVER:turnover
        })
        self.account.loc[eval_date] = account_today
        # REMOVE CLEARED TRADES FROM TRADING BOOK
        self.trade_book = self.trade_book[self.trade_book[Util.TRADE_UNIT] != 0.0]
        self.realized_pnl = 0.0
        self.portfolio_total_value = portfolio_total_value


    """ getters from trade book """

    def get_trade_type(self, id_instrument, trade_unit, trade_long_short):
        if id_instrument not in self.trade_book.index:
            if trade_long_short == LongShort.LONG:
                return TradeType.OPEN_LONG
            else:
                return TradeType.OPEN_SHORT
        else:
            hold_unit = self.trade_book.loc[id_instrument, Util.TRADE_UNIT]
            hold_long_short = self.trade_book.loc[id_instrument, Util.TRADE_LONG_SHORT]
            if hold_long_short != trade_long_short:  # OPPOSITE DIRECTION
                if trade_unit > hold_unit:  # TODO:反开仓暂时按开仓处理，需检查开仓账户现金充足率
                    if trade_long_short == LongShort.LONG:
                        return TradeType.OPEN_LONG
                    else:
                        return TradeType.OPEN_SHORT
                else:
                    if hold_long_short == LongShort.LONG:
                        return TradeType.CLOSE_LONG
                    else:
                        return TradeType.CLOSE_SHORT
            else:  # SAME DIRECTION
                if trade_long_short == LongShort.LONG:
                    return TradeType.OPEN_LONG
                else:
                    return TradeType.OPEN_SHORT

    def get_long_short(self, trade_type):
        if trade_type == TradeType.OPEN_LONG or trade_type == TradeType.CLOSE_SHORT:
            long_short = LongShort.LONG
        else:
            long_short = LongShort.SHORT
        return long_short

    # 投资组合总保证金金额
    def get_portfolio_margin_capital(self):
        if self.trade_book.empty:
            portfolio_margin_capital = 0.0
        else:
            portfolio_margin_capital = self.trade_book[Util.TRADE_MARGIN_CAPITAL].sum()
        return portfolio_margin_capital

    # 投资组合总头寸价值（不包含现金、保证金的价值）
    def get_portfolio_trades_value(self):
        if self.trade_book.empty:
            res = 0.0
        else:
            res = self.trade_book[Util.POSITION_CURRENT_VALUE].sum()
        return res

    # 投资组合的总头寸市值（多空市值的绝对值加总）
    def get_portfolio_total_scale(self):
        if self.trade_book.empty:
            res = 0.0
        else:
            res = (self.trade_book.loc[:, Util.TRADE_UNIT] * self.trade_book.loc[:, Util.LAST_PRICE]
                   * self.trade_book[Util.NBR_MULTIPLIER]).sum()
        return res


    def get_maxdrawdown(self,netvalue):
        '''
        最大回撤率计算
        '''
        maxdrawdowns = pd.Series(index=netvalue.index)
        for i in np.arange(len(netvalue.index)):
            highpoint = netvalue.iloc[0:(i + 1)].max()
            if highpoint == netvalue.iloc[i]:
                maxdrawdowns.iloc[i] = 0
            else:
                maxdrawdowns.iloc[i] = netvalue.iloc[i] / highpoint - 1

        return maxdrawdowns

    def get_netvalue_analysis(self, netvalue, freq='D'):
        '''由净值序列进行指标统计,netvalue应为Series'''
        if freq == 'D':
            oneyear = 252
        elif freq == 'W':
            oneyear = 50
        elif freq == 'M':
            oneyear = 12
        else:
            print('Not Right freq')
        # 交易次数
        tradeslen = netvalue.shape[0]
        # 收益率序列
        tmp = netvalue.shift()
        tmp[0] = 1
        returns = netvalue / tmp - 1
        # 累计收益率
        totalreturn = netvalue.iloc[-1] - 1
        # 年化收益率
        return_yr = (1 + totalreturn) ** (oneyear / tradeslen) - 1
        # 年化波动率
        volatility_yr = np.std(returns, ddof=0) * np.sqrt(oneyear)
        # 夏普比率
        sharpe = (return_yr - self.rf) / volatility_yr
        # 回撤
        drawdowns = self.get_maxdrawdown(netvalue)
        # 最大回撤
        maxdrawdown = min(drawdowns)
        # 收益风险比
        profit_risk_ratio = return_yr / np.abs(maxdrawdown)
        # 盈利次数
        win_count = (returns >= 0).sum()
        # 亏损次数
        lose_count = (returns < 0).sum()
        # 胜率
        win_rate = win_count / (win_count + lose_count)
        # 盈亏比
        p_over_l = returns[returns > 0].mean() / np.abs(returns[returns < 0].mean())
        r = pd.Series()
        r['累计收益率'] = totalreturn
        r['年化收益率'] = return_yr
        r['年化波动率'] = volatility_yr
        r['最大回撤率'] = maxdrawdown
        r['胜率(' + freq + ')'] = win_rate
        r['盈亏比'] = p_over_l
        r['夏普比率'] = sharpe
        r['Calmar比'] = profit_risk_ratio

        return r

    def get_monthly_turnover(self, df_account:pd.DataFrame):
        df_account['year_month'] = df_account[Util.DT_DATE].apply(lambda x: str(x.year)+str(x.month))
        trade_amount = df_account.groupby(['year_month'])[Util.DAILY_EXCECUTED_AMOUNT].sum()
        init_portfolio_value = df_account.groupby(['year_month'])[Util.PORTFOLIO_VALUE].first()
        turnover_monthly = trade_amount/init_portfolio_value
        turnover_avg = turnover_monthly.mean()
        return turnover_avg

    def analysis(self):
        res = self.get_netvalue_analysis(self.account[Util.PORTFOLIO_NPV])
        res['平均换手率(月)'] = self.get_monthly_turnover(self.account)
        return res