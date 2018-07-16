import pandas as pd
from back_test.model.abstract_account import AbstractAccount
from back_test.model.constant import Util


class BaseAccount(AbstractAccount):
    def __init__(self, init_fund, leverage, fee_rate):
        super().__init__()
        self.df_records = pd.DataFrame()
        self.account = pd.DataFrame()
        self.init_fund = init_fund
        self.leverage = leverage
        self.fee_rate = fee_rate
        self.cash = init_fund # 现金账户：初始资金为现金
        self.total_portfolio_value = init_fund # 投资组合总市值：初始状态只有现金
        self.total_invest_market_value = 0.0 # 投资市值，不包括现金

    def add_trade_record(self, dt_trade, id_instrument, trade_type, trade_price, trade_cost, trade_unit,
                         option_premium=None,
                         trade_margin_capital=None):
        record = pd.DataFrame(data={Util.ID_INSTRUMENT: [id_instrument],
                                    Util.DT_TRADE: [dt_trade],
                                    Util.TRADE_TYPE: [trade_type],
                                    Util.TRADE_PRICE: [trade_price],
                                    Util.TRADE_COST: [trade_cost],
                                    Util.TRADE_UNIT: [trade_unit],
                                    Util.OPTION_PREMIIUM: [option_premium],
                                    Util.TRADE_MARGIN_CAPITAL: [trade_margin_capital]
                                    })
        self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)

    def get_investable_market_value(self):
        return self.cash*self.leverage

    def get_current_leverage(self):
        return self.total_portfolio_value/self.cash

    def daily_accounting(self):
        # TODO : recalculate margin requirements in a dauly basis.
        
        return self.account
