from unittest import TestCase
from back_test.model.base_future_coutinuous import BaseFutureCoutinuous
from back_test.model.base_account import BaseAccount
from data_access.get_data import  get_dzqh_cf_minute,get_dzqh_cf_daily
import datetime
from back_test.model.trade import Trade
from back_test.model.constant import TradeType, Util, FrequentType
class TestBaseAccount(TestCase):
    @classmethod
    def setUpClass(cls):
        start_date = datetime.date(2017, 10, 1)
        end_date = datetime.date(2017, 11, 21)
        df_cf_minute = get_dzqh_cf_minute(start_date, end_date, 'if')
        df_cf = get_dzqh_cf_daily(start_date, end_date, 'if')
        cls.future = BaseFutureCoutinuous(df_cf_minute, df_cf, frequency=FrequentType.MINUTE)
        cls.future.init()
        cls.account = BaseAccount(Util.BILLION)
        cls.trading_desk = Trade()

    def test_over_all_account(self):
        while self.future.has_next():
            order = self.account.create_trade_order(self.future, TradeType.OPEN_LONG,10)
            execution_res = self.future.execute_order(order)
            self.account.add_record(execution_res, self.future)
            self.assertGreater(self.account.cash, 0,
                               "account cash lower than 0 on order {}".format("order"))
            self.trading_desk.add_pending_order(order)
            self.future.next()

