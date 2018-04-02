import QuantLib as ql
import datetime
from back_test.bkt_util import BktUtil
from back_test.bkt_strategy_longshort import CarryLongShort_RW as strategy
from back_test.data_option import get_50option_mktdata


"""Back Test Settings"""
# start_date = datetime.date(2016, 1, 1)
start_date = datetime.date(2017, 1, 1)
end_date = datetime.date(2017, 12, 31)
calendar = ql.China()
daycounter = ql.ActualActual()
util = BktUtil()
open_trades = []


"""Collect Mkt Date"""

df_option_metrics = get_50option_mktdata(start_date,end_date)


"""Run Backtest"""
hp = 20

bkt = strategy(df_option_metrics,hp,money_utilization=0.2,buy_ratio = 0.5,sell_ratio = 0.5,
                        nbr_top_bottom = 2)
bkt.set_option_type('put')
bkt.set_trade_type(util.long_top)
# bkt.set_trade_type(util.long_bottom)
bkt.set_min_ttm(hp+1)
bkt.set_max_ttm(40)
bkt.set_min_trading_volume(200)
# bkt.set_moneyness_type('atm')
bkt.run()


bkt.bkt_account.df_account.to_csv('../save_results/df_account.csv')
bkt.bkt_account.df_trading_book.to_csv('../save_results/df_trading_book.csv')
bkt.bkt_account.df_trading_records.to_csv('../save_results/df_trading_records.csv')

bkt.return_analysis()



































