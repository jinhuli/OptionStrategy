from back_test.bkt_strategy import BktOptionStrategy
import QuantLib as ql
import numpy as np
import datetime
from back_test.bkt_util import BktUtil
from back_test.data_option import get_50option_mktdata as get_mktdata,get_eventsdata

class BktStrategyEventVol(BktOptionStrategy):


    def __init__(self, df_option_metrics,df_events, hp=20, money_utilization=0.2, init_fund=100000000.0, tick_size=0.0001,
                 fee_rate=2.0/10000,nbr_slippage=0, max_money_utilization=0.5):

        BktOptionStrategy.__init__(self, df_option_metrics, hp, money_utilization, init_fund, tick_size,
                 fee_rate,nbr_slippage, max_money_utilization)
        self.df_events = df_events.sort_values(by='dt_impact_beg',ascending=True).reset_index()
        self.moneyness = 0

    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        idx_event = 0
        print(self.df_events)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            evalDate = bkt_optionset.eval_date







            print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
            bkt_optionset.next()
            if idx_event >= len(self.df_events):return


"""Back Test Settings"""
# start_date = datetime.date(2016, 1, 1)
start_date = datetime.date(2015, 6, 1)
end_date = datetime.date(2017, 12, 31)
calendar = ql.China()
daycounter = ql.ActualActual()
util = BktUtil()
open_trades = []


"""Collect Mkt Date"""

df_option_metrics = get_mktdata(start_date,end_date)
df_events = get_eventsdata(start_date,end_date)

"""Run Backtest"""

bkt = BktStrategyEventVol(df_option_metrics,df_events)
bkt.set_min_ttm(20)
bkt.run()


# bkt.bkt_account.df_account.to_csv('../save_results/df_account.csv')
# bkt.bkt_account.df_trading_book.to_csv('../save_results/df_trading_book.csv')
# bkt.bkt_account.df_trading_records.to_csv('../save_results/df_trading_records.csv')

bkt.return_analysis()
print(bkt.bkt_account.df_trading_records)




















