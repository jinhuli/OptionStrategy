from back_test.bkt_account import BktAccount
from back_test.bkt_option_set import BktOptionSet
from back_test.bkt_strategy import BktOptionStrategy
import QuantLib as ql
from back_test.bkt_util import BktUtil
import pandas as pd
import datetime
from back_test.bkt_util import BktUtil
from back_test.data_option import get_50option_mktdata as get_mktdata,get_eventsdata

class BktStrategyEventVol(BktOptionStrategy):


    def __init__(self, df_option_metrics,df_events, hp=20, money_utilization=0.2, init_fund=100000000.0, tick_size=0.0001,
                 fee_rate=2.0/10000,nbr_slippage=0, max_money_utilization=0.5, buy_ratio=0.5,
                 sell_ratio=0.5):

        BktOptionStrategy.__init__(self, df_option_metrics, hp, money_utilization, init_fund, tick_size,
                 fee_rate,nbr_slippage, max_money_utilization)
        self.df_events = df_events.sort_values(by='dt_impact_beg',ascending=True).reset_index()
        self.buy_ratio = buy_ratio
        self.sell_ratio = sell_ratio

    def update_holding_period(self,hp):
        self.holding_period = hp


    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account

        while bkt_optionset.index < len(bkt_optionset.dt_list):
            # if bkt_optionset.index == 0:
            #     bkt_optionset.next()
            #     continue
            idx_event = 0
            dt_event = self.df_events.loc[idx_event,'dt_impact_beg']
            dt_volpeak = self.df_events.loc[idx_event,'dt_vol_peak']
            evalDate = bkt_optionset.eval_date
            # hp_enddate = self.to_dt_date(e
            #     self.calendar.advance(self.to_ql_date(evalDate), ql.Period(self.holding_period, ql.Days)))
            # df_metrics_today = self.df_option_metrics[(self.df_option_metrics[self.col_date] == evalDate)]

            df_metrics_today = bkt_optionset.df_daily_state
            """回测期最后一天全部清仓"""
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate, df_metrics_today, self.col_close)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break

            """清仓到期头寸"""
            for bktoption in bkt.holdings:
                if bktoption.maturitydt == evalDate:
                    print('Liquidate position at maturity : ', evalDate, ' , ', bktoption.maturitydt)
                    bkt.close_position(evalDate, bktoption)

            """Event day，open vol position"""
            if evalDate == dt_event:
                """ Select Strategy"""
                df_open_position = self.long_straddle(df_metrics_today,0,self.cash)
                for (idx, row) in df_open_position.iterrows():
                    bktoption = row['bktoption']
                    unit = row['unit']
                    bkt.open_long(evalDate, bktoption, unit)

            """持有期holding_period满，进行调仓 """
            if (bkt_optionset.index - 1) % self.holding_period == 0 or not self.flag_trade:
                print('调仓 : ', evalDate)
                invest_fund = bkt.cash * self.money_utl
                df_option = self.get_ranked_options(evalDate)
                df_option = self.get_weighted_ls(invest_fund, df_option)
                df_buy = df_option[df_option['weight'] > 0]
                df_sell = df_option[df_option['weight'] < 0]

                """平仓：将手中头寸进行平仓，除非当前头寸在新一轮持有期中仍判断持有相同的方向，则不会先平仓再开仓"""
                for bktoption in bkt.holdings:
                    if bktoption.maturitydt <= hp_enddate:
                        bkt.close_position(evalDate, bktoption)
                    else:
                        if bktoption.trade_long_short == 1 and bktoption in df_buy['bktoption']: continue
                        if bktoption.trade_long_short == -1 and bktoption in df_sell['bktoption']: continue
                        bkt.close_position(evalDate, bktoption)

                """开仓：做多df_buy，做空df_sell"""
                if len(df_buy) + len(df_sell) == 0:
                    self.flag_trade = False
                else:

                    for (idx, row) in df_buy.iterrows():
                        bktoption = row['bktoption']
                        unit = row['unit']
                        if bktoption in bkt.holdings and bktoption.trade_flag_open:
                            bkt.rebalance_position(evalDate, bktoption, unit)
                        else:
                            bkt.open_long(evalDate, bktoption, unit)
                    for (idx, row) in df_sell.iterrows():
                        bktoption = row['bktoption']
                        unit = row['unit']
                        if bktoption in bkt.holdings and bktoption.trade_flag_open:
                            bkt.rebalance_position(evalDate, bktoption, unit)
                        else:
                            bkt.open_short(evalDate, bktoption, unit)
                    self.flag_trade = True

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt.mkm_update(evalDate, df_metrics_today, self.col_close)
            print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
            bkt_optionset.next()


"""Back Test Settings"""
# start_date = datetime.date(2016, 1, 1)
start_date = datetime.date(2017, 1, 1)
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
bkt.run()


# bkt.bkt_account.df_account.to_csv('../save_results/df_account.csv')
# bkt.bkt_account.df_trading_book.to_csv('../save_results/df_trading_book.csv')
# bkt.bkt_account.df_trading_records.to_csv('../save_results/df_trading_records.csv')

# bkt.return_analysis()





















