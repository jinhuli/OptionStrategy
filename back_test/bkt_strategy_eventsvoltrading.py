from back_test.bkt_strategy import BktOptionStrategy
import QuantLib as ql
import numpy as np
import datetime
from back_test.bkt_util import BktUtil
from back_test.data_option import get_50option_mktdata as get_mktdata,get_eventsdata

class BktStrategyEventVol(BktOptionStrategy):


    def __init__(self, df_option_metrics,df_events, money_utilization=0.2, init_fund=100000000.0, tick_size=0.0001,
                 fee_rate=2.0/10000,nbr_slippage=0, max_money_utilization=0.5):

        BktOptionStrategy.__init__(self, df_option_metrics, money_utilization, init_fund, tick_size,
                 fee_rate,nbr_slippage, max_money_utilization)
        self.df_events = df_events.sort_values(by='dt_impact_beg',ascending=True).reset_index()
        self.moneyness = 0

    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        idx_event = 0
        print(self.df_events)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            dt_event = self.df_events.loc[idx_event,'dt_impact_beg']
            dt_volpeak = self.df_events.loc[idx_event,'dt_vol_peak']
            cd_trade_deriction = self.df_events.loc[idx_event,'cd_trade_direction']
            evalDate = bkt_optionset.eval_date

            df_metrics_today = bkt_optionset.df_daily_state

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate, df_metrics_today, self.util.col_close)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break

            """ 持有期较长或者标的价格大幅变化--可能需要调整Delta中性 """


            """ 快到期前移仓换月 (5日)"""
            # min_maturity = self.util.to_dt_date(
            #     self.calendar.advance(self.util.to_ql_date(evalDate), ql.Period(5, ql.Days)))
            # if self.flag_trade:
            #     if self.holdings_mdt < min_maturity:
            #         # self.bkt_optionset.update_bktoptionset_mdts()
            #         mdts = sorted(self.bkt_optionset.keys())
            #         mdt_next = mdts[mdts.index(self.holdings_mdt)+1]
            #         df = self.bkt_optionset.get_straddle(0,mdt_next)
            """ Open position on event day, close on vol peak day"""
            if evalDate == dt_event:
                print(idx_event,' ',evalDate,' open position')

                # optionset = self.bkt_optionset.bktoptionset
                # eligible_options = self.get_mdt1_candidate_set(evalDate, self.bkt_optionset.bktoptionset)
                if self.flag_trade :
                    print(evalDate,' Trying to open position before previous one closed!')
                    return

                """ Select Strategy and Open Position"""
                if cd_trade_deriction == -1:
                    df_open_position = self.bkt_optionset.get_put(self.moneyness,self.get_1st_eligible_maturity(evalDate))  # 选择跨式期权头寸
                elif cd_trade_deriction == 1:
                    df_open_position = self.bkt_optionset.get_call(self.moneyness,self.get_1st_eligible_maturity(evalDate))
                else:
                    df_open_position = self.bkt_optionset.get_straddle(self.moneyness,self.get_1st_eligible_maturity(evalDate))

                fund = 0
                for (idx, row) in df_open_position.iterrows():
                    bktoption = row[self.util.bktoption]
                    delta0_ratio = row[self.util.unit]
                    margin = bktoption.get_init_margin() # 每手初始保证金
                    fund += margin*delta0_ratio
                unit = bkt.cash/fund
                delta = 0
                for (idx, row) in df_open_position.iterrows():
                    bktoption = row[self.util.bktoption]
                    delta0_ratio = row[self.util.unit]
                    trade_unit = np.floor(delta0_ratio*unit)
                    delta += bktoption.get_delta()*unit*delta0_ratio
                    bkt.open_long(evalDate, bktoption, trade_unit)
                    self.holdings_mdt = bktoption.maturitydt
                self.flag_trade = True

            """ Close position """
            if evalDate == dt_volpeak:
                print(idx_event,' ',evalDate,' close position')

                self.flag_trade = False
                for bktoption in bkt.holdings:
                    bkt.close_position(evalDate,bktoption)
                idx_event += 1

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt.mkm_update(evalDate, df_metrics_today, self.util.col_close)
            print(evalDate,bkt_optionset.eval_date, ' , ', bkt.npv)  # npv是组合净值，期初为1
            bkt_optionset.next()
            if idx_event >= len(self.df_events):return


"""Back Test Settings"""
# start_date = datetime.date(2016, 1, 1)
start_date = datetime.date(2017, 6, 1)
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




















