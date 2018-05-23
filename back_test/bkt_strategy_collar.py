from back_test.bkt_strategy import BktOptionStrategy,BktOptionIndex
from back_test.bkt_strategy import BktInstrument
import numpy as np
import QuantLib as ql
import datetime
from back_test.data_option import get_50option_mktdata2 as get_mktdata, get_eventsdata, get_index_mktdata



class BktStrategyCollar(BktOptionStrategy):


    def __init__(self, df_option, df_index, money_utilization=0.2, init_fund=100000000.0,
                 cash_reserve_pct=0.3):
        self.validate_data(df_option, df_index)
        BktOptionStrategy.__init__(self, self.df_option, money_utilization=money_utilization,
                                       init_fund=init_fund)
        self.bkt_index = BktInstrument('daily', self.df_index)
        self.cash_reserve_pct = cash_reserve_pct

    def validate_data(self, df_option, df_index):
        dates1 = df_option['dt_date'].unique()
        dates2 = df_index['dt_date'].unique()
        for (idx, dt) in enumerate(dates1):
            if dt != dates2[idx]:
                print(' Recheck dates! option dates and index dates are not equal !')
                self.df_option = None
                self.df_index = None
                return
        self.df_option = df_option
        self.df_index = df_index

    def next(self):
        self.bkt_optionset.next()
        self.bkt_index.next()

    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt_index = self.bkt_index
        bkt_account = self.bkt_account
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            evalDate = bkt_optionset.eval_date
            write_ratio = 1.0
            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt_account.liquidate_all(evalDate)
                bkt_account.mkm_update_portfolio(evalDate,self.portfolio)
                print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
                break

            unit_underlying = 0.0
            if not self.flag_trade:
                inv_fund = (1 - self.cash_reserve_pct) * bkt_account.cash

                """Option: Select Strategy and Open Position"""
                cd_underlying_price = 'close'
                cd_open_position_time = 'close'
                portfolio = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate),bkt_index)

                print(portfolio.optionset[0].id_instrument,portfolio.optionset[0].dt_date,portfolio.optionset[0].underlying_price)

                self.portfolio = portfolio
                unit_underlying = np.floor(inv_fund/portfolio.underlying.mktprice_close)
                bkt_account.update_invest_units_c2(portfolio, write_ratio, unit_underlying)
                bkt_account.open_position(evalDate, portfolio, cd_open_by_price=cd_open_position_time)
                self.flag_trade = True
            else:
                dt = self.util.to_dt_date(self.calendar.advance(self.util.to_ql_date(evalDate), ql.Period(8, ql.Days)))
                call = portfolio.write_call
                put = portfolio.buy_put
                # update portfolio components
                if call.maturitydt <= dt:
                    portfolio = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate), bkt_index)
                    self.portfolio.update_portfolio(buy_put=portfolio.buy_put,write_call=portfolio.write_call)
                    bkt_account.update_invest_units_c2(self.portfolio, write_ratio, unit_underlying)
                    bkt_account.rebalance_position(evalDate, self.portfolio)
                else:
                    spot = portfolio.underlying.mktprice_close
                    k_call = call.strike
                    k_put = put.strike
                    if spot >= 3.0:
                        if k_call - spot <= 1.0 or spot - k_put <= 1.0:
                            portfolio = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate), bkt_index)
                            if portfolio.write_call == None :
                                bkt_account.close_position_option(dt,portfolio.write_call,cd_close_by_price='close')
                                self.flag_trade = False
                            elif portfolio.buy_put == None:
                                bkt_account.close_position_option(dt,portfolio.buy_put,cd_close_by_price='close')
                                self.flag_trade = False
                            self.portfolio.update_portfolio(buy_put=portfolio.buy_put, write_call=portfolio.write_call)
                            bkt_account.update_invest_units_c2(self.portfolio, write_ratio, unit_underlying)
                            bkt_account.rebalance_position(evalDate, self.portfolio)
                    else:
                        if k_call - spot <= 0.5 or spot - k_put <= 0.5:
                            portfolio = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate), bkt_index)
                            if portfolio.write_call == None :
                                bkt_account.close_position_option(dt,portfolio.write_call,cd_close_by_price='close')
                                self.flag_trade = False
                            elif portfolio.buy_put == None:
                                bkt_account.close_position_option(dt,portfolio.buy_put,cd_close_by_price='close')
                                self.flag_trade = False
                            else:
                                self.portfolio.update_portfolio(buy_put=portfolio.buy_put, write_call=portfolio.write_call)
                                bkt_account.update_invest_units_c2(self.portfolio, write_ratio, unit_underlying)
                                bkt_account.rebalance_position(evalDate, self.portfolio)


            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update_portfolio(evalDate,self.portfolio)

            print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.cash)
            self.next()


"""Back Test Settings"""
# end_date = datetime.date(2017, 12, 31)
start_date = datetime.date(2017, 1, 1)
end_date = datetime.date(2018, 5, 5)


"""Collect Mkt Date"""

df_option_metrics = get_mktdata(start_date, end_date)
df_index_metrics = get_index_mktdata(start_date,end_date,'index_50etf')

"""Run Backtest"""

bkt_strategy = BktStrategyCollar(df_option_metrics, df_index_metrics)
bkt_strategy.set_min_holding_days(15)

bkt_strategy.run()

# bkt_strategy.bkt_account.df_account.to_csv('../save_results/bkt_df_account.csv')
# bkt_strategy.bkt_account.df_trading_book.to_csv('../save_results/bkt_df_trading_book.csv')
# bkt_strategy.bkt_account.df_trading_records.to_csv('../save_results/bkt_df_trading_records.csv')
# bkt_strategy.bkt_account.df_ivs.to_csv('../save_results/bkt_df_ivs.csv')
#
bkt_strategy.return_analysis()

