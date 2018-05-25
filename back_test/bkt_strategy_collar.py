from back_test.bkt_strategy import BktOptionStrategy,BktOptionIndex
from back_test.bkt_strategy import BktInstrument
import numpy as np
import QuantLib as ql
import datetime
from back_test.data_option import get_50option_mktdata, get_index_ma, get_index_mktdata



class BktStrategyCollar(BktOptionStrategy):


    def __init__(self, df_option, df_index, money_utilization=0.2, init_fund=100000000.0,
                 cash_reserve_pct=0.15):
        self.validate_data(df_option, df_index)
        BktOptionStrategy.__init__(self, self.df_option, money_utilization=money_utilization,
                                       init_fund=init_fund)
        self.bkt_index = BktInstrument('daily', self.df_index)
        self.cash_reserve_pct = cash_reserve_pct
        self.write_ratio = 1.0

    def validate_data(self, df_option, df_index):
        dates1 = df_option['dt_date'].unique()
        dates2 = df_index['dt_date'].unique()
        if dates1.all() != dates2.all():
            print(' Recheck dates! option dates and index dates are not equal !')
            self.df_option = None
            self.df_index = None
            return
        self.df_option = df_option
        self.df_index = df_index

    def set_index_ma(self,df_index_ma):
        self.index_ma = df_index_ma

    def set_volatility_ma(self,df_vol_ma):
        self.volatility_ma = df_vol_ma

    def next(self):
        self.bkt_optionset.next()
        self.bkt_index.next()

    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt_index = self.bkt_index
        bkt_account = self.bkt_account
        print(bkt_optionset.dt_list)
        print(bkt_index.dt_list)
        inv_fund = (1 - self.cash_reserve_pct) * bkt_account.cash
        evalDate = bkt_optionset.eval_date
        """Option: Select Strategy and Open Position"""
        cd_underlying_price = 'close'
        cd_open_position_time = 'close'
        self.portfolio = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate), bkt_index)

        unit_underlying = np.floor(inv_fund / self.portfolio.underlying.mktprice_close)
        bkt_account.update_invest_units_c2(self.portfolio, self.write_ratio, unit_underlying)
        bkt_account.open_portfolio(evalDate, self.portfolio, cd_open_by_price=cd_open_position_time)
        self.flag_trade = True
        bkt_account.mkm_update_portfolio(evalDate, self.portfolio)
        print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.cash)
        moneyness_call = -1
        moneyness_put = -2
        write_ratio = 1.0
        buy_ratio = 1.0
        flag_vol = self.util.neutrual
        flag_index = self.util.neutrual
        while bkt_optionset.index < len(bkt_optionset.dt_list):

            self.next()
            evalDate = bkt_optionset.eval_date

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt_account.close_portfolio(evalDate,self.portfolio,cd_close_by_price='close')
                bkt_account.mkm_update_portfolio(evalDate,self.portfolio)
                print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
                break

            """ 根据动量指标与波动率指标调整write ratio """
            vol_ma = self.volatility_ma.loc[evalDate,'ma_3']
            index_ma = self.index_ma.loc[evalDate,'amt_close']
            vol_signal = self.boll_signal(flag_vol,vol_ma,self.volatility_ma.loc[evalDate])
            index_signal = self.boll_signal(flag_index,index_ma,self.index_ma.loc[evalDate])
            " research paper method"
            if vol_signal != None:
                self.flag_trade = False
                if vol_signal == self.util.long:
                    print('!!! vol signal : long')
                    write_ratio = 0.5
                    if index_signal == self.util.short: buy_ratio = 1.5
                    else: buy_ratio = 1.0
                elif vol_signal == self.util.short:
                    print('!!! vol signal : short')
                    buy_ratio = 0.5
                    if index_signal == self.util.short:
                        write_ratio = 1.5
                    else:
                        write_ratio = 0.5
                else:
                    print('!!! vol signal : neutral')
                    if index_signal == self.util.short:
                        write_ratio = 1.5
                        buy_ratio = 1.5
                    elif index_signal == self.util.long:
                        write_ratio = 0.5
                        buy_ratio = 0.5
                    else:
                        write_ratio = 1.0
                        buy_ratio = 1.0
                flag_vol = vol_signal

            if index_signal != None:
                self.flag_trade = False
                if index_signal == self.util.long:
                    print('!!! index signal : long')
                    moneyness_call = -2
                    moneyness_put = -3
                # elif index_signal == self.util.short:
                #     print('!!! index signal : short')
                #     moneyness_call = -1
                #     moneyness_put = -1
                else:
                    moneyness_call = -1
                    moneyness_put = -1
                    print('!!! index signal : shot/neutral')
                flag_index = index_signal

            # if vol_signal != flag_vol:
            #     self.flag_trade = False
            #     if vol_signal == self.util.long:
            #         print('!!! vol signal : long')
            #         write_ratio = 0.5
            #         if index_signal == self.util.short: buy_ratio = 1.25
            #     elif vol_signal == self.util.short:
            #         print('!!! vol signal : short')
            #         if index_signal == self.util.short:
            #             write_ratio = 1.25
            #         else:
            #             write_ratio = 0.75
            #             buy_ratio = 0.75
            #     else:
            #         write_ratio = self.write_ratio
            #         print('!!! vol signal : neutral')

            if not self.flag_trade:
                portfolio_new = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate),bkt_index,
                                                              moneyness_call=moneyness_call,moneyness_put=moneyness_put)
                self.portfolio.update_portfolio(buy_put=portfolio_new.buy_put, write_call=portfolio_new.write_call)
                bkt_account.update_invest_units_c2(self.portfolio, write_ratio, unit_underlying,buy_ratio=buy_ratio)
                bkt_account.rebalance_portfolio(evalDate, self.portfolio)
                self.flag_trade = True
            else:
                dt = self.util.to_dt_date(self.calendar.advance(self.util.to_ql_date(evalDate), ql.Period(8, ql.Days)))
                call = self.portfolio.write_call
                put = self.portfolio.buy_put
                flag_update = False
                """Update portfolio when formar portfolio is  maturity reaches, spot nears strike price"""
                if call == None or put == None:
                    flag_update = True
                elif call.maturitydt <= dt:
                    flag_update = True
                else:
                    spot = self.portfolio.underlying.mktprice_close
                    k_call = call.adj_strike
                    k_put = put.adj_strike
                    if spot >= 3.0:
                        if k_call - spot <= 0.1 or spot - k_put <= 0.1:
                            flag_update = True
                            print('1')
                    else:
                        if k_call - spot <= 0.05 or spot - k_put <= 0.05:
                            flag_update = True
                            print('2')
                if flag_update:
                    portfolio_new = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate), bkt_index,
                                                                  moneyness_call=moneyness_call, moneyness_put=moneyness_put)
                    self.portfolio.update_portfolio(buy_put=portfolio_new.buy_put, write_call=portfolio_new.write_call)
                    bkt_account.update_invest_units_c2(self.portfolio, write_ratio, unit_underlying,buy_ratio=buy_ratio)
                    bkt_account.rebalance_portfolio(evalDate, self.portfolio)
            if self.portfolio.write_call == None or self.portfolio.buy_put==None:
                self.flag_trade = False
                print(evalDate, 'No complete collar portfolio constructed, try next day !')
            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update_portfolio(evalDate,self.portfolio)
            print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.mtm_long_positions)



"""Back Test Settings"""
start_date = datetime.date(2015, 3, 20)
# start_date = datetime.date(2018, 3, 13)
end_date = datetime.date(2018, 5, 21)


"""Collect Mkt Date"""

df_option_metrics = get_50option_mktdata(start_date, end_date)
df_index_metrics = get_index_mktdata(start_date,end_date,'index_50etf')
"""Run Backtest"""


bkt_strategy = BktStrategyCollar(df_option_metrics, df_index_metrics)
bkt_strategy.set_min_holding_days(15)


# bkt_strategy.get_moving_average_signal(get_index_ma(start_date,end_date,'index_cvix'))
df_vix_boll = bkt_strategy.get_bollinger_signal(get_index_ma(start_date,end_date,'index_cvix'))
df_index_boll = bkt_strategy.get_bollinger_signal(get_index_ma(start_date,end_date,'index_50etf'))
bkt_strategy.set_index_ma(df_index_boll)
bkt_strategy.set_volatility_ma(df_vix_boll)
df_vix_boll.to_csv('../df_vix_boll.csv')
df_index_boll.to_csv('../df_index_boll.csv')

bkt_strategy.run()

bkt_strategy.bkt_account.df_account.to_csv('../save_results/bkt_df_account.csv')
bkt_strategy.bkt_account.df_trading_book.to_csv('../save_results/bkt_df_trading_book.csv')
bkt_strategy.bkt_account.df_trading_records.to_csv('../save_results/bkt_df_trading_records.csv')
# bkt_strategy.bkt_account.df_ivs.to_csv('../save_results/bkt_df_ivs.csv')
#
benckmark = df_index_metrics[bkt_strategy.util.col_close].tolist()
b = np.array(benckmark)/benckmark[0]
benckmark = b.tolist()
bkt_strategy.return_analysis(benckmark)

