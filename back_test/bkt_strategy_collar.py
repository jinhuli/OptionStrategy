from back_test.BktOptionStrategy import BktOptionStrategy,BktOptionIndex
from back_test.BktInstrument import BktInstrument
import numpy as np
import QuantLib as ql
import datetime
from data_access.get_data import get_50option_mktdata, get_index_ma, get_index_mktdata,get_put_call_iv


class BktStrategyCollar(BktOptionStrategy):

    def __init__(self, df_option, df_index, money_utilization=0.2, init_fund=100000000.0,
                 cash_reserve_pct=0.20):
        self.validate_data(df_option, df_index)
        BktOptionStrategy.__init__(self, self.df_option, money_utilization=money_utilization,
                                       init_fund=init_fund)
        self.bkt_index = BktInstrument('daily', self.df_index)
        self.cash_reserve_pct = cash_reserve_pct
        self.moneyness_call = -2
        self.moneyness_put = -2
        self.write_ratio = 1.0
        self.buy_ratio = 1.0

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

    def set_index_ma(self, df_index_ma):
        self.index_ma = df_index_ma

    def set_volatility_ma(self,df_vol_ma):
        self.volatility_ma = df_vol_ma

    def set_index_boll(self,df):
        self.index_boll = df

    def set_volatility_boll(self,df):
        self.volatility_boll = df

    def set_atm_iv(self,df):
        self.iv_boll = df

    def next(self):
        self.bkt_optionset.next()
        self.bkt_index.next()

    def run_collar(self):
        bkt_optionset = self.bkt_optionset
        bkt_index = self.bkt_index
        bkt_account = self.bkt_account
        print(bkt_optionset.dt_list)
        print(bkt_index.dt_list)
        inv_fund = (1-self.cash_reserve_pct)*bkt_account.cash
        evalDate = bkt_optionset.eval_date
        """Option: Select Strategy and Open Position"""
        cd_open_position_time = 'close'
        moneyness_call = self.moneyness_call
        moneyness_put = self.moneyness_put
        write_ratio = self.write_ratio
        buy_ratio = self.buy_ratio
        self.portfolio = self.bkt_optionset.get_collar(
            self.get_1st_eligible_maturity(evalDate),
            self.get_2nd_eligible_maturity(evalDate),
            bkt_index, moneyness_call=moneyness_call,moneyness_put=moneyness_put)

        unit_underlying = np.floor(inv_fund / self.portfolio.underlying.mktprice_close())
        bkt_account.portfolio_rebalancing_eqlunit(self.portfolio, unit_underlying,call_ratio=write_ratio,put_ratio=buy_ratio)
        bkt_account.open_portfolio(evalDate, self.portfolio, cd_open_by_price=cd_open_position_time)
        self.flag_trade = True
        bkt_account.mkm_update_portfolio(evalDate, self.portfolio)
        print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.cash)

        flag_protect = False
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            self.next()
            evalDate = bkt_optionset.eval_date
            print(evalDate)
            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt_account.close_portfolio(evalDate, self.portfolio, cd_close_by_price='close')
                bkt_account.mkm_update_portfolio(evalDate, self.portfolio)
                print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
                break

            """ 根据动量指标与波动率指标调整write ratio """
            # if evalDate not in self.volatility_boll.index:
            #     vol_signal = None
            # else:
            #     vol_status,vol_signal = self.vol_bolliger_signal(vol_status,self.volatility_boll.loc[evalDate])
            # if evalDate not in self.index_boll.index:
            #     index_signal = None
            # else:
            #     index_status,index_signal = self.boll_signal(index_status,self.index_boll.loc[evalDate])
            # if evalDate not in self.iv_boll.index:
            #     iv_signal = None
            # else:
            #     iv_status,iv_signal = self.boll_signal_2(flag_iv,self.iv_boll.loc[evalDate])

            # iv_status,iv_signal = self.percentile_signal(flag_iv,self.iv_boll.loc[evalDate])
            " research paper method "
            # if vol_signal == 2: # extreme condition long
            #     self.flag_trade = False
            #     write_ratio = 0.25
            #     print('vol_signal ',evalDate)
            # elif vol_signal == -2:# extreme condition short
            #     self.flag_trade = False
            #     write_ratio = 1.25
                # print('vol signal ', evalDate, ' ', vol_signal, ' ', vol_status, ' ',
                #           self.volatility_boll.loc[evalDate]['amt_close'], ' ',
                #           self.volatility_boll.loc[evalDate]['upper_sigma1'], ' ',
                #           self.volatility_boll.loc[evalDate]['lower_sigma1'],
                #           ' ', write_ratio, ' ', moneyness_call)
            " Change moneyness by monmentum "
            # if index_signal != None:
            #     self.flag_trade = False
            #     flag_protect = False
            #     # write_ratio = self.write_ratio
            #     # buy_ratio = self.buy_ratio
            #     # write_ratio = buy_ratio = 1.0
            #     # if index_signal == self.util.long:
            #     #     moneyness_call = -3
            #     #     moneyness_put = -3
            #     if index_signal == self.util.short:
            #         moneyness_call = -1
            #         moneyness_put = -1
            #         flag_protect = True
            #     else:
            #         moneyness_call = -2
            #         moneyness_put = -2
            #     print('index_signal ',index_signal,' ', evalDate)

            " change buy/write ratio by volatility signal "
            # if iv_signal != None:
            #     self.flag_trade = False
            #     flag_iv = iv_signal
            #     write_ratio = self.write_ratio
            #     buy_ratio = self.buy_ratio
            #     # buy_ratio = 0.5
            #     if iv_signal == self.util.long:
            #         buy_ratio = 2.0
            #         # moneyness_put = 0
            #         # buy_ratio = 2.0
            #         # write_ratio = 0.25
            #     elif iv_signal == self.util.short:
            #         write_ratio = 0.0
            #         buy_ratio = 0.0
            #     print('iv signal ',iv_signal,' ', evalDate,' ',write_ratio,' ',buy_ratio)

            if not self.flag_trade:
                portfolio_new = self.bkt_optionset.get_collar(
                    self.get_1st_eligible_maturity(evalDate),
                    self.get_2nd_eligible_maturity(evalDate),
                    bkt_index,
                    moneyness_call=moneyness_call,moneyness_put=moneyness_put,
                    flag_protect=flag_protect)
                self.portfolio.update_portfolio(buy_put=portfolio_new.buy_put, write_call=portfolio_new.write_call)
                bkt_account.portfolio_rebalancing_eqlunit(self.portfolio, unit_underlying,
                                                          call_ratio=write_ratio,put_ratio=buy_ratio)
                bkt_account.rebalance_portfolio(evalDate, self.portfolio)
                self.flag_trade = True
            else:
                dt = self.util.to_dt_date(self.calendar.advance(
                    self.util.to_ql_date(evalDate), ql.Period(8, ql.Days)))
                call = self.portfolio.write_call
                put = self.portfolio.buy_put
                flag_update = False
                """Update portfolio when formar portfolio is  maturity reaches, spot nears strike price"""
                if call == None or put == None:
                    flag_update = True
                elif call.maturitydt() <= dt:
                    flag_update = True
                else:
                    spot = call.underlying_close()
                    k_call = call.strike()
                    k_put = put.strike()
                    if spot >= 3.0:
                        if k_call - spot <= 0.1 or spot - k_put <= 0:
                        # if k_call - spot <= 0.1:
                            flag_update = True
                            # print('1 ',evalDate,' kput=',k_put,'kcall=',k_call,' s=',spot)
                    else:
                        if k_call - spot <= 0.05 or spot - k_put <= 0:
                        # if k_call - spot <= 0.05:
                            flag_update = True
                            # print('2 ',evalDate,' kput=',k_put,'kcall=',k_call,' s=',spot)

                if flag_update:
                    portfolio_new = self.bkt_optionset.get_collar(
                        self.get_1st_eligible_maturity(evalDate),
                        self.get_2nd_eligible_maturity(evalDate),
                        bkt_index,
                        moneyness_call=moneyness_call, moneyness_put=moneyness_put,
                        flag_protect=flag_protect)
                    self.portfolio.update_portfolio(buy_put=portfolio_new.buy_put,
                                                    write_call=portfolio_new.write_call)
                    bkt_account.portfolio_rebalancing_eqlunit(self.portfolio, unit_underlying,
                                                              call_ratio=write_ratio,put_ratio=buy_ratio)
                    bkt_account.rebalance_portfolio(evalDate, self.portfolio)
            if self.portfolio.write_call == None or self.portfolio.buy_put==None:
                self.flag_trade = False
                print(evalDate, 'No complete collar portfolio constructed, try next day !')
            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update_portfolio(evalDate,self.portfolio)
            # print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.mtm_long_positions)


    def run_active_collar(self):
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
        moneyness_call = self.moneyness_call
        moneyness_put = self.moneyness_put
        write_ratio = self.write_ratio
        buy_ratio = self.buy_ratio
        self.portfolio = self.bkt_optionset.get_collar(
            self.get_1st_eligible_maturity(evalDate),
            self.get_2nd_eligible_maturity(evalDate),
            bkt_index, moneyness_call=moneyness_call,moneyness_put=moneyness_put)

        unit_underlying = np.floor(inv_fund / self.portfolio.underlying.mktprice_close())
        bkt_account.update_invest_units_c2(self.portfolio, unit_underlying, call_ratio=write_ratio,put_ratio=buy_ratio)
        bkt_account.open_portfolio(evalDate, self.portfolio, cd_open_by_price=cd_open_position_time)
        self.flag_trade = True
        bkt_account.mkm_update_portfolio(evalDate, self.portfolio)
        print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.cash)

        vol_status = self.util.neutrual
        index_status = self.util.neutrual
        flag_iv = self.util.neutrual
        flag_protect = False
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            self.next()
            evalDate = bkt_optionset.eval_date
            print(evalDate)
            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt_account.close_portfolio(evalDate, self.portfolio, cd_close_by_price='close')
                bkt_account.mkm_update_portfolio(evalDate, self.portfolio)
                print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
                break

            """ 根据动量指标与波动率指标调整write ratio """
            # if evalDate not in self.volatility_boll.index:
            #     vol_signal = None
            # else:
            #     vol_status,vol_signal = self.vol_bolliger_signal(vol_status,self.volatility_boll.loc[evalDate])
            if evalDate not in self.index_boll.index:
                index_signal = None
            else:
                index_status,index_signal = self.boll_signal(index_status,self.index_boll.loc[evalDate])
            if evalDate not in self.iv_boll.index:
                iv_signal = None
            else:
                iv_status,iv_signal = self.boll_signal_2(flag_iv,self.iv_boll.loc[evalDate])
            # iv_status,iv_signal = self.percentile_signal(flag_iv,self.iv_boll.loc[evalDate])
            " research paper method "
            # if vol_signal == 2: # extreme condition long
            #     self.flag_trade = False
            #     write_ratio = 0.25
            #     print('vol_signal ',evalDate)
            # elif vol_signal == -2:# extreme condition short
            #     self.flag_trade = False
            #     write_ratio = 1.25
                # print('vol signal ', evalDate, ' ', vol_signal, ' ', vol_status, ' ',
                #           self.volatility_boll.loc[evalDate]['amt_close'], ' ',
                #           self.volatility_boll.loc[evalDate]['upper_sigma1'], ' ',
                #           self.volatility_boll.loc[evalDate]['lower_sigma1'],
                #           ' ', write_ratio, ' ', moneyness_call)
            " Change moneyness by monmentum "
            if index_signal != None:
                self.flag_trade = False
                flag_protect = False
                # write_ratio = self.write_ratio
                # buy_ratio = self.buy_ratio
                # write_ratio = buy_ratio = 1.0
                # if index_signal == self.util.long:
                #     moneyness_call = -3
                #     moneyness_put = -3
                if index_signal == self.util.short:
                    moneyness_call = -1
                    moneyness_put = -1
                    flag_protect = True
                else:
                    moneyness_call = -2
                    moneyness_put = -2
                print('index_signal ',index_signal,' ', evalDate)

            " change buy/write ratio by volatility signal "
            if iv_signal != None:
                self.flag_trade = False
                flag_iv = iv_signal
                write_ratio = self.write_ratio
                buy_ratio = self.buy_ratio
                # buy_ratio = 0.5
                if iv_signal == self.util.long:
                    buy_ratio = 2.0
                    # moneyness_put = 0
                    # buy_ratio = 2.0
                    # write_ratio = 0.25
                elif iv_signal == self.util.short:
                    write_ratio = 0.0
                    buy_ratio = 0.0
                print('iv signal ',iv_signal,' ', evalDate,' ',write_ratio,' ',buy_ratio)

            if not self.flag_trade:
                portfolio_new = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate),
                                                              self.get_2nd_eligible_maturity(evalDate),
                                                              bkt_index,
                                                              moneyness_call=moneyness_call,moneyness_put=moneyness_put,
                                                              flag_protect=flag_protect)
                self.portfolio.update_portfolio(buy_put=portfolio_new.buy_put, write_call=portfolio_new.write_call)
                bkt_account.update_invest_units_c2(self.portfolio, unit_underlying,call_ratio=write_ratio,put_ratio=buy_ratio)
                bkt_account.rebalance_portfolio(evalDate, self.portfolio)
                self.flag_trade = True
            else:
                dt = self.util.to_dt_date(self.calendar.advance(
                    self.util.to_ql_date(evalDate), ql.Period(8, ql.Days)))
                call = self.portfolio.write_call
                put = self.portfolio.buy_put
                flag_update = False
                """Update portfolio when formar portfolio is  maturity reaches, spot nears strike price"""
                if call == None or put == None:
                    flag_update = True
                elif call.maturitydt() <= dt:
                    flag_update = True
                else:
                    spot = self.portfolio.underlying.mktprice_close()
                    k_call = call.adj_strike()
                    k_put = put.adj_strike()
                    if spot >= 3.0:
                        if k_call - spot <= 0.1 or spot - k_put <= 0:
                        # if k_call - spot <= 0.1:
                            flag_update = True
                            # print('1 ',evalDate,' kput=',k_put,'kcall=',k_call,' s=',spot)
                    else:
                        if k_call - spot <= 0.05 or spot - k_put <= 0:
                        # if k_call - spot <= 0.05:
                            flag_update = True
                            # print('2 ',evalDate,' kput=',k_put,'kcall=',k_call,' s=',spot)

                if flag_update:
                    portfolio_new = self.bkt_optionset.get_collar(self.get_1st_eligible_maturity(evalDate),
                                                                  self.get_2nd_eligible_maturity(evalDate),
                                                                  bkt_index,
                                                                  moneyness_call=moneyness_call, moneyness_put=moneyness_put,
                                                                  flag_protect=flag_protect)
                    self.portfolio.update_portfolio(buy_put=portfolio_new.buy_put, write_call=portfolio_new.write_call)
                    bkt_account.update_invest_units_c2(self.portfolio, write_ratio, unit_underlying,buy_ratio=buy_ratio)
                    bkt_account.rebalance_portfolio(evalDate, self.portfolio)
            if self.portfolio.write_call == None or self.portfolio.buy_put==None:
                self.flag_trade = False
                print(evalDate, 'No complete collar portfolio constructed, try next day !')
            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update_portfolio(evalDate,self.portfolio)
            # print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.mtm_long_positions)



"""Back Test Settings"""
# start_date = datetime.date(2015, 3, 1)
start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2018, 1, 21)


"""Collect Mkt Date"""

df_option_metrics = get_50option_mktdata(start_date, end_date)
df_index_metrics = get_index_mktdata(start_date,end_date,'index_50etf')
"""Run Backtest"""


bkt_strategy = BktStrategyCollar(df_option_metrics, df_index_metrics)
bkt_strategy.set_min_holding_days(8)


df_index_ma = bkt_strategy.get_moving_average_signal(get_index_ma(start_date,end_date,'index_50etf'))
df_vix_boll = bkt_strategy.get_bollinger_signal(get_index_ma(start_date,end_date,'index_cvix'),cd_long='60')
df_index_boll = bkt_strategy.get_bollinger_signal(get_index_ma(start_date,end_date,'index_50etf'),cd_long='60')
df_iv = bkt_strategy.get_bollinger_signal_calculate(get_put_call_iv(end_date),start_date)
# df_iv = bkt_strategy.get_percentile_signal(get_put_call_iv(end_date),start_date)

bkt_strategy.set_index_ma(df_index_ma)
bkt_strategy.set_index_boll(df_index_boll)
bkt_strategy.set_volatility_boll(df_vix_boll)
bkt_strategy.set_atm_iv(df_iv)
# df_vix_boll.to_csv('../df_vix_boll.csv')
# df_index_boll.to_csv('../df_index_boll.csv')
# df_iv.to_csv('../df_iv.csv')

bkt_strategy.run_collar()

bkt_strategy.bkt_account.df_account.to_csv('../save_results/bkt_df_account.csv')
bkt_strategy.bkt_account.df_trading_book.to_csv('../save_results/bkt_df_trading_book.csv')
bkt_strategy.bkt_account.df_trading_records.to_csv('../save_results/bkt_df_trading_records.csv')
bkt_strategy.bkt_account.df_ivs.to_csv('../save_results/bkt_df_ivs.csv')
#
benckmark = df_index_metrics[bkt_strategy.util.col_close].tolist()
b = np.array(benckmark)/benckmark[0]
benckmark = b.tolist()
bkt_strategy.return_analysis(benckmark)

