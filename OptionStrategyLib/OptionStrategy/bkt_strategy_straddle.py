from back_test.BktOptionStrategy import BktOptionStrategy
import pandas as pd
import QuantLib as ql
import datetime
from data_access.get_data import get_50option_mktdata, get_index_ma, get_index_mktdata,get_pciv_ratio,get_comoption_mktdata


class BktStrategyStraddle(BktOptionStrategy):

    def __init__(self, df_option, money_utilization=0.2, init_fund=100000000.0,
                 cash_reserve_pct=0.20):
        BktOptionStrategy.__init__(self, df_option, money_utilization=money_utilization,
                                       init_fund=init_fund)
        self.cash_reserve_pct = cash_reserve_pct

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

    def run_straddle(self):
        bkt_optionset = self.bkt_optionset
        bkt_account = self.bkt_account
        print(bkt_optionset.dt_list)
        inv_fund = (1 - self.cash_reserve_pct) * bkt_account.cash
        evalDate = bkt_optionset.eval_date
        """Option: Select Strategy and Open Position"""
        cd_open_position_time = 'close'
        long_short = self.util.short
        vol_status = self.util.neutrual
        index_status = self.util.neutrual
        delta_exposure = 0
        moneyness = -2
        self.portfolio = self.bkt_optionset.get_straddle(moneyness, self.get_1st_eligible_maturity(evalDate),
                                                         delta_exposure, long_short, cd_underlying_price='close')
        bkt_account.portfolio_units_adjust(self.portfolio, delta_exposure, fund=inv_fund)
        portfolio_unit = self.portfolio.unit_portfolio
        bkt_account.open_portfolio(evalDate, self.portfolio, cd_open_by_price=cd_open_position_time)
        self.flag_trade = True
        bkt_account.mkm_update_portfolio(evalDate, self.portfolio)
        print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv, bkt_account.cash)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            self.next()
            evalDate = bkt_optionset.eval_date
            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt_account.close_portfolio(evalDate, self.portfolio, cd_close_by_price='close')
                bkt_account.mkm_update_portfolio(evalDate, self.portfolio)
                print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
                break
            if evalDate not in self.index_boll.index:
                index_signal = None
            else:
                index_status,index_signal = self.boll_signal(index_status,self.index_boll.loc[evalDate])
            if evalDate not in self.volatility_boll.index:
                vol_signal = None
            else:
                vol_status,vol_signal = self.boll_direction_signal(vol_status,self.volatility_boll.loc[evalDate])
                # vol_status, vol_signal = self.percentile_signal(vol_status, self.volatility_boll.loc[evalDate],
                #                                                 upper='percentile_75', lower='percentile_25')
                # print('vol status ',vol_status,' , ',evalDate)
            if evalDate == datetime.date(2017,7,20):
                print('')
            if vol_signal != None:
                self.flag_trade = False
                if vol_signal == self.util.long:
                    long_short = self.util.long
                    moneyness = 0
                elif vol_signal == self.util.short:
                    long_short = self.util.short
                    moneyness = -2
                else:
                    long_short = self.util.short
                    moneyness = -3
                print('vol signal : ',vol_signal,' , ',evalDate)
            if index_signal != None:
                self.flag_trade = False
                if index_signal == self.util.long:
                    delta_exposure = 0.5
                elif index_signal == self.util.short:
                    delta_exposure = -0.5
                else:
                    delta_exposure = 0
                print('index signal : ',index_signal,' , ',evalDate)
            if not self.flag_trade:
                portfolio_new = self.bkt_optionset.get_straddle(moneyness, self.get_1st_eligible_maturity(evalDate),
                                                                delta_exposure, long_short, cd_underlying_price='close')
                if portfolio_new != None:
                    self.portfolio.update_portfolio(option_call=portfolio_new.option_call, option_put=portfolio_new.option_put,long_short=long_short)
                    # bkt_account.update_invest_units_c2(self.portfolio, portfolio_unit,
                    #                                    call_ratio=self.portfolio.invest_ratio_call,put_ratio=self.portfolio.invest_ratio_put)
                    bkt_account.portfolio_rebalancing_eqlfund(self.portfolio,delta_exposure,cd_open_by_price='close', fund=None)
                    bkt_account.rebalance_portfolio(evalDate, self.portfolio)
                    self.flag_trade = True
            else:
                dt = self.util.to_dt_date(self.calendar.advance(
                    self.util.to_ql_date(evalDate), ql.Period(8, ql.Days)))
                call = self.portfolio.option_call
                put = self.portfolio.option_put
                flag_update = False
                """ Update portfolio when former portfolio is maturity reaches, spot nears strike price """
                if call == None or put == None:
                    flag_update = True
                elif call.maturitydt() <= dt:
                    flag_update = True
                else:
                    spot = call.underlying_close()
                    k_call = call.adj_strike()
                    k_put = put.adj_strike()
                    if k_call - spot <= 50 or spot - k_put <= 50:
                        flag_update = True
                if flag_update:
                    portfolio_new = self.bkt_optionset.get_straddle(moneyness, self.get_1st_eligible_maturity(evalDate),
                                                                    delta_exposure, long_short, cd_underlying_price='close')
                    if portfolio_new != None:
                        self.portfolio.update_portfolio(option_call=portfolio_new.option_call,
                                                        option_put=portfolio_new.option_put, long_short=long_short)
                        # bkt_account.update_invest_units_c2(self.portfolio, portfolio_unit,
                        #                                call_ratio=self.portfolio.invest_ratio_call,put_ratio=self.portfolio.invest_ratio_put)
                        bkt_account.portfolio_rebalancing_eqlfund(self.portfolio, delta_exposure,
                                                        cd_open_by_price='close', fund=None)
                        bkt_account.rebalance_portfolio(evalDate, self.portfolio)
            if self.portfolio.option_call == None or self.portfolio.option_put==None:
                self.flag_trade = False
                print(evalDate, 'No complete collar portfolio constructed, try next day !')
            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update_portfolio(evalDate,self.portfolio)
            print(evalDate, ' , ', bkt_account.npv, bkt_account.mtm_long_positions, bkt_account.mtm_short_positions)


"""Back Test Settings"""
start_date = datetime.date(2017, 6, 1)
# start_date = datetime.date(2018, 1, 20)
end_date = datetime.date(2018, 5, 21)


"""Collect Mkt Date"""
vol_data = pd.read_excel('../data/AE1_HISTVOL.xlsx').rename(columns={'IV':'amt_close','Date':'dt_date'}).sort_values(by='dt_date',ascending=True)
index_data = pd.read_excel('../data/AE1_HISTVOL.xlsx').rename(columns={'PRICE':'amt_close','Date':'dt_date'}).sort_values(by='dt_date',ascending=True).dropna()
df_option_metrics = get_comoption_mktdata(start_date, end_date, 'm')

"""Run Backtest"""
bkt_strategy = BktStrategyStraddle(df_option_metrics)
bkt_strategy.set_min_holding_days(8)

df_vol_boll = bkt_strategy.get_bollinger_signal_calculate(vol_data,start_date,cd_long=60)
df_index_boll = bkt_strategy.get_bollinger_signal_calculate(index_data,start_date,cd_long=60)
# df_vol_pctl = bkt_strategy.get_percentile_signal(vol_data,start_date,cd_long=60)

bkt_strategy.set_volatility_boll(df_vol_boll)
bkt_strategy.set_index_boll(df_index_boll)
# bkt_strategy.set_volatility_boll(df_vol_pctl)
df_vol_boll.to_csv('../df_m_vol_boll.csv')
# df_vol_pctl.to_csv('../df_m_vol_pctl.csv')

bkt_strategy.run_straddle()
bkt_strategy.return_analysis()

bkt_strategy.bkt_account.df_account.to_csv('../save_results/bkt_df_account.csv')
bkt_strategy.bkt_account.df_trading_book.to_csv('../save_results/bkt_df_trading_book.csv')
bkt_strategy.bkt_account.df_trading_records.to_csv('../save_results/bkt_df_trading_records.csv')
bkt_strategy.bkt_account.df_ivs.to_csv('../save_results/bkt_df_ivs.csv')



