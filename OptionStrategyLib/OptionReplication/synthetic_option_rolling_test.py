import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from OptionStrategyLib.OptionReplication.synthetic_option import SytheticOption
from PricingLibrary.Options import EuropeanOption
from back_test.model.base_account import BaseAccount
from back_test.model.base_instrument import BaseInstrument
from back_test.model.constant import Util, OptionType, LongShort, ExecuteType,Calendar,DeltaBound,BuyWrite
from back_test.model.trade import Trade
from data_access.get_data import get_dzqh_cf_daily, get_dzqh_cf_c1_daily, \
    get_dzqh_cf_c1_minute, get_index_mktdata
from OptionStrategyLib.VolatilityModel.historical_volatility import historical_volatility_model as Histvol
from Utilities import Analysis


#####################################################################

class SyntheticOptionHedgedPortfolio():
    def __init__(self,start_date,end_date):
        self.n1 = 30 # 30min调仓一次
        self.fund = Util.BILLION
        self.invest_underlying_ratio = 0.7
        # self.hedge_notional = Util.BILLION / 2
        self.slippage = 0
        self.start_date = start_date
        # self.end_date = end_date = datetime.date(2018, 3, 1)
        self.end_date = end_date
        hist_date = start_date - datetime.timedelta(days=40)
        df_future_c1 = get_dzqh_cf_c1_minute(start_date, end_date, 'if')
        df_future_c1_daily = get_dzqh_cf_c1_daily(hist_date, end_date, 'if')
        df_futures_all_daily = get_dzqh_cf_daily(start_date, end_date, 'if')  # daily data of all future contracts
        df_index = get_index_mktdata(start_date, end_date, 'index_300sh')  # daily data of underlying index
        df_index = df_index[df_index[Util.DT_DATE].isin(Util.DZQH_CF_DATA_MISSING_DATES) == False].reset_index(
            drop=True)
        df_index.to_csv('df_index.csv')
        self.trade_dates = sorted(df_future_c1_daily[Util.DT_DATE].unique())
        self.df_vol_1m = Histvol.hist_vol(df_future_c1_daily)
        # df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
        # self.df_garman_klass = Histvol.garman_klass(df_future_c1_daily)
        # df_hist_vol = self.df_vol_1m.join(self.df_garman_klass, how='left')
        # df_hist_vol.to_csv('df_hist_vol.csv')
        # df_future_c1_daily = df_future_c1_daily[df_future_c1_daily[Util.DT_DATE] >= start_date].reset_index(drop=True)

        self.underlying = BaseInstrument(df_data=df_index)
        self.underlying.init()
        self.synthetic_option = SytheticOption(df_c1_minute=df_future_c1,
                                               # df_c1_daily=df_future_c1_daily,
                                               df_futures_all_daily=df_futures_all_daily,
                                               df_index_daily=df_index)
        self.synthetic_option.init()

        self.account = BaseAccount(self.fund, leverage=20.0, rf=0.0)
        self.trading_desk = Trade()
        # self.list_hedge_info = []
        self.init_spot = self.synthetic_option.underlying_index_state_daily[Util.AMT_CLOSE]
        self.df_analysis  = pd.DataFrame()

    def next(self):
        if self.synthetic_option.is_last_minute():
            self.underlying.next()
        self.synthetic_option.next()


    def init_portfolio(self, dt_maturity, fund):

        """ Init position """

        # strike = self.underlying.mktprice_close()
        strike = self.synthetic_option.mktprice_close()
        # dt_maturity = self.synthetic_option.eval_date + datetime.timedelta(days=30)
        self.Option = EuropeanOption(strike, dt_maturity, OptionType.PUT)
        # self.Option = EuropeanOption(strike, dt_maturity, OptionType.CALL)

        """ 用第一天的日收盘价开仓标的现货多头头寸 """
        underlying_unit = np.floor(fund*self.invest_underlying_ratio / self.underlying.mktprice_close())
        order_underlying = self.account.create_trade_order(self.underlying, LongShort.LONG, underlying_unit)
        execution_record = self.underlying.execute_order(order_underlying, slippage=0,
                                                         execute_type=ExecuteType.EXECUTE_ALL_UNITS)
        self.account.add_record(execution_record, self.underlying)
        self.synthetic_option.amt_option = underlying_unit

        """ 用第一天的成交量加权均价初次开仓复制期权头寸 """
        vol = self.get_vol()
        # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]
        self.delta = self.synthetic_option.get_black_delta(self.Option, vol)
        synthetic_unit = self.synthetic_option.get_synthetic_unit(self.delta,buywrite=BuyWrite.BUY)
        if synthetic_unit > 0:
            long_short = LongShort.LONG
        else:
            long_short = LongShort.SHORT
        order = self.account.create_trade_order(self.synthetic_option,
                                                long_short,
                                                synthetic_unit)
        execution_record = self.synthetic_option.execute_order_by_VWAP(order, slippage=self.slippage,
                                                                       execute_type=ExecuteType.EXECUTE_ALL_UNITS)
        self.account.add_record(execution_record, self.synthetic_option)
        # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
        #                              Util.AMT_DELTA: self.delta,
        #                              Util.AMT_HEDHE_UNIT: synthetic_unit,
        #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
        #                              Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
        #                              })
        self.account.daily_accounting(self.synthetic_option.eval_date)
        self.add_additional_to_account()
        self.disp()
        self.next()

    def shift_synthetic_open_by_VWAP(self, hold_unit=0):
        vol = self.get_vol()
        # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]
        self.delta = self.synthetic_option.get_black_delta(self.Option, vol)
        # synthetic_unit = self.synthetic_option.get_synthetic_unit(self.delta)
        # hold unit 指当前持仓，带有正负号
        # d_unit = synthetic_unit-hold_unit
        d_unit = self.synthetic_option.get_synthetic_option_rebalancing_unit(self.delta,
                                                                                     self.Option,
                                                                                     vol,
                                                                                     self.synthetic_option.mktprice_close(),
                                                                                     DeltaBound.WHALLEY_WILLMOTT,
                                                                                     BuyWrite.BUY)
        if d_unit > 0:
            long_short = LongShort.LONG
        else:
            long_short = LongShort.SHORT
        order = self.account.create_trade_order(self.synthetic_option,
                                                long_short,
                                                d_unit)
        execution_record = self.synthetic_option.execute_order_by_VWAP(order, slippage=self.slippage,
                                                                       execute_type=ExecuteType.EXECUTE_ALL_UNITS)
        self.account.add_record(execution_record, self.synthetic_option)

        # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
        #                              Util.AMT_DELTA: self.delta,
        #                              Util.AMT_HEDHE_UNIT: d_unit,
        #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
        #                              Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
        #                              })

        self.account.daily_accounting(self.synthetic_option.eval_date)
        self.add_additional_to_account()
        self.disp()
        self.next()

    #
    # """ 到期平仓 """
    # def hedge(self, dt_end=None):
    #
    #     id_future = self.synthetic_option.current_state[Util.ID_INSTRUMENT]
    #     if dt_end is None:
    #         dt_end = self.Option.dt_maturity
    #     dt_time_end = datetime.datetime(dt_end.year, dt_end.month,
    #                                     dt_end.day, 15, 0, 0)
    #
    #     while self.synthetic_option.has_next() and self.synthetic_option.eval_datetime < dt_time_end:
    #
    #         if id_future != self.synthetic_option.current_state[Util.ID_INSTRUMENT]:
    #             long_short = self.account.trade_book.loc[id_future, Util.TRADE_LONG_SHORT]
    #             hold_unit =  - self.account.trade_book.loc[id_future, Util.TRADE_UNIT]
    #             # synthetic_unit_previous = self.synthetic_option.get_synthetic_unit(delta)
    #             # spot = self.synthetic_option.current_daily_state[Util.AMT_CLOSE]
    #             spot = self.synthetic_option.mktprice_close()
    #             vol = self.get_vol()
    #             # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]
    #             self.delta = self.synthetic_option.get_black_delta(self.Option, vol, spot)
    #             synthetic_unit = self.synthetic_option.get_synthetic_unit(self.delta) # 按照移仓换月日的收盘价计算Delta
    #             # print('Shift Contract: hold_unit, synthetic_unit_previous, synthetic_unit', hold_unit, synthetic_unit_previous, synthetic_unit)
    #             id_c2 = self.synthetic_option.current_state[Util.ID_INSTRUMENT]
    #             open_unit = synthetic_unit
    #             close_execution_record, open_execution_record \
    #                 = self.synthetic_option.shift_contract_by_VWAP(id_c1=id_future,
    #                                                                id_c2=id_c2,
    #                                                                hold_unit=hold_unit,
    #                                                                open_unit=open_unit,
    #                                                                long_short=long_short,
    #                                                                slippage=self.slippage,
    #                                                                execute_type=ExecuteType.EXECUTE_ALL_UNITS
    #                                                                )
    #
    #             self.account.add_record(close_execution_record, self.synthetic_option)
    #             self.synthetic_option._id_instrument = id_c2
    #             self.account.add_record(open_execution_record, self.synthetic_option)
    #             """ 更新当前持仓头寸 """
    #             self.synthetic_option.synthetic_unit = open_unit
    #             """ USE SAME UNIT TO SHIFT CONTRACT AND USE CLOSE PRICE TO REBALANCING DELTA CHANGE. """
    #             print(' Relancing after shift contract, ', self.synthetic_option.eval_date)
    #             # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
    #             #                              Util.AMT_DELTA: self.delta,
    #             #                              Util.AMT_HEDHE_UNIT: -hold_unit,
    #             #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
    #             #                              Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
    #             #                              })
    #             # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
    #             #                              Util.AMT_DELTA: self.delta,
    #             #                              Util.AMT_HEDHE_UNIT: open_unit,
    #             #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
    #             #                              Util.ID_INSTRUMENT: id_future
    #             #                              })
    #             id_future = id_c2
    #
    #         if self.synthetic_option.eval_date == self.synthetic_option.get_next_state_date():
    #             if self.synthetic_option.eval_datetime.minute != 0:
    #                 self.next()
    #                 continue
    #
    #         # print(self.synthetic_option.eval_datetime)
    #         self.rebalancing()
    #
    #         if self.synthetic_option.is_last_minute():
    #             self.account.daily_accounting(self.synthetic_option.eval_date)  # 该日的收盘结算
    #             self.add_additional_to_account()
    #             self.disp()
    #         self.next()
    #     self.close_out()

    """ 到期不平仓 """
    def hedge(self, dt_end=None):

        id_future = self.synthetic_option.current_state[Util.ID_INSTRUMENT]
        if dt_end is None:
            dt_end = self.Option.dt_maturity
        dt_time_end = datetime.datetime(dt_end.year, dt_end.month,
                                        dt_end.day, 15, 00, 0)

        while self.synthetic_option.has_next() and self.synthetic_option.eval_datetime <= dt_time_end:

            if id_future != self.synthetic_option.current_state[Util.ID_INSTRUMENT]:
                long_short = self.account.trade_book.loc[id_future, Util.TRADE_LONG_SHORT]
                hold_unit =  - self.account.trade_book.loc[id_future, Util.TRADE_UNIT]
                # synthetic_unit_previous = self.synthetic_option.get_synthetic_unit(delta)
                # spot = self.synthetic_option.current_daily_state[Util.AMT_CLOSE]
                spot = self.synthetic_option.mktprice_close()
                vol = self.get_vol()
                # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]
                self.delta = self.synthetic_option.get_black_delta(self.Option, vol, spot)
                synthetic_unit = self.synthetic_option.get_synthetic_unit(self.delta, BuyWrite.BUY) # 按照移仓换月日的收盘价计算Delta
                # print('Shift Contract: hold_unit, synthetic_unit_previous, synthetic_unit', hold_unit, synthetic_unit_previous, synthetic_unit)
                id_c2 = self.synthetic_option.current_state[Util.ID_INSTRUMENT]
                open_unit = synthetic_unit
                close_execution_record, open_execution_record \
                    = self.synthetic_option.shift_contract_by_VWAP(id_c1=id_future,
                                                                   id_c2=id_c2,
                                                                   hold_unit=hold_unit,
                                                                   open_unit=open_unit,
                                                                   long_short=long_short,
                                                                   slippage=self.slippage,
                                                                   execute_type=ExecuteType.EXECUTE_ALL_UNITS
                                                                   )

                self.account.add_record(close_execution_record, self.synthetic_option)
                self.synthetic_option._id_instrument = id_c2
                self.account.add_record(open_execution_record, self.synthetic_option)
                """ 更新当前持仓头寸 """
                self.synthetic_option.synthetic_unit = open_unit
                """ USE SAME UNIT TO SHIFT CONTRACT AND USE CLOSE PRICE TO REBALANCING DELTA CHANGE. """
                print(' Relancing after shift contract, ', self.synthetic_option.eval_date)
                # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                #                              Util.AMT_DELTA: self.delta,
                #                              Util.AMT_HEDHE_UNIT: -hold_unit,
                #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                #                              Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
                #                              })
                # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                #                              Util.AMT_DELTA: self.delta,
                #                              Util.AMT_HEDHE_UNIT: open_unit,
                #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                #                              Util.ID_INSTRUMENT: id_future
                #                              })
                id_future = id_c2

            if self.synthetic_option.eval_date == self.synthetic_option.get_next_state_date():
                if self.synthetic_option.eval_datetime.minute != 0:
                    self.next()
                    continue

            # print(self.synthetic_option.eval_datetime)
            self.rebalancing()

            if self.synthetic_option.is_last_minute():
                self.account.daily_accounting(self.synthetic_option.eval_date)  # 该日的收盘结算
                self.add_additional_to_account()
                self.disp()
            if self.synthetic_option.eval_date == dt_end and self.synthetic_option.is_last_minute():
                break
            self.next()

    def rebalancing(self):
        vol = self.get_vol()
        # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]
        self.delta = self.synthetic_option.get_black_delta(self.Option, vol)
        rebalance_unit = self.synthetic_option.get_rebalancing_unit(self.delta,
                                                                                     self.Option,
                                                                                     vol,
                                                                                     self.synthetic_option.mktprice_close(),
                                                                                     DeltaBound.NONE,
                                                                                     BuyWrite.BUY)
        # unit_tmp = -self.account.trade_book.loc[id_future, Util.TRADE_UNIT] + rebalance_unit
        # synthetic_unit_tmp = self.synthetic_option.get_synthetic_unit(self.delta)

        if rebalance_unit > 0:
            order = self.account.create_trade_order(self.synthetic_option,
                                                    LongShort.LONG,
                                                    rebalance_unit)
        elif rebalance_unit < 0:
            order = self.account.create_trade_order(self.synthetic_option,
                                                    LongShort.SHORT,
                                                    rebalance_unit)
        else:
            order = None

        execution_record = self.synthetic_option.execute_order(order, slippage=self.slippage,
                                                               execute_type=ExecuteType.EXECUTE_ALL_UNITS)
        self.account.add_record(execution_record, self.synthetic_option)
        # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
        #                              Util.AMT_DELTA: self.delta,
        #                              Util.AMT_HEDHE_UNIT: rebalance_unit,
        #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
        #                              Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
        #                              })

    def close_out(self):
        close_out_orders = self.account.creat_close_out_order()

        for order in close_out_orders:
            execution_record = self.account.dict_holding[order.id_instrument].execute_order(order, slippage=0,
                                                                                            execute_type=ExecuteType.EXECUTE_ALL_UNITS)
            self.account.add_record(execution_record, self.account.dict_holding[order.id_instrument])
            # self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
            #                              Util.AMT_DELTA: None,
            #                              Util.AMT_HEDHE_UNIT: order.trade_unit,
            #                              Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
            #                              Util.ID_INSTRUMENT: order.id_instrument
            #                              })
        self.account.daily_accounting(self.synthetic_option.eval_date)
        self.add_additional_to_account()

        self.disp()
        """ Final NPV check """
        self.df_records = pd.DataFrame(self.account.list_records)
        total_pnl = self.df_records[Util.TRADE_REALIZED_PNL].sum()
        final_npv = (self.fund + total_pnl) / self.fund
        print('calculate final npv from adding up realized pnl ; ', final_npv)


    def get_vol(self):

        date = self.synthetic_option.eval_date
        if date in self.df_vol_1m.index:
            vol = self.df_vol_1m.loc[date, Util.AMT_HISTVOL]
            # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]

        else:
            dt1 = Util.largest_element_less_than(port.trade_dates, date)
            # vol = self.df_garman_klass.loc[dt1, Util.AMT_GARMAN_KLASS]
            vol = self.df_vol_1m.loc[dt1, Util.AMT_HISTVOL]
        return vol

    def add_additional_to_account(self):
        self.account.account.loc[
            self.synthetic_option.eval_date, 'underlying_npv'] = self.invest_underlying_ratio * self.underlying.mktprice_close() / self.init_spot + 1-self.invest_underlying_ratio
        self.account.account.loc[
            self.synthetic_option.eval_date, 'underlying_price'] = self.underlying.mktprice_close()
        self.account.account.loc[self.synthetic_option.eval_date, 'if_c1'] = self.synthetic_option.mktprice_close()
        self.account.account.loc[self.synthetic_option.eval_date,'hedge_position'] \
            =  - self.account.trade_book[self.account.trade_book[Util.TRADE_LONG_SHORT]==LongShort.SHORT][Util.TRADE_UNIT].sum()
        self.account.account.loc[self.synthetic_option.eval_date, 'hedge_ratio'] = \
            self.account.account.loc[self.synthetic_option.eval_date, Util.PORTFOLIO_SHORT_POSITION_SCALE]/\
            self.account.account.loc[self.synthetic_option.eval_date, Util.PORTFOLIO_LONG_POSITION_SCALE]
        self.account.account.loc[self.synthetic_option.eval_date, 'pct_margin_unrealized_pnl'] = \
            self.account.account.loc[self.synthetic_option.eval_date, Util.MARGIN_UNREALIZED_PNL]/self.account.init_fund
        self.account.account.loc[self.synthetic_option.eval_date, 'pct_nonmargin_unrealized_pnl'] = \
            self.account.account.loc[ self.synthetic_option.eval_date, Util.NONMARGIN_UNREALIZED_PNL] / self.account.init_fund
        self.account.account.loc[self.synthetic_option.eval_date, 'pct_realized_pnl'] = \
            self.account.account.loc[self.synthetic_option.eval_date, Util.TRADE_REALIZED_PNL] / self.account.init_fund

    def save_results(self):

        self.df_records.to_csv('trade_records.csv')
        self.account.account.to_csv('account.csv')
        # self.df_hedge_info = pd.DataFrame(self.list_hedge_info)
        # self.df_hedge_info.to_csv('hedge_info.csv')
        self.df_analysis.to_csv('df_analysis.csv')
        self.account.trade_book_daily.to_csv('trade_book_daily.csv')

    def disp(self):
        if self.synthetic_option.eval_date != self.underlying.eval_date:
            print('Date miss matched!')
        try:
            average_cost = int(self.account.trade_book[self.account.trade_book[Util.TRADE_LONG_SHORT]==LongShort.SHORT][Util.AVERAGE_POSITION_COST].values[0])
        except:
            average_cost=0
            pass
        print(self.synthetic_option.eval_datetime,
              self.account.account.loc[self.synthetic_option.eval_date, Util.PORTFOLIO_NPV],
              self.underlying.mktprice_close() / self.init_spot,
              # self.account.account.loc[self.synthetic_option.eval_date, 'hedge_position'],
              # self.synthetic_option.synthetic_unit,
              int(self.Option.strike),
              int(self.underlying.mktprice_close()),
              int(self.synthetic_option.mktprice_close()),
              average_cost,
              self.delta,
              # self.account.cash,
              round(100*self.account.account.loc[self.synthetic_option.eval_date, 'pct_margin_unrealized_pnl'],1),'%',
              round(100*self.account.account.loc[self.synthetic_option.eval_date, 'pct_nonmargin_unrealized_pnl'],1),'%',
              round(100*self.account.account.loc[self.synthetic_option.eval_date, 'pct_realized_pnl'],1),'%',
              self.underlying.eval_date,
              )


    def reset_option(self,maturity,strike=None):
        print('maturity date : ', maturity)
        if strike is None:
            strike = self.underlying.mktprice_close()
        self.Option = EuropeanOption(strike,maturity,OptionType.PUT)

    def analysis(self, dt_start, dt_end):
        """ Replicate Period Result Analysis """
        self.df_records = pd.DataFrame(self.account.list_records)
        # analysis = Analysis.get_netvalue_analysis(port.account.account[Util.PORTFOLIO_NPV])
        analysis = pd.Series()
        df_hedge_records = self.df_records[(self.df_records[Util.ID_INSTRUMENT] != 'index_300sh')&(self.df_records[Util.DT_TRADE]>=dt_start)&(self.df_records[Util.DT_TRADE]<=dt_end)]
        init_stock_value = self.account.account.loc[dt_start, Util.PORTFOLIO_TRADES_VALUE]
        init_stock_price = self.underlying.df_data[self.underlying.df_data[Util.DT_DATE]==dt_start][Util.AMT_CLOSE].values[0]
        terminal_stock_price = self.underlying.df_data[self.underlying.df_data[Util.DT_DATE]==dt_end][Util.AMT_CLOSE].values[0]
        terminal_stock_value = terminal_stock_price*self.synthetic_option.amt_option
        # init_portfolio_value = self.account.account.loc[dt_start, Util.PORTFOLIO_VALUE]
        replicate_pnl = df_hedge_records[Util.TRADE_REALIZED_PNL].sum()
        option_payoff = self.synthetic_option.amt_option*max(init_stock_price - self.underlying.mktprice_close(), 0)
        replicate_cost = replicate_pnl - option_payoff
        replicate_cost_future = replicate_pnl - self.synthetic_option.amt_option*max(self.Option.strike - self.underlying.mktprice_close(), 0)
        pct_replicate_cost = replicate_cost/init_stock_value
        pct_replicate_pnl = replicate_pnl/init_stock_value
        transaction_cost = df_hedge_records[Util.TRANSACTION_COST].sum()
        pct_underlying_pnl = (terminal_stock_price - init_stock_price)/init_stock_price
        portfolio_pnl = 1- self.account.account.loc[dt_end,Util.PORTFOLIO_NPV]
        analysis['dt_start'] = dt_start
        analysis['dt_end'] = dt_end
        analysis['init_stock_value'] = init_stock_value
        # analysis['init_portfolio_value'] = init_portfolio_value
        analysis['replicate_pnl'] = replicate_pnl
        analysis['option_payoff'] = option_payoff
        analysis['replicate_cost_spot'] = replicate_cost
        analysis['replicate_cost_future'] = replicate_cost_future
        analysis['pct_replicate_cost'] = pct_replicate_cost
        analysis['pct_replicate_pnl'] = pct_replicate_pnl
        analysis['pct_underlying_pnl'] = pct_underlying_pnl
        analysis['transaction_cost'] = transaction_cost
        analysis['dt_maturity'] = self.Option.dt_maturity
        self.df_analysis = self.df_analysis.append(analysis,ignore_index=True)
        # print(self.synthetic_option.eval_datetime)
        # print(analysis)


# """ 每个月重设期权的到期日 """
# start_date = datetime.date(2015,1,1)
# end_date = datetime.date(2016,12,31)
# port = SyntheticOptionHedgedPortfolio(start_date, end_date)
# calendar = Calendar(port.trade_dates)
# dt_start = port.synthetic_option.eval_date
# dt_end = calendar.lastBusinessDayThisMonth(dt_start)
# # dt_maturity = dt_start + datetime.timedelta(days=60)
# dt_maturity = dt_end
# port.init_portfolio(dt_maturity, port.account.cash)
#
# while dt_end < end_date:
#
#     print('start hedge : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
#     port.hedge(dt_end)
#     port.analysis(dt_start,dt_end)
#     print('finished hedge : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
#     port.next()
#     dt_start = calendar.next(dt_end)
#     dt_end = calendar.lastBusinessDayThisMonth(dt_start)
#     # dt_maturity = dt_start + datetime.timedelta(days=60)
#     dt_maturity = dt_end
#     if dt_end == port.trade_dates[-1]:
#         port.close_out()
#         break
#     port.reset_option(dt_maturity)
#     print('reset option : ', port.Option.strike, port.synthetic_option.eval_datetime, port.underlying.eval_date)
#     port.shift_synthetic_open_by_VWAP()
#
# port.save_results()

start_date = datetime.date(2015,1,1)
end_date = datetime.date(2018,8,1)
port = SyntheticOptionHedgedPortfolio(start_date, end_date)
calendar = Calendar(port.trade_dates)
dt_start = port.synthetic_option.eval_date
dt_end = calendar.lastBusinessDayThisMonth(dt_start)

while dt_end != port.trade_dates[-1]:
    dt_maturity = dt_start + datetime.timedelta(days=40)
    # dt_maturity = dt_end
    print('Init Portfolio', dt_start, dt_end)
    port.init_portfolio(dt_maturity, port.account.cash)
    print('start hedge : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
    port.hedge(dt_end)
    port.close_out()
    port.analysis(dt_start,dt_end)
    print('finished hedge : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
    port.next()
    dt_start = calendar.next(dt_end)
    dt_end = calendar.lastBusinessDayThisMonth(dt_start)

port.save_results()


