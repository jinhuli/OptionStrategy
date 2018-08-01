import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from OptionStrategyLib.OptionReplication.synthetic_option import SytheticOption
from PricingLibrary.Options import EuropeanOption
from back_test.model.base_account import BaseAccount
from back_test.model.base_instrument import BaseInstrument
from back_test.model.constant import Util, OptionType, LongShort, ExecuteType
from back_test.model.trade import Trade
from data_access.get_data import get_dzqh_cf_daily, get_dzqh_cf_c1_daily, \
    get_dzqh_cf_c1_minute, get_index_mktdata
from OptionStrategyLib.VolatilityModel.historical_volatility import historical_volatility_model as Histvol
from Utilities import Analysis


#####################################################################

class SyntheticOptionHedgedPortfolio():
    def __init__(self):
        self.slippage = 0
        self.start_date = start_date = datetime.date(2015, 1, 1)
        # self.end_date = end_date = datetime.date(2018, 3, 1)
        self.end_date = end_date = datetime.date(2018, 8, 1)
        hist_date = start_date - datetime.timedelta(days=40)
        df_future_c1 = get_dzqh_cf_c1_minute(start_date, end_date, 'if')
        df_future_c1_daily = get_dzqh_cf_c1_daily(hist_date, end_date, 'if')
        df_futures_all_daily = get_dzqh_cf_daily(start_date, end_date, 'if')  # daily data of all future contracts
        df_index = get_index_mktdata(start_date, end_date, 'index_300sh')  # daily data of underlying index
        df_index = df_index[df_index[Util.DT_DATE].isin(Util.DZQH_CF_DATA_MISSING_DATES) == False].reset_index(
            drop=True)
        df_index.to_csv('df_index.csv')
        self.trade_dates = list(df_index[Util.DT_DATE].unique())
        self.df_vol_1m = Histvol.hist_vol(df_future_c1_daily)
        # df_parkinson_1m = Histvol.parkinson_number(df_future_c1_daily)
        self.df_garman_klass = Histvol.garman_klass(df_future_c1_daily)
        df_hist_vol = self.df_vol_1m.join(self.df_garman_klass, how='left')
        df_hist_vol.to_csv('df_hist_vol.csv')
        df_future_c1_daily = df_future_c1_daily[df_future_c1_daily[Util.DT_DATE] >= start_date].reset_index(drop=True)
        self.underlying_notional = Util.BILLION
        self.hedge_notional = Util.BILLION / 2
        self.underlying = BaseInstrument(df_data=df_index)
        self.underlying.init()
        self.amt_option = self.underlying_notional/self.underlying.mktprice_close()
        self.synthetic_option = SytheticOption(df_c1_minute=df_future_c1,
                                               # df_c1_daily=df_future_c1_daily,
                                               df_futures_all_daily=df_futures_all_daily,
                                               df_index_daily=df_index,
                                               amt_option=self.amt_option)
        self.synthetic_option.init()

        self.account = BaseAccount(self.underlying_notional + self.hedge_notional, leverage=20.0, rf=0.0)
        self.trading_desk = Trade()
        self.list_hedge_info = []
        self.init_spot = self.synthetic_option.underlying_index_state_daily[Util.AMT_CLOSE]
        self.df_analysis  = pd.DataFrame()

    def next(self):
        if self.synthetic_option.is_last_minute():
            self.underlying.next()
        self.synthetic_option.next()


    def init_portfolio(self, dt_maturity):

        """ Init position """

        strike = self.synthetic_option.underlying_index_state_daily[Util.AMT_CLOSE]
        # dt_maturity = self.synthetic_option.eval_date + datetime.timedelta(days=30)
        print('maturity date : ', dt_maturity)
        self.Option = EuropeanOption(strike, dt_maturity, OptionType.PUT)



        """ 用第一天的日收盘价开仓标的现货多头头寸 """
        underlying_unit = np.floor(Util.BILLION / self.underlying.mktprice_close())
        order_underlying = self.account.create_trade_order(self.underlying, LongShort.LONG, underlying_unit)
        execution_record = self.underlying.execute_order(order_underlying, slippage=0,
                                                         execute_type=ExecuteType.EXECUTE_ALL_UNITS)
        self.account.add_record(execution_record, self.underlying)

        """ 用第一天的成交量加权均价初次开仓复制期权头寸 """
        vol = self.get_vol()
        # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]
        self.delta = self.synthetic_option.get_black_delta(self.Option, vol)
        synthetic_unit = self.synthetic_option.get_synthetic_unit(self.delta)
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

        """ disp """
        self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                     Util.AMT_DELTA: self.delta,
                                     Util.AMT_HEDHE_UNIT: synthetic_unit,
                                     Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                     Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
                                     })

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
        d_unit = self.synthetic_option.get_synthetic_option_rebalancing_unit(self.delta)
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

        """ disp """
        self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                     Util.AMT_DELTA: self.delta,
                                     Util.AMT_HEDHE_UNIT: d_unit,
                                     Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                     Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
                                     })

        self.account.daily_accounting(self.synthetic_option.eval_date)
        self.add_additional_to_account()
        self.disp()
        self.next()

    def hedge(self, dt_end=None):

        id_future = self.synthetic_option.current_state[Util.ID_INSTRUMENT]
        if dt_end is None:
            dt_end = self.Option.dt_maturity
        dt_time_end = datetime.datetime(dt_end.year, dt_end.month,
                                        dt_end.day, 14, 59, 0)

        while self.synthetic_option.has_next() and self.synthetic_option.eval_datetime < dt_time_end:

            if id_future != self.synthetic_option.current_state[Util.ID_INSTRUMENT]:
                open_long_short = self.account.trade_book.loc[id_future, Util.TRADE_LONG_SHORT]
                hold_unit = self.account.trade_book.loc[id_future, Util.TRADE_UNIT]
                spot = self.synthetic_option.current_daily_state[Util.AMT_CLOSE]
                vol = self.get_vol()
                self.delta = self.synthetic_option.get_black_delta(self.Option, vol, spot)
                synthetic_unit = self.synthetic_option.get_synthetic_unit(self.delta)
                id_c2 = self.synthetic_option.current_state[Util.ID_INSTRUMENT]
                close_execution_record, open_execution_record \
                    = self.synthetic_option.shift_contract_by_VWAP(id_c1=id_future,
                                                                   id_c2=id_c2,
                                                                   hold_unit=hold_unit,
                                                                   open_unit=synthetic_unit,
                                                                   hold_long_short=open_long_short,
                                                                   slippage=self.slippage,
                                                                   execute_type=ExecuteType.EXECUTE_ALL_UNITS)

                self.account.add_record(close_execution_record, self.synthetic_option)
                self.synthetic_option._id_instrument = id_c2
                self.account.add_record(open_execution_record, self.synthetic_option)
                id_future = id_c2
                self.account.daily_accounting(self.synthetic_option.eval_date)  # 该日的收盘结算
                self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                             Util.AMT_DELTA: self.delta,
                                             Util.AMT_HEDHE_UNIT: synthetic_unit,
                                             Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                             Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
                                             })
                self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                             Util.AMT_DELTA: self.delta,
                                             Util.AMT_HEDHE_UNIT: hold_unit,
                                             Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                             Util.ID_INSTRUMENT: id_future
                                             })
                self.add_additional_to_account()
                self.disp()

                self.synthetic_option.next()
                self.underlying.next()

            if self.synthetic_option.eval_date != self.synthetic_option.get_next_state_date():
                date = self.synthetic_option.eval_date
                self.account.daily_accounting(date)  # 该日的收盘结算
                self.add_additional_to_account()
                self.disp()

                self.synthetic_option.next()

                # # 最后一个交易日不更新标的状态（否则会到到期下一天）
                # if self.synthetic_option.eval_date != dt_end:
                #     self.underlying.next()
                self.underlying.next()

            if self.synthetic_option.eval_datetime.minute % 10 != 0:
                self.synthetic_option.next()
                continue

            vol = self.get_vol()
            self.delta = self.synthetic_option.get_black_delta(self.Option, vol)
            rebalance_unit = self.synthetic_option.get_synthetic_option_rebalancing_unit(self.delta)
            if rebalance_unit > 0:
                long_short = LongShort.LONG
            elif rebalance_unit < 0:
                long_short = LongShort.SHORT
            else:
                self.synthetic_option.next()
                continue
            order = self.account.create_trade_order(self.synthetic_option,
                                                    long_short,
                                                    rebalance_unit)
            execution_record = self.synthetic_option.execute_order(order, slippage=self.slippage,
                                                                   execute_type=ExecuteType.EXECUTE_ALL_UNITS)
            self.account.add_record(execution_record, self.synthetic_option)
            self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                         Util.AMT_DELTA: self.delta,
                                         Util.AMT_HEDHE_UNIT: rebalance_unit,
                                         Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                         Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
                                         })

        close_out_orders = self.account.creat_close_out_order()

        for order in close_out_orders:
            execution_record = self.account.dict_holding[order.id_instrument].execute_order(order, slippage=self.slippage,
                                                                                            execute_type=ExecuteType.EXECUTE_ALL_UNITS)
            self.account.add_record(execution_record, self.account.dict_holding[order.id_instrument])
            self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                        Util.AMT_DELTA: None,
                                        Util.AMT_HEDHE_UNIT: order.trade_unit,
                                        Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                        Util.ID_INSTRUMENT: order.id_instrument
                                        })
        self.account.daily_accounting(self.synthetic_option.eval_date)
        self.add_additional_to_account()
        self.disp()


        """ Result Analysis """
        self.analysis()

    """ 到期不平仓 """
    def hedge_continuous(self, dt_end=None):

        id_future = self.synthetic_option.current_state[Util.ID_INSTRUMENT]
        if dt_end is None:
            dt_end = self.Option.dt_maturity
        dt_time_end = datetime.datetime(dt_end.year, dt_end.month,
                                        dt_end.day, 15, 0, 0)

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
                synthetic_unit = self.synthetic_option.get_synthetic_unit(self.delta) # 按照移仓换月日的收盘价计算Delta
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
                self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                             Util.AMT_DELTA: self.delta,
                                             Util.AMT_HEDHE_UNIT: -hold_unit,
                                             Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                             Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
                                             })
                self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                             Util.AMT_DELTA: self.delta,
                                             Util.AMT_HEDHE_UNIT: open_unit,
                                             Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                             Util.ID_INSTRUMENT: id_future
                                             })
                id_future = id_c2

            if self.synthetic_option.eval_date == self.synthetic_option.get_next_state_date():
                if self.synthetic_option.eval_datetime.minute % 10 != 0:
                    self.next()
                    continue

            # print(self.synthetic_option.eval_datetime)
            self.rebalancing()

            if self.synthetic_option.is_last_minute():
                self.account.daily_accounting(self.synthetic_option.eval_date)  # 该日的收盘结算
                self.add_additional_to_account()
                self.disp()
            self.next()

        # self.account.daily_accounting(self.synthetic_option.eval_date)  # 该日的收盘结算
        # self.add_additional_to_account()
        # self.disp()
        # self.analysis()
        # self.underlying.next()
        # self.synthetic_option.next()

    def rebalancing(self):
        vol = self.get_vol()
        # vol = self.df_garman_klass.loc[self.synthetic_option.eval_date, Util.AMT_GARMAN_KLASS]
        self.delta = self.synthetic_option.get_black_delta(self.Option, vol)
        rebalance_unit = self.synthetic_option.get_synthetic_option_rebalancing_unit(self.delta)
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
        self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                     Util.AMT_DELTA: self.delta,
                                     Util.AMT_HEDHE_UNIT: rebalance_unit,
                                     Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                     Util.ID_INSTRUMENT: self.synthetic_option.id_instrument()
                                     })

    def close_out(self):
        close_out_orders = self.account.creat_close_out_order()

        for order in close_out_orders:
            execution_record = self.account.dict_holding[order.id_instrument].execute_order(order, slippage=self.slippage,
                                                                                            execute_type=ExecuteType.EXECUTE_ALL_UNITS)
            self.account.add_record(execution_record, self.account.dict_holding[order.id_instrument])
            self.list_hedge_info.append({Util.DT_DATETIME: self.synthetic_option.eval_datetime,
                                         Util.AMT_DELTA: None,
                                         Util.AMT_HEDHE_UNIT: order.trade_unit,
                                         Util.AMT_UNDERLYING_CLOSE: self.underlying.mktprice_close(),
                                         Util.ID_INSTRUMENT: order.id_instrument
                                         })
        self.account.daily_accounting(self.synthetic_option.eval_date)
        self.add_additional_to_account()
        self.disp()

        """ Result Analysis """
        self.analysis()

    def get_vol(self):

        date = self.synthetic_option.eval_date
        if date in self.df_vol_1m.index:
            vol = self.df_vol_1m.loc[date, Util.AMT_HISTVOL]
        else:
            dt1 = Util.largest_element_less_than(port.trade_dates, date)
            vol = self.df_vol_1m.loc[dt1, Util.AMT_HISTVOL]
        return vol

    def add_additional_to_account(self):
        self.account.account.loc[
            self.synthetic_option.eval_date, 'underlying_npv'] = self.underlying.mktprice_close() / self.init_spot
        self.account.account.loc[
            self.synthetic_option.eval_date, 'underlying_price'] = self.underlying.mktprice_close()
        self.account.account.loc[self.synthetic_option.eval_date, 'if_c1'] = self.synthetic_option.mktprice_close()
        self.account.account.loc[self.synthetic_option.eval_date,'hedge_position'] \
            =  - self.account.trade_book[self.account.trade_book[Util.TRADE_LONG_SHORT]==LongShort.SHORT][Util.TRADE_UNIT].sum()

    def analysis(self):
        """ Result Analysis """
        self.df_records = pd.DataFrame(self.account.list_records)
        total_pnl = self.df_records[Util.TRADE_REALIZED_PNL].sum()
        final_npv = (self.underlying_notional + self.hedge_notional + total_pnl) / (self.underlying_notional + self.hedge_notional)
        print('calculate final npv from adding up realized pnl ; ', final_npv)
        analysis = Analysis.get_netvalue_analysis(port.account.account[Util.PORTFOLIO_NPV])
        df_hedge_records = self.df_records[self.df_records[Util.ID_INSTRUMENT] != 'index_300sh']
        hedge_position_pnl = df_hedge_records[Util.TRADE_REALIZED_PNL].sum()
        option_pnl = self.amt_option*max(self.Option.strike - self.underlying.mktprice_close(), 0)
        replicate_cost = hedge_position_pnl - option_pnl
        pct_replicate_cost = replicate_cost/Util.BILLION
        transaction_cost = df_hedge_records[Util.TRANSACTION_COST].sum()
        analysis['hedge_position_pnl'] = hedge_position_pnl
        analysis['option_pnl'] = option_pnl
        analysis['replicate_cost'] = replicate_cost
        analysis['pct_replicate_cost'] = pct_replicate_cost
        analysis['transaction_cost'] = transaction_cost
        analysis['dt_maturity'] = self.Option.dt_maturity
        self.df_analysis.append(analysis,ignore_index=True)
        # print(analysis)

    def save_results(self):

        self.df_records.to_csv('trade_records.csv')
        self.account.account.to_csv('account.csv')
        self.df_hedge_info = pd.DataFrame(self.list_hedge_info)
        self.df_hedge_info.to_csv('hedge_info.csv')
        self.df_analysis.to_csv('df_analysis.csv')
        self.account.trade_book_daily.to_csv('trade_book_daily.csv')

    def disp(self):
        if self.synthetic_option.eval_date != self.underlying.eval_date:
            print('Date miss matched!')
        print(self.synthetic_option.eval_datetime,
              self.account.account.loc[self.synthetic_option.eval_date, Util.PORTFOLIO_NPV],
              self.underlying.mktprice_close() / self.init_spot,
              self.account.account.loc[self.synthetic_option.eval_date, 'hedge_position'],
              self.synthetic_option.synthetic_unit,
              self.underlying.eval_date,
              self.Option.strike,
              self.synthetic_option.mktprice_close(),
              self.delta,
              self.account.cash)


    def reset_option(self,maturity,strike=None):
        print('maturity date : ', maturity)
        if strike is None:
            strike = self.underlying.mktprice_close()
        self.Option = EuropeanOption(strike,maturity,OptionType.PUT)
#
# port = SyntheticOptionHedgedPortfolio()
# dt_maturity = Util.largest_element_less_than(port.trade_dates,port.start_date + datetime.timedelta(days=30))
# dt_end = Util.largest_element_less_than(port.trade_dates,port.start_date + datetime.timedelta(days=20))
# port.init_portfolio(dt_maturity)
# while dt_end < port.end_date:
#     print('start time : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
#     port.hedge_continuous(dt_end)
#     print('finished time : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
#
#     dt_end = Util.largest_element_less_than(port.trade_dates, port.synthetic_option.eval_date + datetime.timedelta(days=20))
#     dt_maturity = Util.largest_element_less_than(port.trade_dates, port.synthetic_option.eval_date + datetime.timedelta(days=30))
#     if dt_end == port.trade_dates[-1]:
#         port.close_out()
#         break
#     port.synthetic_option.next()
#     port.underlying.next()
#     print('next open time : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
#     port.reset_option(dt_maturity)
#     port.shift_synthetic_open_by_VWAP()
#
# # port.close_out()
# port.save_results()

""" 1st month """
port = SyntheticOptionHedgedPortfolio()
dt_maturity = port.start_date + datetime.timedelta(days=40)
tmp = port.start_date + relativedelta(months=1)
dt_end = Util.largest_element_less_than(port.trade_dates,tmp)
port.init_portfolio(dt_maturity)
while dt_end <= port.end_date:
    print('start hedge : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
    port.hedge_continuous(dt_end)
    print('finished hedge : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
    tmp += relativedelta(months=1)
    dt_end = Util.largest_element_less_than(port.trade_dates, tmp)
    dt_maturity = port.synthetic_option.eval_date + datetime.timedelta(days=40)
    if dt_end == port.trade_dates[-1]:
        port.close_out()
        break
    print('reset option : ', port.synthetic_option.eval_datetime, port.underlying.eval_date)
    port.reset_option(dt_maturity)
    # hold_unit = - port.account.trade_book[port.account.trade_book[Util.TRADE_LONG_SHORT]==LongShort.SHORT][Util.TRADE_UNIT].sum()
    port.shift_synthetic_open_by_VWAP()

# port.close_out()
port.save_results()

