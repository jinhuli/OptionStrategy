import pandas as pd
import datetime
import math
from back_test.bkt_util import BktUtil
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import host_subplot
from Utilities.PlotUtil import PlotUtil
import numpy as np
from back_test.bkt_option import BktOption
from back_test.OptionPortfolio import *


class BktAccount(object):
    def __init__(self, cd_open_price='close', cd_close_price='close', leverage=1.0, margin_rate=0.1,
                 init_fund=1000000.0, tick_size=0.0001,
                 contract_multiplier=10000, fee_rate=0.0 / 10000, nbr_slippage=0, rf=0.0):

        self.util = BktUtil()
        self.init_fund = init_fund
        self.fee = fee_rate
        self.cash = init_fund
        self.total_asset = init_fund
        self.rf = rf
        self.cd_open_by_price = cd_open_price
        self.cd_close_by_price = cd_close_price
        self.contract_multiplier = contract_multiplier
        self.total_margin_capital = 0.0
        self.total_transaction_cost = 0.0
        self.npv = 1.0
        self.mtm_value = 0.0
        self.realized_value = 0.0
        self.nbr_trade = 0
        self.realized_pnl = 0.0
        self.df_trading_book = pd.DataFrame()  # 持仓信息
        self.df_account = pd.DataFrame()  # 交易账户
        self.df_trading_records = pd.DataFrame()  # 交易记录
        self.df_ivs = pd.DataFrame() # 交易期权的隐含波动率
        self.holdings = []  # 当前持仓
        self.pu = PlotUtil()
        self.trade_order_dict = {}
        self.total_premiums_long = 0.0


    def get_open_position_price(self, bktoption,cd_open_by_price):
        if cd_open_by_price == 'open':
            mkt_price = bktoption.option_price_open
        elif cd_open_by_price == 'close':
            mkt_price = bktoption.option_price
        elif cd_open_by_price == 'morning_open_15min':
            if bktoption.option_morning_open_15min != -999.0:
                mkt_price = bktoption.option_morning_open_15min
            elif bktoption.option_price_open != -999.0:
                mkt_price = bktoption.option_price_open
            elif bktoption.option_morning_avg != -999.0:
                mkt_price = bktoption.option_morning_avg
            elif bktoption.option_daily_avg != -999.0:
                mkt_price = bktoption.option_daily_avg
            else:
                print(bktoption.id_instrument,'No volume to open position')
                mkt_price = bktoption.option_price

        elif cd_open_by_price == 'afternoon_close_15min':
            if bktoption.option_afternoon_close_15min != -999.0:
                mkt_price = bktoption.option_afternoon_close_15min
            else:
                print(bktoption.id_instrument,'No volume to open position')
                mkt_price = bktoption.option_price

        elif cd_open_by_price == 'daily_avg':
            if bktoption.option_daily_avg != -999.0:
                mkt_price = bktoption.option_daily_avg
            else:
                mkt_price = bktoption.option_price
        else:
            mkt_price = bktoption.option_price
        return mkt_price

    def get_close_position_price(self, bktoption,cd_close_by_price=None):
        if cd_close_by_price == 'open':
            mkt_price = bktoption.option_price_open
        elif cd_close_by_price == 'close':
            mkt_price = bktoption.option_price
        elif cd_close_by_price == 'morning_open_15min':
            if bktoption.option_morning_open_15min != -999.0:
                mkt_price = bktoption.option_morning_open_15min
            elif bktoption.option_morning_avg != -999.0:
                mkt_price = bktoption.option_morning_avg
            elif bktoption.option_daily_avg != -999.0:
                mkt_price = bktoption.option_daily_avg
            else:
                print(bktoption.id_instrument,'No volume to close position')
                mkt_price = bktoption.option_price
        elif cd_close_by_price == 'afternoon_close_15min':
            if bktoption.option_afternoon_close_15min != -999.0:
                mkt_price = bktoption.option_afternoon_close_15min
            elif bktoption.option_afternoon_avg != -999.0:
                mkt_price = bktoption.option_afternoon_avg
            elif bktoption.option_daily_avg != -999.0:
                mkt_price = bktoption.option_daily_avg
            else:
                print(bktoption.id_instrument,'No volume to close position')
                mkt_price = bktoption.option_price
        else:
            mkt_price = bktoption.option_price
        return mkt_price

    def update_invest_units(self,option_port,cd_long_short,cd_open_by_price=None, fund=None):
        if isinstance(option_port,BackSpread):
            option_long = option_port.option_long
            option_short = option_port.option_short
            plong = self.get_open_position_price(option_long,cd_open_by_price)
            pshort = self.get_open_position_price(option_short,cd_open_by_price)
            option_port.delta_neutral_rebalancing()

            if fund == None:
                fund = plong*option_long.multiplier*option_port.unit_long + \
                       option_short.trade_margin_capital-pshort*option_short.multiplier*option_port.unit_short
                fund0 = plong * option_long.multiplier * option_port.invest_ratio_long + \
                        (option_short.get_maintain_margin() - pshort*option_short.multiplier)*option_port.invest_ratio_short
            else:
                fund0 = plong * option_long.multiplier * option_port.invest_ratio_long + \
                        (option_short.get_init_margin()-pshort*option_short.multiplier)*option_port.invest_ratio_short
            unit = fund / fund0
            option_port.unit_portfolio = unit
            option_port.unit_long = unit*option_port.invest_ratio_long
            option_port.unit_short = unit*option_port.invest_ratio_short
        if isinstance(option_port,Straddle):
            option_call = option_port.option_call
            option_put = option_port.option_put
            price_call = self.get_open_position_price(option_call, cd_open_by_price)
            price_put = self.get_open_position_price(option_put, cd_open_by_price)
            if fund == None:
                fund = price_call*option_call.multiplier*option_port.unit_call + \
                       price_put*option_put.multiplier*option_port.unit_put
            option_port.delta_neutral_rebalancing()
            if cd_long_short == self.util.long:
                fund0 = price_call*option_call.multiplier*option_port.invest_ratio_call + \
                       price_put*option_put.multiplier*option_port.invest_ratio_put
                unit_straddle = fund / fund0
            else:
                fund_straddle = option_call.get_init_margin()*option_call.multiplier*option_port.invest_ratio_call + \
                       option_put.get_maintain_margin()*option_put.multiplier*option_port.invest_ratio_put
                unit_straddle = fund / fund_straddle
            option_port.unit_portfolio = unit_straddle
            option_port.unit_call = unit_straddle*option_port.invest_ratio_call
            option_port.unit_put = unit_straddle*option_port.invest_ratio_put
        elif isinstance(option_port,Calls) or isinstance(option_port,Puts):
            if option_port.cd_weighted == 'equal_unit':
                if fund == None:
                    fund = 0
                    for (idx,option) in enumerate(option_port.optionset):
                        price = self.get_open_position_price(option, cd_open_by_price)
                        fund += price*option.multiplier*option_port.unit_portfolio[idx]
                fund0 = 0
                for option in option_port.optionset: # equal weighted by mkt value
                    price = self.get_open_position_price(option, cd_open_by_price)
                    fund0 += price*option.multiplier
                unit_calls = fund/fund0
                option_port.unit_portfolio = [unit_calls]*len(option_port.optionset)
        elif isinstance(option_port,CalandarSpread):
            p1 = self.get_open_position_price(option_port.option_mdt1, cd_open_by_price)
            p2 = self.get_open_position_price(option_port.option_mdt2, cd_open_by_price)
            fund0 = p1+p2
            unit = fund / fund0
            option_port.unit_portfolio = unit

    def open_position(self, dt, portfolio,unit=None, cd_open_by_price=None):
        if isinstance(portfolio,BktOption):
            self.open_long_option(dt, portfolio, unit,cd_open_by_price)
        elif type(portfolio) == dict:
            self.option_long_index(portfolio)
        elif isinstance(portfolio,Straddle):
            self.open_long_option(dt,portfolio.option_call,portfolio.unit_call,cd_open_by_price)
            self.open_long_option(dt,portfolio.option_put,portfolio.unit_put,cd_open_by_price)
        elif isinstance(portfolio,Calls) or isinstance(portfolio,Puts):
            for option in portfolio.optionset:
                self.open_long_option(dt,option,portfolio.unit_portfolio[0],cd_open_by_price)
        elif isinstance(portfolio,CalandarSpread):
            self.open_long_option(dt, portfolio.option_mdt1, portfolio.unit_portfolio, cd_open_by_price)
            self.open_long_option(dt, portfolio.option_mdt2, portfolio.unit_portfolio, cd_open_by_price)
        elif isinstance(portfolio,BackSpread):
            self.open_long_option(dt,portfolio.option_long,portfolio.unit_long,cd_open_by_price)
            self.open_short_option(dt,portfolio.option_short,portfolio.unit_short,cd_open_by_price)

    def rebalance_position(self, dt, portfolio,unit=None):
        if isinstance(portfolio,BktOption):
            self.rebalance_position_option(dt, portfolio, unit)
        elif type(portfolio) == dict:
            self.rebalance_position_index(dt, portfolio)
        elif isinstance(portfolio,Straddle):
            self.rebalance_position_option(dt,portfolio.option_call,portfolio.unit_call)
            self.rebalance_position_option(dt,portfolio.option_put,portfolio.unit_put)
        elif isinstance(portfolio,BackSpread):
            self.rebalance_position_option(dt,portfolio.option_long,portfolio.unit_long)
            self.rebalance_position_option(dt,portfolio.option_short,portfolio.unit_short)
        elif isinstance(portfolio,Calls) or isinstance(portfolio,Puts):
            for (i,option) in enumerate(portfolio.optionset):
                self.rebalance_position_option(dt,option,portfolio.unit_portfolio[i])

    def close_position(self, dt, position,cd_close_by_price=None):
        if type(position) == dict:
            self.close_position_index(position)
        else:
            self.close_position_option(dt, position,cd_close_by_price)

    def option_long_index(self, trade_order_dict):
        mkt_price = trade_order_dict['price']
        unit = trade_order_dict['unit']
        id_instrument = trade_order_dict['id_instrument']
        dt = trade_order_dict['dt_date']
        premium = unit * mkt_price
        fee = premium * self.fee
        trade_type = 'open'
        self.cash = self.cash - mkt_price * unit - fee
        self.total_transaction_cost += fee
        self.nbr_trade += 1
        trade_order_dict['open_transaction_fee'] = fee
        trade_order_dict['long_short'] = 1
        # if trade_order_dict not in self.holdings: self.holdings.append(trade_order_dict)
        record = pd.DataFrame(data={self.util.id_instrument: [id_instrument],
                                    self.util.dt_trade: [dt],
                                    self.util.trading_type: [trade_type],
                                    self.util.trade_price: [mkt_price],
                                    self.util.trading_cost: [fee],
                                    self.util.unit: [unit],
                                    'premium paid': [premium],
                                    'cash': [self.cash],
                                    'margin capital': [self.total_margin_capital]
                                    })
        self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)
        self.trade_order_dict = trade_order_dict

    def open_long_option(self, dt, bktoption, unit,cd_open_by_price):  # 多开
        bktoption.trade_dt_open = dt
        bktoption.trade_long_short = self.util.long
        id_instrument = bktoption.id_instrument
        mkt_price = self.get_open_position_price(bktoption,cd_open_by_price)
        multiplier = bktoption.multiplier
        trade_type = 'open long'
        fee = unit * mkt_price * self.fee * multiplier
        premium = unit * mkt_price * multiplier
        margin_capital = 0.0
        bktoption.trade_unit = unit
        bktoption.premium = premium
        bktoption.trade_open_price = mkt_price
        bktoption.trade_margin_capital = margin_capital
        bktoption.transaction_fee = fee
        bktoption.trade_flag_open = True
        if bktoption not in self.holdings: self.holdings.append(bktoption)
        self.cash = self.cash - premium - margin_capital-fee
        self.total_transaction_cost += fee
        self.nbr_trade += 1
        record = pd.DataFrame(data={self.util.id_instrument: [id_instrument],
                                    self.util.dt_trade: [dt],
                                    self.util.trading_type: [trade_type],
                                    self.util.trade_price: [mkt_price],
                                    self.util.trading_cost: [fee],
                                    self.util.unit: [unit],
                                    'premium paid': [premium],
                                    'cash': [self.cash],
                                    'margin capital': [self.total_margin_capital]
                                    })
        self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)

    def open_short_option(self, dt, bktoption, unit,cd_open_by_price):
        bktoption.trade_dt_open = dt
        bktoption.trade_long_short = self.util.short
        id_instrument = bktoption.id_instrument
        mkt_price = self.get_open_position_price(bktoption,cd_open_by_price)
        multiplier = bktoption.multiplier
        trade_type = 'open short'
        fee = unit * mkt_price * self.fee * multiplier
        premium = unit * mkt_price * multiplier
        margin_capital = unit * bktoption.get_init_margin()
        bktoption.trade_unit = unit
        bktoption.premium = premium
        bktoption.trade_open_price = mkt_price
        bktoption.trade_margin_capital = margin_capital
        bktoption.transaction_fee = fee
        bktoption.trade_flag_open = True
        if bktoption not in self.holdings : self.holdings.append(bktoption)
        self.cash = self.cash + premium - margin_capital-fee
        self.total_margin_capital += margin_capital
        self.total_transaction_cost += fee
        self.nbr_trade += 1
        record = pd.DataFrame(data={self.util.id_instrument: [id_instrument],
                                    self.util.dt_trade: [dt],
                                    self.util.trading_type: [trade_type],
                                    self.util.trade_price: [mkt_price],
                                    self.util.trading_cost: [fee],
                                    self.util.unit: [unit],
                                    'premium paid': [-premium],
                                    'cash': [self.cash],
                                    'margin capital': [self.total_margin_capital]
                                    })
        self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)

    def close_position_index(self, trade_order_dict):
        position = pd.Series()
        mkt_price = float(trade_order_dict['price'])
        unit = self.trade_order_dict['unit']
        id_instrument = trade_order_dict['id_instrument']
        dt = trade_order_dict['dt_date']
        long_short = self.trade_order_dict['long_short']
        open_price = self.trade_order_dict['price']
        open_transaction_fee = self.trade_order_dict['open_transaction_fee']
        self.cash = self.cash + mkt_price * unit
        trade_type = 'close'
        fee = mkt_price * unit * self.fee
        realized_pnl = long_short * (
            unit * (mkt_price - open_price)) - open_transaction_fee - fee
        self.trade_order_dict['close_transaction_fee'] = fee
        position[self.util.close_price] = mkt_price
        position[self.util.open_price] = open_price
        position[self.util.realized_pnl] = realized_pnl
        self.df_trading_book = self.df_trading_book.append(position, ignore_index=True)
        record = pd.DataFrame(data={self.util.id_instrument: [id_instrument],
                                    self.util.dt_trade: [dt],
                                    self.util.trading_type: [trade_type],
                                    self.util.trade_price: [mkt_price],
                                    self.util.trading_cost: [fee],
                                    self.util.unit: [unit],
                                    'premium paid': [- mkt_price * unit],
                                    'cash': [self.cash],
                                    'margin capital': [self.total_margin_capital]
                                    })
        self.nbr_trade += 1
        self.realized_pnl += realized_pnl
        self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)
        self.trade_order_dict = {}

    def close_position_option(self, dt, bktoption,cd_close_by_price):  # 多空平仓
        if bktoption.trade_flag_open:
            id_instrument = bktoption.id_instrument
            mkt_price = self.get_close_position_price(bktoption,cd_close_by_price)
            unit = bktoption.trade_unit
            long_short = bktoption.trade_long_short
            margin_capital = bktoption.trade_margin_capital
            dt_open = bktoption.trade_dt_open
            multiplier = bktoption.multiplier
            premium_open = bktoption.premium
            open_price = bktoption.trade_open_price
            premium = unit * mkt_price * multiplier

            if long_short == self.util.long:
                trade_type = 'close long'
            else:
                trade_type = 'close short'
            fee = unit * mkt_price * self.fee * multiplier
            realized_pnl = long_short * (
                unit * mkt_price * multiplier - premium_open) - bktoption.transaction_fee - fee
            premium_to_cash = long_short * premium

            self.cash = self.cash + margin_capital + premium_to_cash
            # self.cash = self.cash + margin_capital + realized_pnl + premium_to_cash
            self.total_margin_capital -= margin_capital
            self.total_transaction_cost += fee
            self.nbr_trade += 1
            self.realized_pnl += realized_pnl

            position = pd.Series()
            position[self.util.id_instrument] = id_instrument
            position[self.util.dt_open] = dt_open
            position[self.util.long_short] = long_short
            position[self.util.open_price] = open_price
            position[self.util.unit] = unit
            position[self.util.margin_capital] = margin_capital
            position[self.util.flag_open] = False
            position[self.util.multiplier] = multiplier
            position[self.util.dt_close] = dt
            position[self.util.days_holding] = (dt - dt_open).days
            position[self.util.close_price] = mkt_price
            position[self.util.realized_pnl] = realized_pnl
            self.df_trading_book = self.df_trading_book.append(position, ignore_index=True)

            record = pd.DataFrame(data={self.util.id_instrument: [id_instrument],
                                        self.util.dt_trade: [dt],
                                        self.util.trading_type: [trade_type],
                                        self.util.trade_price: [mkt_price],
                                        self.util.trading_cost: [fee],
                                        self.util.unit: [unit],
                                        'premium paid': [-long_short * premium],
                                        'cash': [self.cash],
                                        'margin capital': [self.total_margin_capital]
                                        })
            self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)
            self.liquidate_option(bktoption=bktoption)

    def rebalance_position_index(self, dt, trade_order_dict):
        holding_unit = self.trade_order_dict['unit']
        open_price = self.trade_order_dict['price']
        long_short = self.trade_order_dict['long_short']
        id_instrument = self.trade_order_dict['id_instrument']
        unit = trade_order_dict['unit']
        mkt_price = trade_order_dict['price']
        if unit != holding_unit:
            if unit > holding_unit:  # 加仓
                open_price = ((unit - holding_unit) * mkt_price + holding_unit * open_price) / unit  # 加权开仓价格
                market_value = (unit - holding_unit) * mkt_price
                fee = market_value * self.fee
                self.trade_order_dict['open_transaction_fee'] += fee
                self.cash = self.cash - long_short * market_value - fee
                self.total_transaction_cost += fee
            else:  # 减仓
                liquidated_unit = holding_unit - unit
                market_value = liquidated_unit * mkt_price
                fee = market_value * self.fee
                d_fee = self.trade_order_dict['open_transaction_fee'] * liquidated_unit / holding_unit
                realized_pnl = long_short * liquidated_unit * (mkt_price - open_price) - fee - d_fee
                self.trade_order_dict['open_transaction_fee'] -= d_fee
                self.realized_pnl += realized_pnl
                self.cash = self.cash + long_short * market_value - fee
                self.total_transaction_cost += fee
            self.trade_order_dict['price'] = open_price
            self.trade_order_dict['unit'] = unit
            if long_short == self.util.long:
                if unit > holding_unit:
                    trade_type = '多开'
                else:
                    trade_type = '多平'
            else:
                if unit > holding_unit:
                    trade_type = '空开'
                else:
                    trade_type = '空平'
            record = pd.DataFrame(data={self.util.id_instrument: [id_instrument],
                                        self.util.dt_trade: [dt],
                                        self.util.trading_type: [trade_type],
                                        self.util.trade_price: [mkt_price],
                                        self.util.trading_cost: [fee],
                                        self.util.unit: [unit],
                                        'premium paid': [market_value],
                                        'cash': [self.cash],
                                        'margin capital': [self.total_margin_capital]
                                        })
            self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)
            self.nbr_trade += 1

    def rebalance_position_option(self, dt, bktoption,unit):
        id_instrument = bktoption.id_instrument
        mkt_price = self.get_close_position_price(bktoption)
        holding_unit = bktoption.trade_unit
        long_short = bktoption.trade_long_short
        open_price = bktoption.trade_open_price
        multiplier = bktoption.multiplier
        premium = bktoption.premium
        total_cash_returned = 0.0
        if bktoption.maturitydt == dt:
            self.close_position_option(dt,bktoption,None)
            print('close option position on maturity date : ',bktoption.id_instrument)
        else:
            if unit != holding_unit:
                if unit > holding_unit:  # 加仓
                    margin_add = (unit - holding_unit) * bktoption.get_init_margin()
                    open_price = ((unit - holding_unit) * mkt_price + holding_unit * open_price) / unit  # 加权开仓价格
                    premium_add_mkt_v = (unit - holding_unit) * mkt_price * multiplier
                    fee = premium_add_mkt_v * self.fee
                    premium += premium_add_mkt_v
                    premium_paid = long_short * premium_add_mkt_v
                    self.cash = self.cash - margin_add - premium_paid - fee
                    self.total_margin_capital += margin_add
                    self.total_transaction_cost += fee
                    bktoption.trade_margin_capital += margin_add
                    bktoption.transaction_fee += fee
                    total_cash_returned += - margin_add - premium_paid - fee
                else:  # 减仓
                    liquidated_unit = holding_unit - unit
                    margin_returned = liquidated_unit * bktoption.trade_margin_capital / bktoption.trade_unit
                    premium_lqdt_init_v = liquidated_unit * open_price * multiplier # 减仓部分的初始开仓价值
                    premium_lqdt_mkt_v = liquidated_unit * mkt_price * multiplier # 减仓部分的现值
                    fee = premium_lqdt_mkt_v * self.fee  # 卖出liquidated_unit的交易费用
                    d_fee = bktoption.transaction_fee * liquidated_unit / holding_unit  # 持仓总交易费用按比例扣减
                    realized_pnl = long_short * (premium_lqdt_mkt_v - premium_lqdt_init_v) - fee - d_fee
                    premium -= premium_lqdt_mkt_v
                    premium_paid = -long_short * premium_lqdt_mkt_v
                    self.realized_pnl += realized_pnl
                    self.cash = self.cash + margin_returned - premium_paid - fee
                    self.total_margin_capital -= margin_returned
                    self.total_transaction_cost += fee
                    bktoption.trade_margin_capital -= margin_returned
                    bktoption.transaction_fee -= d_fee
                    total_cash_returned += margin_returned - premium_paid - fee

                bktoption.trade_unit = unit
                bktoption.premium = premium
                bktoption.trade_open_price = open_price
                if long_short == self.util.long:
                    if unit > holding_unit:
                        trade_type = 'open long'
                    else:
                        trade_type = 'close long'
                else:
                    if unit > holding_unit:
                        trade_type = 'open short'
                    else:
                        trade_type = 'close short'
                record = pd.DataFrame(data={self.util.id_instrument: [id_instrument],
                                            self.util.dt_trade: [dt],
                                            self.util.trading_type: [trade_type],
                                            self.util.trade_price: [mkt_price],
                                            self.util.trading_cost: [fee],
                                            self.util.unit: [unit],
                                            'premium paid': [premium_paid],
                                            'cash': [self.cash],
                                            'margin capital': [self.total_margin_capital],
                                            'total_cash_returned': [total_cash_returned]
                                            })
                self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)
                self.nbr_trade += 1

    def mkm_update(self, dt, trade_order_dict=None):  # 每日更新
        unrealized_pnl = 0.0
        mtm_long_positions = 0.0
        mtm_short_positions = 0.0
        # total_premium_paied = 0.0
        # short_positions_pnl = 0.0
        self.cash = self.cash * (1 + (1.0 / 365) * self.rf)
        port_delta = 0.0
        holdings = []
        for bktoption in self.holdings:
            if not bktoption.trade_flag_open: continue
            if bktoption.maturitydt == dt:
                self.close_position_option(dt, bktoption, None)
                print('close option position on maturity date : ', bktoption.id_instrument)
                continue
            holdings.append(bktoption)
            # if bktoption.get_settlement != -999.0:
            #     mkt_price = bktoption.get_settlement() # 优先用结算价计算每日净值
            # else:
            #     mkt_price = bktoption.option_price
            mkt_price = bktoption.option_price
            unit = bktoption.trade_unit
            long_short = bktoption.trade_long_short
            margin_account = bktoption.trade_margin_capital
            multiplier = bktoption.multiplier
            maintain_margin = unit * bktoption.get_maintain_margin()
            margin_call = maintain_margin - margin_account
            unrealized_pnl += long_short * (mkt_price-bktoption.trade_open_price) * unit * multiplier
            if long_short == self.util.long:
                mtm_long_positions += mkt_price * unit * multiplier
                # total_premium_paied += bktoption.premium
                port_delta += unit * multiplier * bktoption.get_delta() / self.contract_multiplier
            else:
                mtm_short_positions -= mkt_price * unit * multiplier
                port_delta -= unit * multiplier * bktoption.get_delta() / self.contract_multiplier
                # short_positions_pnl += (bktoption.trade_open_price-mkt_price)*unit*multiplier
            # bktoption.trade_margin_capital = maintain_margin
            self.cash -= margin_call
            self.total_margin_capital += margin_call
            bktoption.trade_margin_capital += margin_call
            iv = pd.DataFrame(data={'dt_date':[dt],'id':[bktoption.id_instrument],'price':[bktoption.option_price],
                                    'unit':[bktoption.trade_unit],
                                    'iv':[bktoption.implied_vol],'long_short':[bktoption.trade_long_short]})
            self.df_ivs = self.df_ivs.append(iv, ignore_index=True)

        """trade_order_dict : Only Contains LONG underlying positions"""
        trade_order_mktv = 0.0
        if self.trade_order_dict != {} and trade_order_dict != None:
            unit = self.trade_order_dict['unit']
            mkt_price = trade_order_dict['price']
            trade_order_mktv += mkt_price * unit

        self.trade_order_mktv = trade_order_mktv # Underlying market value
        """ For long positions only, total_asset = cash + mtm_long_positions(i.e., total premiums);
            For short positions only, total_asset = cash + margin_capital + short posiitons pnl(unrealized)"""
        self.total_premiums_long = mtm_long_positions
        interest = self.cash * (1.0 / 365) * self.rf
        # total_asset1 = self.cash + self.total_margin_capital + mtm_long_positions + mtm_short_positions + trade_order_mktv
        # total_asset2 = self.total_asset - self.total_transaction_cost + unrealized_pnl + interest
        self.total_asset = self.cash + self.total_margin_capital + mtm_long_positions + mtm_short_positions + trade_order_mktv
        self.mtm_short_positions = mtm_short_positions
        self.mtm_long_positions = mtm_long_positions
        money_utilization = 1- self.cash / self.total_asset
        self.npv = self.total_asset / self.init_fund
        self.holdings = holdings
        account = pd.DataFrame(data={self.util.dt_date: [dt],self.util.npv: [self.npv],self.util.nbr_trade: [self.nbr_trade],
                                     self.util.margin_capital: [self.total_margin_capital], self.util.realized_pnl: [self.realized_pnl],
                                     self.util.unrealized_pnl: [unrealized_pnl], self.util.mtm_long_positions: [mtm_long_positions],
                                     self.util.mtm_short_positions: [mtm_short_positions], self.util.cash: [self.cash],
                                     self.util.money_utilization: [money_utilization],self.util.total_asset: [self.total_asset],
                                     'portfolio delta': [port_delta]})
        self.df_account = self.df_account.append(account, ignore_index=True)

        self.nbr_trade = 0
        self.realized_pnl = 0

    def liquidate_all(self, dt):
        holdings = self.holdings.copy()
        for bktoption in holdings:
            self.close_position(dt, bktoption)

    def liquidate_option(self, bktoption=None, trade_order_dict=None):
        bktoption.trade_flag_open = False
        bktoption.trade_unit = None
        bktoption.trade_dt_open = None
        bktoption.trade_long_short = None
        bktoption.premium = None
        bktoption.trade_open_price = None
        bktoption.trade_margin_capital = None
        bktoption.transaction_fee = None
        bktoption.open_price = None

    def calculate_drawdown(self):
        hist_max = 1.0
        for (idx, row) in self.df_account.iterrows():
            if idx == 0:
                drawdown = 0.0
            else:
                npv = row[self.util.npv]
                hist_max = max(npv, hist_max)
                drawdown = -(hist_max - npv) / hist_max
            self.df_account.loc[idx, 'drawdown'] = drawdown

    def calculate_max_drawdown(self):
        self.calculate_drawdown()
        max_drawdown = None
        try:
            drawdown_list = self.df_account['drawdown']
            max_drawdown = min(drawdown_list)
        except Exception as e:
            print(e)
            pass
        return max_drawdown

    def calculate_annulized_return(self):
        dt_start = self.df_account.loc[0, self.util.dt_date]
        dt_end = self.df_account.loc[len(self.df_account) - 1, self.util.dt_date]
        invest_days = (dt_end - dt_start).days
        annulized_return = (self.total_asset / self.init_fund) ** (365 / invest_days) - 1
        print('annulized_return',annulized_return)
        netvalue = self.df_account[self.util.npv]
        tradeslen = len(netvalue)
        # 累计收益率
        totalreturn = netvalue.iloc[-1] - 1
        # 年化收益率
        return_yr = (1 + totalreturn) ** (252.0 / tradeslen) - 1
        self.annualized_return = return_yr
        return return_yr

    def calculate_sharpe_ratio(self):
        df = self.df_account
        netvalue = df[self.util.npv]
        tradeslen = len(netvalue)
        tmp = netvalue.shift()
        tmp[0] = 1
        returns = netvalue / tmp - 1
        # 累计收益率
        totalreturn = netvalue.iloc[-1] - 1
        # 年化收益率
        return_yr = (1 + totalreturn) ** (252.0 / tradeslen) - 1
        # 年化波动率
        volatility_yr = np.std(returns, ddof=0) * np.sqrt(252.0)
        print('volatility_yr',volatility_yr)

        # 夏普比率
        sharpe = (return_yr - 0.024) / volatility_yr
        return sharpe

    def hisvol(self, data, n):
        datas = np.log(data)
        df = datas.diff()
        vol = df.std() * np.sqrt(252)
        return vol

    def plot_npv(self):
        fig = plt.figure()
        host = host_subplot(111)
        par = host.twinx()
        host.set_xlabel("日期")
        x = self.df_account[self.util.dt_date].tolist()
        npv = self.df_account[self.util.npv].tolist()
        dd = self.df_account['drawdown'].tolist()
        host.plot(x, npv, label='npv', color=self.pu.colors[0], linestyle=self.pu.lines[0], linewidth=2)
        par.fill_between(x, [0] * len(dd), dd, label='drawdown', color=self.pu.colors[1])
        host.set_ylabel('Net Value')
        par.set_ylabel('Drawdown')
        host.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
                    ncol=3, mode="expand", borderaxespad=0., frameon=False)
        fig.savefig('../save_figure/npv.png', dpi=300, format='png')
        plt.show()
