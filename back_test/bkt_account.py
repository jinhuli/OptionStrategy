import pandas as pd
import datetime
import math
from back_test.bkt_util import BktUtil
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import host_subplot
from Utilities.PlotUtil import PlotUtil
import numpy as np

class BktAccount(object):
    def __init__(self,cd_open_price='close',cd_close_price = 'close', leverage=1.0, margin_rate=0.1, init_fund=1000000.0, tick_size=0.0001,
                 contract_multiplier=10000, fee_rate=2.0 / 10000, nbr_slippage=0,rf = 0.03):

        self.util = BktUtil()
        self.init_fund = init_fund
        self.fee = fee_rate
        self.cash = init_fund
        self.total_asset = init_fund
        self.rf = rf
        self.cd_open_by_price = cd_open_price
        self.cd_close_by_price = cd_close_price
        self.contract_multiplier=contract_multiplier
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
        self.holdings = []  # 当前持仓
        self.pu = PlotUtil()
        self.trade_order_dict = []


    def  open_long(self, dt, unit=None, bktoption=None, trade_order_dict=None):
        if bktoption != None:
            self.open_long_option(dt,bktoption,unit)
        if trade_order_dict != None:
            self.option_long_1(trade_order_dict)


    def get_open_position_price(self,bktoption):
        if self.cd_open_by_price == 'open':
            mkt_price = bktoption.option_price_open
        elif self.cd_open_by_price == 'close':
            mkt_price = bktoption.option_price
        elif self.cd_open_by_price == 'open_15min':
            mkt_price = bktoption.option_price_open_15min
        elif self.cd_open_by_price == 'close_15min':
            mkt_price = bktoption.option_price_close_15min
        else:
            mkt_price = bktoption.option_price
        return mkt_price

    def get_close_position_price(self,bktoption):
        if self.cd_open_by_price == 'open':
            mkt_price = bktoption.option_price_open
        elif self.cd_open_by_price == 'close':
            mkt_price = bktoption.option_price
        elif self.cd_open_by_price == 'open_15min':
            mkt_price = bktoption.option_price_open_15min
        elif self.cd_open_by_price == 'close_15min':
            mkt_price = bktoption.option_price_close_15min
        else:
            mkt_price = bktoption.option_price
        return mkt_price

    def option_long_1(self,trade_order_dict):
        mkt_price = float(trade_order_dict['price'])
        unit = float(trade_order_dict['unit'])
        id_instrument = trade_order_dict['id_instrument']
        dt = trade_order_dict['dt_date']
        premium = unit * mkt_price
        fee = premium * self.fee
        trade_type = 'open'
        self.cash = self.cash - mkt_price*unit - fee
        self.nbr_trade += 1
        trade_order_dict['open_transaction_fee'] = fee
        trade_order_dict['long_short'] = 1
        if trade_order_dict not in self.holdings: self.holdings.append(trade_order_dict)
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

    def open_long_option(self, dt, bktoption, unit):  # 多开
        bktoption.trade_dt_open = dt
        bktoption.trade_long_short = self.util.long
        id_instrument = bktoption.id_instrument
        mkt_price = self.get_open_position_price(bktoption)
        multiplier = bktoption.multiplier
        trade_type = '多开'
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
        self.cash = self.cash - premium - margin_capital
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

    def open_short(self, dt, bktoption, unit):
        bktoption.trade_dt_open = dt
        bktoption.trade_long_short = self.util.short
        id_instrument = bktoption.id_instrument
        mkt_price = self.get_open_position_price(bktoption)
        multiplier = bktoption.multiplier
        trade_type = '空开'
        fee = unit * mkt_price * self.fee * multiplier
        premium = unit * mkt_price * multiplier
        margin_capital = unit * bktoption.get_init_margin()
        bktoption.trade_unit = unit
        bktoption.premium = premium
        bktoption.trade_open_price = mkt_price
        bktoption.trade_margin_capital = margin_capital
        bktoption.transaction_fee = fee
        bktoption.trade_flag_open = True
        if bktoption not in self.holdings: self.holdings.append(bktoption)
        self.cash = self.cash + premium - margin_capital
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

    def close_position(self,dt,position_obj):
        if position_obj != dict:
            self.close_position_option(dt,position_obj)
        else:
            self.close_position_1(position_obj)

    def close_position_1(self,trade_order_dict):
        position = pd.Series()
        mkt_price = float(trade_order_dict['price'])
        unit = self.trade_order_dict['unit']
        id_instrument = trade_order_dict['id_instrument']
        dt = trade_order_dict['dt_date']
        long_short = self.trade_order_dict['long_short']
        open_price = self.trade_order_dict['price']
        open_transaction_fee = self.trade_order_dict['open_transaction_fee']
        self.cash = self.cash + mkt_price*unit
        trade_type = 'close'
        fee = mkt_price*unit*self.fee
        realized_pnl = long_short * (
            unit * (mkt_price - open_price)) -open_transaction_fee - fee
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
                                    'premium paid': [- mkt_price*unit],
                                    'cash': [self.cash],
                                    'margin capital': [self.total_margin_capital]
                                    })
        self.nbr_trade += 1
        self.realized_pnl += realized_pnl
        self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)
        self.liquidate_option(trade_order_dict=trade_order_dict)

    def close_position_option(self, dt, bktoption):  # 多空平仓
        if bktoption.trade_flag_open:
            id_instrument = bktoption.id_instrument
            mkt_price = self.get_close_position_price(bktoption)
            unit = bktoption.trade_unit
            long_short = bktoption.trade_long_short
            margin_capital = bktoption.trade_margin_capital
            dt_open = bktoption.trade_dt_open
            multiplier = bktoption.multiplier
            premium_open = bktoption.premium
            open_price = bktoption.trade_open_price
            premium = unit * mkt_price * multiplier
            position = pd.Series()
            position[self.util.id_instrument] = id_instrument
            position[self.util.dt_open] = dt_open
            position[self.util.long_short] = long_short
            position[self.util.open_price] = open_price
            position[self.util.unit] = unit
            position[self.util.margin_capital] = margin_capital
            position[self.util.flag_open] = False
            position[self.util.multiplier] = multiplier
            if long_short == self.util.long:
                trade_type = '多平'
            else:
                trade_type = '空平'
            fee = unit * mkt_price * self.fee * multiplier
            realized_pnl = long_short * (
                unit * mkt_price * multiplier - bktoption.premium) - bktoption.transaction_fee - fee
            premium_to_cash = long_short * premium_open
            position[self.util.dt_close] = dt
            position[self.util.days_holding] = (dt - dt_open).days
            position[self.util.close_price] = mkt_price
            position[self.util.realized_pnl] = realized_pnl
            self.df_trading_book = self.df_trading_book.append(position, ignore_index=True)
            self.cash = self.cash + margin_capital + realized_pnl + premium_to_cash
            self.total_margin_capital -= margin_capital
            self.total_transaction_cost += fee
            self.nbr_trade += 1
            self.realized_pnl += realized_pnl
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

    def rebalance_position(self, dt, unit=None ,bktoption=None,trade_order_dict=None):
        if bktoption != None:
            self.rebalance_position_option(dt, bktoption, unit)
        elif trade_order_dict != None:
            self.rebalance_position_1(dt,trade_order_dict)

    def rebalance_position_1(self,dt,unit,trade_order_dict):
        holding_unit = self.trade_order_dict['unit']
        open_price = self.trade_order_dict['price']
        long_short = self.trade_order_dict['long_short']
        id_instrument = self.trade_order_dict['id_instrument']
        mkt_price = trade_order_dict['price']

        if unit != holding_unit:
            if unit > holding_unit:  # 加仓
                open_price = ((unit - holding_unit) * mkt_price + holding_unit * open_price) / unit  # 加权开仓价格
                market_value = (unit - holding_unit) * mkt_price
                fee = market_value * self.fee
                self.trade_order_dict['open_transaction_fee'] += fee
                self.cash -= long_short *market_value
                self.total_transaction_cost += fee

            else:  # 减仓
                liquidated_unit = holding_unit - unit
                market_value = liquidated_unit * mkt_price
                fee = market_value * self.fee
                d_fee = self.trade_order_dict['open_transaction_fee'] * liquidated_unit / holding_unit
                realized_pnl = long_short * (liquidated_unit * mkt_price - market_value) - fee - d_fee
                self.trade_order_dict['open_transaction_fee'] -= d_fee

                self.realized_pnl += realized_pnl
                self.cash = self.cash + long_short*market_value + realized_pnl
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

    def rebalance_position_option(self, dt, unit, bktoption):

        id_instrument = bktoption.id_instrument
        mkt_price = self.get_close_position_price(bktoption)
        holding_unit = bktoption.trade_unit
        long_short = bktoption.trade_long_short
        open_price = bktoption.trade_open_price
        multiplier = bktoption.multiplier
        premium = bktoption.premium
        if unit != holding_unit:
            if unit > holding_unit:  # 加仓
                margin_add = (unit - holding_unit) * bktoption.get_init_margin()
                open_price = ((unit - holding_unit) * mkt_price + holding_unit * open_price) / unit  # 加权开仓价格
                premium_add = (unit - holding_unit) * mkt_price * multiplier
                fee = premium_add * self.fee
                bktoption.transaction_fee += fee
                bktoption.trade_margin_capital += margin_add
                premium += premium_add
                premium_paid = long_short * premium_add
                self.cash = self.cash - margin_add - premium_paid
                self.total_margin_capital += margin_add
                self.total_transaction_cost += fee
            else:  # 减仓
                liquidated_unit = holding_unit - unit
                margin_returned = liquidated_unit * bktoption.trade_margin_capital / bktoption.trade_unit
                premium_liquidated = liquidated_unit * mkt_price * multiplier
                fee = premium_liquidated * self.fee
                d_fee = bktoption.transaction_fee * liquidated_unit / holding_unit
                realized_pnl = long_short * (liquidated_unit * multiplier * mkt_price
                                             - premium_liquidated) - fee - d_fee
                premium -= premium_liquidated
                premium_paid = -long_short * premium_liquidated
                bktoption.transaction_fee -= d_fee
                self.realized_pnl += realized_pnl
                self.cash = self.cash + margin_returned + realized_pnl
                self.total_margin_capital -= margin_returned
                self.total_transaction_cost += fee
            bktoption.trade_unit = unit
            bktoption.premium = premium
            bktoption.trade_open_price = open_price
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
                                        'premium paid': [premium_paid],
                                        'cash': [self.cash],
                                        'margin capital': [self.total_margin_capital]
                                        })
            self.df_trading_records = self.df_trading_records.append(record, ignore_index=True)
            self.nbr_trade += 1

    def liquidate_option(self,bktoption=None,trade_order_dict=None):
        if bktoption != None:
            bktoption.trade_flag_open = False
            bktoption.trade_unit = None
            bktoption.trade_dt_open = None
            bktoption.trade_long_short = None
            bktoption.premium = None
            bktoption.trade_open_price = None
            bktoption.trade_margin_capital = None
            bktoption.transaction_fee = None
            bktoption.open_price = None
        if trade_order_dict != None:
            self.holdings.remove(trade_order_dict)



    def switch_long(self):
        return None

    def switch_short(self):
        return None

    def mkm_update(self, dt, trade_order_dict=None):  # 每日更新
        unrealized_pnl = 0
        mtm_portfolio_value = 0
        mtm_long_positions = 0
        mtm_short_positions = 0
        total_premium_paied = 0
        holdings = []
        self.cash = self.cash*(1+(1.0/365)*self.rf)
        port_delta = 0.0
        for bktoption in self.holdings:
            if type(bktoption) != dict:
                if not bktoption.trade_flag_open: continue
                holdings.append(bktoption)
                mkt_price = bktoption.option_price
                unit = bktoption.trade_unit
                long_short = bktoption.trade_long_short
                margin_account = bktoption.trade_margin_capital
                multiplier = bktoption.multiplier
                premium = bktoption.premium
                maintain_margin = unit * bktoption.get_maintain_margin()
                margin_call = maintain_margin - margin_account
                unrealized_pnl += long_short * (mkt_price * unit * multiplier - premium)
                if long_short == self.util.long:
                    mtm_long_positions += mkt_price * unit * multiplier
                    total_premium_paied += bktoption.premium
                    port_delta += unit * multiplier * bktoption.get_delta() / self.contract_multiplier
                else:
                    mtm_short_positions -= mkt_price * unit * multiplier
                    port_delta -= unit * multiplier * bktoption.get_delta() / self.contract_multiplier
                mtm_portfolio_value += mtm_long_positions + mtm_short_positions
                bktoption.trade_margin_capital = maintain_margin
                self.cash -= margin_call
                self.total_margin_capital += margin_call
            else:
                long_short = self.trade_order_dict['long_short']
                unit = self.trade_order_dict['unit']
                mkt_price = trade_order_dict['price']
                if long_short == self.util.long:
                    mtm_long_positions += mkt_price * unit
                else:
                    mtm_short_positions -= mkt_price * unit


        money_utilization = self.total_margin_capital / (self.total_margin_capital + self.cash)
        self.total_asset = self.cash + self.total_margin_capital + mtm_long_positions + mtm_short_positions
        self.npv = self.total_asset / self.init_fund
        self.holdings = holdings
        account = pd.DataFrame(data={self.util.dt_date: [dt],
                                     self.util.nbr_trade: [self.nbr_trade],
                                     self.util.margin_capital: [self.total_margin_capital],
                                     self.util.realized_pnl: [self.realized_pnl],
                                     self.util.unrealized_pnl: [unrealized_pnl],
                                     self.util.mtm_long_positions: [mtm_long_positions],
                                     self.util.mtm_short_positions: [mtm_short_positions],
                                     self.util.cash: [self.cash],
                                     self.util.money_utilization: [money_utilization],
                                     self.util.npv: [self.npv],
                                     self.util.total_asset: [self.total_asset],
                                     'portfolio delta':[port_delta]
                                     }
                               )
        self.df_account = self.df_account.append(account, ignore_index=True)
        # npv_last = self.npv
        self.nbr_trade = 0
        self.realized_pnl = 0

    def liquidate_all(self, dt):
        holdings = self.holdings.copy()
        for bktoption in holdings:
            self.close_position(dt, bktoption)

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
        self.annualized_return = annulized_return
        return annulized_return

    def calculate_sharpe_ratio(self):
        df = self.df_account
        npvs = df[self.util.npv]
        std = self.hisvol(npvs,1)
        sharp = (self.annualized_return-self.rf)/std
        return sharp

    def hisvol(self,data, n):
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
