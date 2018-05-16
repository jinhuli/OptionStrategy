from back_test.bkt_strategy import BktOptionStrategy
from back_test.bkt_account import BktAccount
import QuantLib as ql
import numpy as np
import datetime
from back_test.bkt_util import BktUtil
from back_test.data_option import get_50option_mktdata2 as get_mktdata, get_eventsdata, get_50etf_mktdata
from back_test.OptionPortfolio import *
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd


class BktStrategyEventVol(BktOptionStrategy):

    def __init__(self, df_option_metrics, df_events, option_invest_pct=0.1):

        BktOptionStrategy.__init__(self, df_option_metrics,rf = 0.03)
        self.df_events = df_events.sort_values(by='dt_impact_beg', ascending=True).reset_index()
        self.moneyness = 0
        self.option_invest_pct = option_invest_pct
        self.cash_reserve_pct = 0.05
        self.delta_neutral = False
        self.portfolio = None

    def options_run(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        # bkt2 = BktAccount()
        idx_event = 0
        print(self.df_events)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            # dt_event = self.df_events.loc[idx_event, 'dt_impact_beg']
            dt_event = self.df_events.loc[idx_event, 'dt_test']
            # dt_volpeak = self.df_events.loc[idx_event, 'dt_vol_peak']
            dt_volpeak = self.df_events.loc[idx_event, 'dt_test2']
            cd_trade_deriction = self.df_events.loc[idx_event, 'cd_trade_direction']
            cd_open_position_time = self.df_events.loc[idx_event, 'cd_open_position_time']
            cd_close_position_time = self.df_events.loc[idx_event, 'cd_close_position_time']
            cd_event = self.df_events.loc[idx_event, 'cd_occurrence']
            # cd_open_position_time = 'morning_open_15min'
            # cd_close_position_time = 'close'

            # cd_close_position_time = 'daily_avg'
            # cd_close_position_time = None


            evalDate = bkt_optionset.eval_date

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break

            """Option: Open position on event day, close on vol peak day"""
            if evalDate == dt_event:
                print(idx_event, ' ', evalDate, ' open position')

                if self.flag_trade:
                    print(evalDate, ' Trying to open position before previous one closed!')
                    return
                cash_for_option = (1 - self.cash_reserve_pct) * bkt.cash

                """Option: Select Strategy and Open Position"""
                cd_underlying_price = 'open'
                delta_exposure = 0.0
                if cd_close_position_time == 'afternoon_close_15min': cd_underlying_price = 'close'
                if cd_event == 'e':
                    portfolio = self.bkt_optionset.get_straddle(
                        self.moneyness, self.get_1st_eligible_maturity(evalDate),delta_exposure,
                        cd_underlying_price=cd_underlying_price)
                else:
                    if cd_trade_deriction == -1:
                        delta_exposure = -0.1
                        # portfolio = self.bkt_optionset.get_put(
                        #     self.moneyness,self.get_1st_eligible_maturity(evalDate), self.util.long, cd_underlying_price)
                        portfolio = self.bkt_optionset.get_call(
                            self.moneyness, self.get_1st_eligible_maturity(evalDate), self.util.short,
                            cd_underlying_price)
                    else:
                        delta_exposure = 0.1
                        # portfolio = self.bkt_optionset.get_call(
                        #     self.moneyness, self.get_1st_eligible_maturity(evalDate), self.util.long, cd_underlying_price)
                        portfolio = self.bkt_optionset.get_put(
                            self.moneyness, self.get_1st_eligible_maturity(evalDate), self.util.short,
                            cd_underlying_price)

                # portfolio = self.bkt_optionset.get_straddle(
                #     self.moneyness, self.get_1st_eligible_maturity(evalDate),delta_exposure,
                #     cd_underlying_price=cd_underlying_price)

                # if cd_trade_deriction == 1:
                #     option_type = self.util.type_call
                # else:
                #     option_type = self.util.type_put
                # # option_type = self.util.type_put
                # portfolio = self.bkt_optionset.get_backspread(option_type,self.get_1st_eligible_maturity(evalDate),
                #     cd_underlying_price=cd_underlying_price,moneyness1=0,moneyness2=-2)

                print(portfolio.optionset[0].id_instrument,portfolio.optionset[0].dt_date,portfolio.optionset[0].underlying_price)
                # print(portfolio.optionset[1].id_instrument,portfolio.optionset[1].dt_date,portfolio.optionset[1].underlying_price)
                # mdt1 = self.get_1st_eligible_maturity(evalDate)
                # mdt2 = self.get_2nd_eligible_maturity(evalDate)
                # portfolio = self.bkt_optionset.get_calendar_spread_long(self.moneyness, mdt1, mdt2,
                #                                                                option_type=self.util.type_put)
                # self.delta_neutral = True
                self.portfolio = portfolio
                self.bkt_account.update_invest_units(portfolio, self.util.long,delta_exposure,
                                                     cd_open_by_price=cd_open_position_time,fund=cash_for_option)
                self.bkt_account.open_position(evalDate, portfolio, cd_open_by_price=cd_open_position_time)
                self.flag_trade = True
            elif evalDate != dt_volpeak:
                if self.flag_trade and self.bkt_account.holdings != []:
                    # if isinstance(self.portfolio, Straddle) or isinstance(self.portfolio, BackSpread):
                    #     """ Delta neutral rebalancing """
                    #     self.bkt_account.update_invest_units(self.portfolio, self.util.long, delta_exposure)
                    #     self.bkt_account.rebalance_position(evalDate, self.portfolio)

                    morming_npv = self.bkt_account.calculate_nvp(evalDate)
                    if cd_event == 's' and (morming_npv - self.bkt_account.npv) / self.bkt_account.npv <= -0.01:
                        idx_event += 1
                        if self.flag_trade:
                            print(idx_event, ' ', evalDate, ' close position by NPV')
                            """ Close position by NPV"""
                            self.flag_trade = False
                            cd_close_position_time = 'amt_afternoon_avg'
                            for bktoption in bkt.holdings:
                                self.bkt_account.close_position(evalDate, bktoption,
                                                                cd_close_by_price=cd_close_position_time)
                    else:
                        if isinstance(self.portfolio, Straddle) or isinstance(self.portfolio, BackSpread):
                            """ Delta neutral rebalancing """
                            self.bkt_account.update_invest_units(self.portfolio, self.util.long, delta_exposure)
                            self.bkt_account.rebalance_position(evalDate, self.portfolio)



            # if self.flag_trade and self.bkt_account.holdings != []:
            #     if self.bkt_account.total_margin_capital != self.bkt_account.holdings[1].trade_margin_capital:
            #         print("hello")

            if evalDate >= dt_volpeak:
                idx_event += 1
                if self.flag_trade:
                    print(idx_event, ' ', evalDate, ' close position')
                    """ Close position"""
                    self.flag_trade = False
                    for bktoption in bkt.holdings:
                        self.bkt_account.close_position(evalDate, bktoption, cd_close_by_price=cd_close_position_time)

            """按当日价格调整保证金，计算投资组合盯市价值"""
            self.bkt_account.mkm_update(evalDate)

            # if self.flag_trade and self.bkt_account.holdings != []:
            #     if self.bkt_account.total_margin_capital != self.bkt_account.holdings[1].trade_margin_capital:
            #         print("hello")
            print(evalDate, bkt_optionset.eval_date, ' , ', bkt.npv,bkt.port_delta)
            bkt_optionset.next()
            if idx_event >= len(self.df_events): break

    def ivs_ranked_run(self):
        bkt_optionset = self.bkt_optionset
        df_ivs = pd.DataFrame()
        df_skew = pd.DataFrame()
        while bkt_optionset.index < len(bkt_optionset.dt_list) - 1:
            evalDate = bkt_optionset.eval_date
            cd_underlying_price = 'close'
            mdt = self.get_1st_eligible_maturity(evalDate)
            option_by_moneyness = self.bkt_optionset.update_options_by_moneyness(cd_underlying_price)
            optionset = option_by_moneyness[mdt]
            options_call = optionset[self.util.type_call]
            options_put = optionset[self.util.type_put]
            m_call = list(options_call.keys())
            m_put = list(options_put.keys())
            iv_call = []
            iv_put = []
            dt = []
            mdts = []

            for m in m_call:
                iv = options_call[m].get_implied_vol()
                iv_call.append(iv)
                dt.append(evalDate)
                mdts.append(mdt)
            for m1 in m_put:
                iv = options_put[m1].get_implied_vol()
                iv_put.append(iv)

            ivset = pd.DataFrame(data={'dt':dt,'mdt':mdt,'m_call':m_call,'m_put':m_put,
                          'iv_call':iv_call,'iv_put':iv_put})
            df_ivcall = ivset[ivset['m_call']<=0].sort_values(by='m_call',ascending=False).reset_index(drop=True).query('index <= 4')
            if len(df_ivcall) <= 1:
                otm_skew_call = np.nan
            else:
                df_diffcall = df_ivcall['iv_call'].diff()
                otm_skew_call = df_diffcall.sum()/(len(df_diffcall)-1)
            df_ivput = ivset[ivset['m_put']<=0].sort_values(by='m_put',ascending=False).reset_index(drop=True).query('index <= 4')
            if len(df_ivput) <= 1:
                otm_skew_put = np.nan
            else:
                df_diffput = df_ivput['iv_put'].diff()
                otm_skew_put = df_diffput.sum()/(len(df_diffput)-1)
            ivskew = pd.DataFrame(data={'dt':[evalDate],'mdt':[mdt],'otm_skew_call':[otm_skew_call],
                                        'otm_skew_put':[otm_skew_put]})
            df_skew = df_skew.append(ivskew,ignore_index=True)
            bkt_optionset.next()
        df_skew.to_csv('../save_results/df_skew_otm.csv')

    def ivs_run(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        # bkt2 = BktAccount()
        idx_event = 0
        print(self.df_events)
        df_ivs = pd.DataFrame()
        while bkt_optionset.index < len(bkt_optionset.dt_list)-1:

            evalDate = bkt_optionset.eval_date
            cd_underlying_price = 'close'
            option_by_moneyness = self.bkt_optionset.update_options_by_moneyness(cd_underlying_price)

            call_atm = self.bkt_optionset.get_call(
                0, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            put_atm = self.bkt_optionset.get_put(
                0, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()

            try:
                call_itm = self.bkt_optionset.get_call(
                    2, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                call_itm = np.nan
            try:
                call_itm1 = self.bkt_optionset.get_call(
                    1, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                call_itm1 = np.nan
            try:
                call_otm = self.bkt_optionset.get_call(
                    -2, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                call_otm = np.nan
            try:
                call_otm1 = self.bkt_optionset.get_call(
                    -1, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                call_otm1 = np.nan
            try:
                put_itm = self.bkt_optionset.get_put(
                    2, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                put_itm = np.nan
            try:
                put_itm1 = self.bkt_optionset.get_put(
                    1, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                put_itm1 = np.nan
            try:
                put_otm = self.bkt_optionset.get_put(
                    -2, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                put_otm = np.nan
            try:
                put_otm1 = self.bkt_optionset.get_put(
                    -1, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0].get_implied_vol()
            except:
                put_otm1 = np.nan
            iv = pd.DataFrame(data={'dt': [evalDate],
                                    'call_atm': [call_atm],
                                    'call_itm': [call_itm],
                                    'call_itm 1': [call_itm1],
                                    'call_otm': [call_otm],
                                    'call_otm 1': [call_otm1],
                                    'put_atm': [put_atm],
                                    'put_itm': [put_itm],
                                    'put_itm 1': [put_itm1],
                                    'put_otm': [put_otm],
                                    'put_otm 1': [put_otm1]
                                    })
            print(iv)
            df_ivs = df_ivs.append(iv,ignore_index=True)
            bkt_optionset.next()
        df_ivs.to_csv('../save_results/df_ivs_total.csv')

    def options_straddle_etf(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        bkt2 = BktAccount()
        idx_event = 0
        print(self.df_events)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            dt_event = self.df_events.loc[idx_event, 'dt_impact_beg']
            dt_volpeak = self.df_events.loc[idx_event, 'dt_vol_peak']
            cd_trade_deriction = self.df_events.loc[idx_event, 'cd_trade_direction']
            cd_open_position_time = self.df_events.loc[idx_event, 'cd_open_position_time']
            cd_close_position_time = self.df_events.loc[idx_event, 'cd_close_position_time']

            evalDate = bkt_optionset.eval_date
            df_metrics_today = bkt_optionset.df_daily_state

            """ 50ETF仓位: track index """
            etf_price = df_metrics_today.loc[0, self.util.col_underlying_price]
            trade_order_dict = {'id_instrument': 'index_50etf', 'dt_date': evalDate, 'price': etf_price }
            trade_order_dict2 = { 'id_instrument': 'index_50etf', 'dt_date': evalDate, 'price': etf_price}

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate, trade_order_dict)
                bkt2.liquidate_all(evalDate)
                bkt2.mkm_update(evalDate, trade_order_dict2)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break

            """ 开仓 50ETF """
            if bkt_optionset.index == 0:
                fund_etf = bkt.cash * (1 - self.option_invest_pct - self.cash_reserve_pct) * (1 - bkt.fee)
                unit = np.floor(fund_etf / etf_price)
                trade_order_dict.update({'unit': unit})
                bkt.open_long(evalDate, trade_order_dict)
                fund_etf2 = bkt2.cash * (1 - self.cash_reserve_pct) * (1 - bkt2.fee)
                unit2 = np.floor(fund_etf2 / etf_price)
                trade_order_dict2.update({'unit': unit2})
                bkt2.open_long(evalDate, trade_order_dict2)
            else:  # """ 根据投资比例调整50ETF仓位"""
                etf_invest = bkt.trade_order_mktv / bkt.total_asset
                etf_ratio = 1 - self.option_invest_pct - self.cash_reserve_pct
                if etf_invest > etf_ratio + 0.01 or etf_invest < etf_ratio - 0.01:
                    fund_etf = bkt.total_asset * (1 - self.option_invest_pct) * (1 - bkt.fee)
                    unit = np.floor(fund_etf / etf_price)
                    trade_order_dict.update({'unit': unit})
                    bkt.rebalance_position(evalDate, trade_order_dict)

            """Option: Open position on event day, close on vol peak day"""
            if evalDate == dt_event:
                print(idx_event, ' ', evalDate, ' open position')

                if self.flag_trade:
                    print(evalDate, ' Trying to open position before previous one closed!')
                    return
                cash_for_option = (1 - self.cash_reserve_pct) * bkt.cash

                """Option: Select Strategy and Open Position"""
                if cd_trade_deriction == -1:
                    portfolio = self.bkt_optionset.get_put(self.moneyness,
                                                                  self.get_1st_eligible_maturity(evalDate))  # 选择跨式期权头寸
                elif cd_trade_deriction == 1:
                    portfolio = self.bkt_optionset.get_call(self.moneyness, self.get_1st_eligible_maturity(evalDate))
                else:
                    portfolio = self.bkt_optionset.get_straddle(self.moneyness,
                                                                       self.get_1st_eligible_maturity(evalDate))
                # portfolio = self.bkt_optionset.get_straddle(self.moneyness,
                #                                             self.get_1st_eligible_maturity(evalDate))

                # self.delta_neutral = True
                self.portfolio = portfolio
                self.bkt_account.update_invest_units(portfolio, self.util.long, cd_open_position_time,
                                                     fund=cash_for_option)
                bkt.open_long(evalDate, portfolio, cd_open_by_price=cd_open_position_time)
                self.flag_trade = True
            elif evalDate != dt_volpeak:
                if self.flag_trade:
                    if isinstance(self.portfolio, Straddle):  # DELTA NEUTRAL REBALANCING
                        """ Rebalance Straddle on delta neutral """
                        self.bkt_account.update_invest_units(self.portfolio, self.util.long)
                        bkt.rebalance_position(evalDate, self.portfolio)

            if evalDate == dt_volpeak:
                idx_event += 1
                if self.flag_trade:
                    print(idx_event, ' ', evalDate, ' close position')
                    """ Close position"""
                    self.flag_trade = False
                    self.delta_neutral = False
                    for bktoption in bkt.holdings:
                        bkt.close_position(evalDate, bktoption, cd_close_by_price=cd_close_position_time)

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt.mkm_update(evalDate, trade_order_dict=trade_order_dict)
            bkt2.mkm_update(evalDate, trade_order_dict=trade_order_dict2)
            print(evalDate, bkt_optionset.eval_date, ' , ', bkt.npv)  # npv是组合净值，期初为1
            bkt_optionset.next()
            if idx_event >= len(self.df_events): break
            self.bkt_account1 = bkt
            self.bkt_account2 = bkt2

    def options_calendar_spread(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        idx_event = 0
        print(self.df_events)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            dt_event = self.df_events.loc[idx_event, 'dt_impact_beg']
            dt_volpeak = self.df_events.loc[idx_event, 'dt_vol_peak']
            cd_trade_deriction = self.df_events.loc[idx_event, 'cd_trade_direction']
            cd_open_position_time = self.df_events.loc[idx_event, 'cd_open_position_time']
            evalDate = bkt_optionset.eval_date

            df_metrics_today = bkt_optionset.df_daily_state

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break

            """Option: Open position on event day, close on vol peak day"""
            cash_for_option = (1 - self.cash_reserve_pct) * bkt.cash
            if evalDate == dt_event:
                print(idx_event, ' ', evalDate, ' open position')

                if self.flag_trade:
                    print(evalDate, ' Trying to open position before previous one closed!')
                    return

                """Option: Select Strategy and Open Position"""
                mdt1 = self.get_1st_eligible_maturity(evalDate)
                mdt2 = self.get_2nd_eligible_maturity(evalDate)
                if cd_trade_deriction == -1:
                    df_open_position = self.bkt_optionset.get_calendar_spread_long(self.moneyness, mdt1, mdt2,
                                                                                   option_type=self.util.type_put)
                elif cd_trade_deriction == 1:
                    df_open_position = self.bkt_optionset.get_calendar_spread_long(self.moneyness, mdt1, mdt2,
                                                                                   option_type=self.util.type_call)
                else:
                    df_open_position = self.bkt_optionset.get_calendar_spread_long(self.moneyness, mdt1, mdt2)
                fund = 0
                for (idx, row) in df_open_position.iterrows():
                    bktoption = row[self.util.bktoption]
                    delta0_ratio = row[self.util.unit]
                    margin = bktoption.get_init_margin()  # 每手初始保证金
                    fund += margin * delta0_ratio
                unit = cash_for_option / fund
                # delta = 0
                for (idx, row) in df_open_position.iterrows():
                    bktoption = row[self.util.bktoption]
                    delta0_ratio = row[self.util.unit]
                    trade_unit = np.floor(delta0_ratio * unit)
                    # delta += bktoption.get_delta()*unit*delta0_ratio
                    if trade_unit > 0:
                        bkt.open_long(evalDate, unit=trade_unit, bktoption=bktoption)
                    else:
                        bkt.open_short(evalDate, unit=-trade_unit, bktoption=bktoption)

                    # self.holdings_mdt = bktoption.maturitydt
                self.flag_trade = True

            """Option: Close position """
            if evalDate == dt_volpeak:
                idx_event += 1
                if self.flag_trade:
                    print(idx_event, ' ', evalDate, ' close position')

                    self.flag_trade = False
                    for bktoption in bkt.holdings:
                        bkt.close_position(evalDate, bktoption)

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt.mkm_update(evalDate)
            print(evalDate, bkt_optionset.eval_date, ' , ', bkt.npv)  # npv是组合净值，期初为1
            bkt_optionset.next()
            if idx_event >= len(self.df_events): break
        self.bkt_account1 = bkt

    def etf_enhanced_by_options(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        bkt2 = BktAccount()
        idx_event = 0
        print(self.df_events)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            dt_event = self.df_events.loc[idx_event, 'dt_impact_beg']
            dt_volpeak = self.df_events.loc[idx_event, 'dt_vol_peak']
            cd_trade_deriction = self.df_events.loc[idx_event, 'cd_trade_direction']
            evalDate = bkt_optionset.eval_date

            df_metrics_today = bkt_optionset.df_daily_state

            """ 50ETF仓位: track index """
            etf_price = df_metrics_today.loc[0, self.util.col_underlying_price]
            trade_order_dict = {
                'id_instrument': 'index_50etf',
                'dt_date': evalDate,
                'price': etf_price,
            }
            trade_order_dict2 = {
                'id_instrument': 'index_50etf',
                'dt_date': evalDate,
                'price': etf_price}

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate, trade_order_dict)
                bkt2.liquidate_all(evalDate)
                bkt2.mkm_update(evalDate, trade_order_dict2)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break
            # """ 开仓 50ETF """
            if bkt_optionset.index == 0:
                fund_etf = bkt.cash * (1 - self.option_invest_pct - self.cash_reserve_pct) * (1 - bkt.fee)
                unit = np.floor(fund_etf / etf_price)
                trade_order_dict.update({'unit': unit})
                bkt.open_long(evalDate, trade_order_dict=trade_order_dict)
                fund_etf2 = bkt2.cash * (1 - self.cash_reserve_pct) * (1 - bkt2.fee)
                unit2 = np.floor(fund_etf2 / etf_price)
                trade_order_dict2.update({'unit': unit2})
                bkt2.open_long(evalDate, trade_order_dict=trade_order_dict2)
            else:  # """ 根据投资比例调整50ETF仓位"""
                etf_invest = bkt.margin_trade_order / bkt.total_asset
                etf_ratio = 1 - self.option_invest_pct - self.cash_reserve_pct
                if etf_invest > etf_ratio + 0.01 or etf_invest < etf_ratio - 0.01:
                    fund_etf = bkt.total_asset * (1 - self.option_invest_pct) * (1 - bkt.fee)
                    unit = np.floor(fund_etf / etf_price)
                    trade_order_dict.update({'unit': unit})
                    bkt.rebalance_position(evalDate, trade_order_dict=trade_order_dict)

            """Option: Open position on event day, close on vol peak day"""
            cash_for_option = (1 - self.cash_reserve_pct) * bkt.cash
            if evalDate == dt_event:
                print(idx_event, ' ', evalDate, ' open position')

                if self.flag_trade:
                    print(evalDate, ' Trying to open position before previous one closed!')
                    return

                """Option: Select Strategy and Open Position"""

                if cd_trade_deriction == -1:
                    df_open_position = self.bkt_optionset.get_put(self.moneyness,
                                                                  self.get_1st_eligible_maturity(evalDate))  # 选择跨式期权头寸
                elif cd_trade_deriction == 1:
                    df_open_position = self.bkt_optionset.get_call(self.moneyness,
                                                                   self.get_1st_eligible_maturity(evalDate))
                else:
                    df_open_position = self.bkt_optionset.get_straddle(self.moneyness,
                                                                       self.get_1st_eligible_maturity(evalDate))
                fund = 0
                for (idx, row) in df_open_position.iterrows():
                    bktoption = row[self.util.bktoption]
                    delta0_ratio = row[self.util.unit]
                    margin = bktoption.get_init_margin()  # 每手初始保证金
                    fund += margin * delta0_ratio
                unit = cash_for_option / fund
                # delta = 0
                for (idx, row) in df_open_position.iterrows():
                    bktoption = row[self.util.bktoption]
                    delta0_ratio = row[self.util.unit]
                    trade_unit = np.floor(delta0_ratio * unit)
                    # delta += bktoption.get_delta()*unit*delta0_ratio
                    bkt.open_long(evalDate, unit=trade_unit, bktoption=bktoption)
                    self.holdings_mdt = bktoption.maturitydt
                self.flag_trade = True

            """Option: Close position """
            if evalDate == dt_volpeak:
                idx_event += 1
                if self.flag_trade:
                    print(idx_event, ' ', evalDate, ' close position')
                    self.flag_trade = False
                    for bktoption in bkt.holdings:
                        bkt.close_position(evalDate, bktoption)

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt.mkm_update(evalDate, trade_order_dict=trade_order_dict)
            bkt2.mkm_update(evalDate, trade_order_dict=trade_order_dict2)
            print(evalDate, bkt_optionset.eval_date, ' , ', bkt.npv)  # npv是组合净值，期初为1
            bkt_optionset.next()
            if idx_event >= len(self.df_events): break
        self.bkt_account1 = bkt
        self.bkt_account2 = bkt2


"""Back Test Settings"""
# start_date = datetime.date(2017, 11, 9)
# end_date = datetime.date(2017, 12, 31)
start_date = datetime.date(2015, 3, 1)
end_date = datetime.date(2018, 5, 5)

calendar = ql.China()
daycounter = ql.ActualActual()

"""Collect Mkt Date"""
df_events = get_eventsdata(start_date, end_date,1)

df_option_metrics = get_mktdata(start_date, end_date)
# df_etf_metrics = get_50etf_mktdata(start_date,end_date)

"""Run Backtest"""

bkt_strategy = BktStrategyEventVol(df_option_metrics, df_events, option_invest_pct=0.2)
bkt_strategy.set_min_holding_days(15)

# bkt_strategy.options_straddle_etf()
# npv1 = bkt_strategy.bkt_account1.df_account['npv'].tolist()
# npv2 = bkt_strategy.bkt_account2.df_account['npv'].tolist()
#
# pu = PlotUtil()
# dates = bkt_strategy.bkt_account1.df_account['dt_date'].unique()
# f = pu.plot_line_chart(dates, [npv1,npv2], ['85% 50etf & 10% option & 5% cash','50etf'])
# plt.show()

# bkt_strategy.options_run()
# # # bkt_strategy.options_calendar_spread()

# bkt_strategy.bkt_account.df_account.to_csv('../save_results/bkt_df_account.csv')
# bkt_strategy.bkt_account.df_trading_book.to_csv('../save_results/bkt_df_trading_book.csv')
# bkt_strategy.bkt_account.df_trading_records.to_csv('../save_results/bkt_df_trading_records.csv')
# bkt_strategy.bkt_account.df_ivs.to_csv('../save_results/bkt_df_ivs.csv')
# #
# bkt_strategy.return_analysis()

bkt_strategy.ivs_ranked_run()

