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


class BktStrategyEvent(object):

    def __init__(self, df_option_metrics, event,min_holding_days, option_invest_pct=0.1):
        self.util = BktUtil()
        self.moneyness = 0
        self.option_invest_pct = option_invest_pct
        self.cash_reserve_pct = 0.05
        self.delta_neutral = False
        self.portfolio = None
        self.event = event
        self.min_holding = min_holding_days
        dt_event = event['dt_date']
        dates = df_option_metrics['dt_date'].unique().tolist()
        nbr_edate = dates.index(dt_event)
        dt_open_5d = dates[nbr_edate - 5]
        dt_open_3d = dates[nbr_edate - 3]
        dt_open_2d = dates[nbr_edate - 2]
        dt_open_1d = dates[nbr_edate - 1]
        dt_close_5d = dates[nbr_edate + 4]
        dt_close_3d = dates[nbr_edate + 2]
        dt_close_2d = dates[nbr_edate + 1]
        dt_close_1d = dates[nbr_edate]
        dt_close_b1d = dates[nbr_edate -1]
        dt_close_b2d = dates[nbr_edate -2]
        dt_beg = dates[nbr_edate - 6]
        dt_end = dates[nbr_edate + 6]
        self.df_metrics = df_option_metrics[(df_option_metrics['dt_date'] >= dt_beg) &
                                            (df_option_metrics['dt_date'] <= dt_end)].reset_index().drop('index',1)

        self.dt_opens = [dt_open_1d, dt_open_2d, dt_open_3d, dt_open_5d]
        self.dt_closes = [dt_close_b2d,dt_close_b1d,dt_close_1d, dt_close_2d, dt_close_3d, dt_close_5d]

    # def intraday_trade_run(self):
    #     dt1 = datetime.date(2018,3,23)
    #     dt2 = datetime.date(2018,4,9)
    #     dt3 = datetime.date(2018,4,17)
    #     dates = [dt1,dt2,dt3]
    #     for date in dates:
    #         dt_event = date
    #         dt_volpeak = date
    #         cd_open_position_time = 'morning_open_15min'
    #         cd_close_position_time = 'afternoon_close_15min'
    #         bkt_strategy = self.bkt_strategy
    #         bkt_optionset = bkt_strategy.bkt_optionset
    #         bkt_account = bkt_strategy.bkt_account
    #         while bkt_optionset.index < len(bkt_optionset.dt_list):
    #             dt_event = dt_open
    #             dt_volpeak = dt_close
    #             cd_trade_deriction = event['cd_trade_direction']
    #             cd_open_position_time = event['cd_open_position_time']
    #             cd_close_position_time = event['cd_close_position_time']
    #
    #             evalDate = bkt_optionset.eval_date
    #
    #             """ 回测期最后一天全部清仓 """
    #             if evalDate == bkt_optionset.end_date:
    #                 # print(' Liquidate all positions !!! ')
    #                 bkt_account.liquidate_all(evalDate)
    #                 bkt_account.mkm_update(evalDate)
    #                 # print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
    #                 break
    #
    #             """Option: Open position on event day, close on vol peak day"""
    #             if evalDate == dt_event:
    #                 # print(evalDate, ' open position')
    #
    #                 if bkt_strategy.flag_trade:
    #                     # print(evalDate, ' Trying to open position before previous one closed!')
    #                     return
    #                 cash_for_option = (1 - self.cash_reserve_pct) * bkt_account.cash
    #
    #                 """Option: Select Strategy and Open Position"""
    #                 # if cd_trade_deriction == -1:
    #                 #     portfolio = self.bkt_optionset.get_put(self.moneyness,
    #                 #                                                   self.get_1st_eligible_maturity(evalDate))  # 选择跨式期权头寸
    #                 # elif cd_trade_deriction == 1:
    #                 #     portfolio = self.bkt_optionset.get_call(self.moneyness, self.get_1st_eligible_maturity(evalDate))
    #                 # else:
    #                 #     portfolio = self.bkt_optionset.get_straddle(self.moneyness,
    #                 #                                                        self.get_1st_eligible_maturity(evalDate))
    #                 portfolio = self.bkt_optionset.get_call(0, self.get_1st_eligible_maturity(evalDate))
    #
    #                 # cd_underlying_price = 'open'
    #                 # portfolio = bkt_optionset.get_straddle(
    #                 #     self.moneyness, bkt_strategy.get_1st_eligible_maturity(evalDate),
    #                 #     cd_underlying_price=cd_underlying_price)
    #
    #                 self.portfolio = portfolio
    #                 bkt_account.update_invest_units(portfolio, self.util.long, cd_open_position_time,
    #                                                 fund=cash_for_option)
    #                 bkt_account.open_position(evalDate, portfolio, cd_open_by_price=cd_open_position_time)
    #                 bkt_strategy.flag_trade = True
    #
    #             if evalDate >= dt_volpeak:
    #                 # idx_event += 1
    #                 if bkt_strategy.flag_trade:
    #                     # print( evalDate, ' close position')
    #                     """ Close position"""
    #                     bkt_strategy.flag_trade = False
    #                     for bktoption in bkt_account.holdings:
    #                         bkt_account.close_position(evalDate, bktoption, cd_close_by_price=cd_close_position_time)
    #
    #             """按当日价格调整保证金，计算投资组合盯市价值"""
    #             bkt_account.mkm_update(evalDate)
    #             if bkt_optionset.index == len(bkt_optionset.dt_list) - 1: break
    #             bkt_optionset.next()
    #         return bkt_account.npv

    def events_run(self):
        df_res = pd.DataFrame()
        for dt_close in self.dt_closes:
            dict_opens = {}
            for dt_open in self.dt_opens:
                self.bkt_strategy = BktOptionStrategy(self.df_metrics, rf=0.0)
                self.bkt_strategy.set_min_holding_days(self.min_holding)
                npv = self.one_senario(self.event, dt_open, dt_close)
                dict_opens.update({dt_open : [(npv-1)/1.0]})
            dict_opens.update({'index':dt_close})
            row = pd.DataFrame(data=dict_opens).set_index('index')
            df_res = df_res.append(row,ignore_index=False)
        id_event = str(self.event['id_event'])
        df_res.to_csv('../save_results/id_event_'+id_event+'.csv')
        return df_res

    def one_senario(self,event,dt_open,dt_close):
        bkt_strategy = self.bkt_strategy
        bkt_optionset = bkt_strategy.bkt_optionset
        bkt_account = bkt_strategy.bkt_account
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            dt_event = dt_open
            dt_volpeak = dt_close
            cd_trade_deriction = event['cd_trade_direction']
            cd_open_position_time = event['cd_open_position_time']
            cd_close_position_time = event['cd_close_position_time']

            evalDate = bkt_optionset.eval_date

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                # print(' Liquidate all positions !!! ')
                bkt_account.liquidate_all(evalDate)
                bkt_account.mkm_update(evalDate)
                # print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
                break

            """Option: Open position on event day, close on vol peak day"""
            if evalDate == dt_event:
                # print(evalDate, ' open position')

                if bkt_strategy.flag_trade:
                    # print(evalDate, ' Trying to open position before previous one closed!')
                    return
                cash_for_option = (1 - self.cash_reserve_pct) * bkt_account.cash

                """Option: Select Strategy and Open Position"""
                # if cd_trade_deriction == -1:
                #     portfolio = self.bkt_optionset.get_put(self.moneyness,
                #                                                   self.get_1st_eligible_maturity(evalDate))  # 选择跨式期权头寸
                # elif cd_trade_deriction == 1:
                #     portfolio = self.bkt_optionset.get_call(self.moneyness, self.get_1st_eligible_maturity(evalDate))
                # else:
                #     portfolio = self.bkt_optionset.get_straddle(self.moneyness,
                #                                                        self.get_1st_eligible_maturity(evalDate))
                # portfolio = self.bkt_optionset.get_call(0, self.get_1st_eligible_maturity(evalDate))

                cd_underlying_price = 'open'
                portfolio = bkt_optionset.get_straddle(
                    self.moneyness,bkt_strategy.get_1st_eligible_maturity(evalDate),0.0,
                    cd_underlying_price=cd_underlying_price)

                # if cd_trade_deriction == 1:
                #     portfolio = bkt_optionset.get_call(
                #         self.moneyness,bkt_strategy.get_1st_eligible_maturity(evalDate),self.util.long)
                # else:
                #     portfolio = bkt_optionset.get_put(
                #         self.moneyness,bkt_strategy.get_1st_eligible_maturity(evalDate),self.util.long)

                # if cd_trade_deriction == 1:
                #     option_type = self.util.type_call
                # else:
                #     option_type = self.util.type_put
                # portfolio = bkt_optionset.get_backspread(
                #     option_type,bkt_strategy.get_1st_eligible_maturity(evalDate))


                self.portfolio = portfolio
                bkt_account.update_invest_units(portfolio, self.util.long, 0.0,
                                                cd_open_by_price=cd_open_position_time,
                                                     fund=cash_for_option)
                bkt_account.open_position(evalDate, portfolio, cd_open_by_price=cd_open_position_time)
                bkt_strategy.flag_trade = True
            elif evalDate != dt_volpeak:
                if bkt_strategy.flag_trade:
                    if isinstance(self.portfolio, Straddle) or isinstance(self.portfolio, BackSpread):
                        """ Delta neutral rebalancing """
                        bkt_account.update_invest_units(self.portfolio, self.util.long,0.0)
                        bkt_account.rebalance_position(evalDate, self.portfolio)

            if evalDate >= dt_volpeak:
                # idx_event += 1
                if bkt_strategy.flag_trade:
                    # print( evalDate, ' close position')
                    """ Close position"""
                    bkt_strategy.flag_trade = False
                    for bktoption in bkt_account.holdings:
                        bkt_account.close_position(evalDate, bktoption, cd_close_by_price=cd_close_position_time)

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update(evalDate)
            # print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv)
            if bkt_optionset.index == len(bkt_optionset.dt_list)-1 : break
            bkt_optionset.next()
        return bkt_account.npv


class BktStrategyEventS(object):

    def __init__(self, df_option_metrics, event,min_holding_days, option_invest_pct=0.1):
        self.util = BktUtil()
        self.moneyness = 0
        self.option_invest_pct = option_invest_pct
        self.cash_reserve_pct = 0.05
        self.delta_neutral = False
        self.portfolio = None
        self.event = event
        self.min_holding = min_holding_days
        dt_event = event['dt_impact_beg']
        dates = df_option_metrics['dt_date'].unique().tolist()
        nbr_edate = dates.index(dt_event)
        dt_close_6 = dates[nbr_edate + 20]
        dt_close_5 = dates[nbr_edate + 10]
        dt_close_4 = dates[nbr_edate + 5]
        dt_close_3 = dates[nbr_edate + 3]
        dt_close_2 = dates[nbr_edate + 1]
        dt_close_1 = dates[nbr_edate]
        dt_beg = dates[nbr_edate - 1]
        dt_end = dt_close_6
        self.df_metrics = df_option_metrics[(df_option_metrics['dt_date'] >= dt_beg) &
                                            (df_option_metrics['dt_date'] <= dt_end)].reset_index().drop('index',1)
        self.dt_closes = [dt_close_1,dt_close_2,dt_close_3, dt_close_4, dt_close_5, dt_close_6]

    def events_run(self):
        df_res = pd.DataFrame()
        dict_res = []
        for dt_close in self.dt_closes:
            self.bkt_strategy = BktOptionStrategy(self.df_metrics, rf=0.0)
            self.bkt_strategy.set_min_holding_days(self.min_holding)
            npv = self.one_senario(self.event, dt_close)
            r = (npv-1)/1.0
            dict_res.append({'index':dt_close,'yield':r})
        row = pd.DataFrame(data=dict_res).set_index('index')
        df_res = df_res.append(row,ignore_index=False)
        id_event = str(self.event['id_event'])
        df_res.to_csv('../save_results/id_event_'+id_event+'.csv')
        return df_res

    def one_senario(self,event,dt_close):
        bkt_strategy = self.bkt_strategy
        bkt_optionset = bkt_strategy.bkt_optionset
        bkt_account = bkt_strategy.bkt_account
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            dt_event = event['dt_impact_beg']
            dt_volpeak = dt_close
            cd_trade_deriction = event['cd_trade_direction']
            cd_open_position_time = event['cd_open_position_time']
            cd_close_position_time = event['cd_close_position_time']

            evalDate = bkt_optionset.eval_date

            """ 回测期最后一天全部清仓 """
            if evalDate == bkt_optionset.end_date:
                bkt_account.liquidate_all(evalDate)
                bkt_account.mkm_update(evalDate)
                break

            """Option: Open position on event day, close on vol peak day"""
            if evalDate == dt_event:
                # print(evalDate, ' open position')

                if bkt_strategy.flag_trade:
                    # print(evalDate, ' Trying to open position before previous one closed!')
                    return
                cash_for_option = (1 - self.cash_reserve_pct) * bkt_account.cash

                """Option: Select Strategy and Open Position"""
                # if cd_trade_deriction == 1:
                #     delta = 0.2
                # else:
                #     delta = -0.2
                delta = 0.0
                if cd_open_position_time == 'afternoon_close_15min':
                    cd_underlying_price = 'close'
                else:
                    cd_underlying_price = 'open'
                portfolio = bkt_optionset.get_straddle(
                    self.moneyness,bkt_strategy.get_1st_eligible_maturity(evalDate),delta,
                    cd_underlying_price=cd_underlying_price)
                #
                # if cd_trade_deriction == 1:
                #     portfolio = bkt_optionset.get_call(
                #         self.moneyness,bkt_strategy.get_1st_eligible_maturity(evalDate),self.util.long,
                #           cd_underlying_price=cd_underlying_price)
                # else:
                #     portfolio = bkt_optionset.get_put(
                #         self.moneyness,bkt_strategy.get_1st_eligible_maturity(evalDate),self.util.long,
                #           cd_underlying_price=cd_underlying_price)

                # if cd_trade_deriction == 1:
                #     option_type = self.util.type_call
                # else:
                #     option_type = self.util.type_put
                # portfolio = bkt_optionset.get_backspread(
                #     option_type,bkt_strategy.get_1st_eligible_maturity(evalDate),
                #     moneyness1=2,moneyness2=0,cd_underlying_price=cd_underlying_price)


                self.portfolio = portfolio
                bkt_account.update_invest_units(portfolio, self.util.long, delta,
                                                cd_open_by_price=cd_open_position_time,
                                                     fund=cash_for_option)
                bkt_account.open_position(evalDate, portfolio, cd_open_by_price=cd_open_position_time)
                bkt_strategy.flag_trade = True
            elif evalDate != dt_volpeak:
                if bkt_strategy.flag_trade:
                    if isinstance(self.portfolio, Straddle) or isinstance(self.portfolio, BackSpread):
                        """ Delta neutral rebalancing """
                        bkt_account.update_invest_units(self.portfolio, self.util.long,0.0)
                        bkt_account.rebalance_position(evalDate, self.portfolio)

            if evalDate >= dt_volpeak:
                if bkt_strategy.flag_trade:
                    """ Close position"""
                    bkt_strategy.flag_trade = False
                    for bktoption in bkt_account.holdings:
                        bkt_account.close_position(evalDate, bktoption, cd_close_by_price=cd_close_position_time)

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update(evalDate)
            if bkt_optionset.index == len(bkt_optionset.dt_list)-1 : break
            bkt_optionset.next()
        return bkt_account.npv



"""Back Test Settings"""
# start_date = datetime.date(2015, 8, 1)
start_date = datetime.date(2017, 6, 1)
end_date = datetime.date(2017, 8, 1)
# end_date = datetime.date(2018, 5, 1)

calendar = ql.China()
daycounter = ql.ActualActual()

"""Collect Mkt Date"""
df_events = get_eventsdata(start_date, end_date,1)

df_option_metrics = get_mktdata(start_date, end_date)

"""Run Backtest"""
min_holding_days = 20

for (i,event) in df_events.iterrows():
    bkt_event = BktStrategyEvent(df_option_metrics, event, min_holding_days, option_invest_pct=0.2)
    df_res = bkt_event.events_run()
    print(df_res)

# event = df_events.iloc[0]
# bkt_event = BktStrategyEvent(df_option_metrics, event, min_holding_days, option_invest_pct=0.2)
# df_res = bkt_event.events_run()
# print(df_res)

