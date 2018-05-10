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


class AnalysisVolStrategy(BktOptionStrategy):

    def __init__(self, df_option_metrics, option_invest_pct=0.1):

        BktOptionStrategy.__init__(self, df_option_metrics,rf = 0.03)
        self.moneyness = 0
        self.option_invest_pct = option_invest_pct
        self.cash_reserve_pct = 0.05
        self.delta_neutral = False
        self.portfolio = None

    """ Effect of vol changes on option strategies"""
    def senario_vols(self):
        bkt_optionset = self.bkt_optionset

        evalDate = bkt_optionset.eval_date
        maturityDate = self.get_1st_eligible_maturity(evalDate)
        print(evalDate)
        print(maturityDate)
        delta_exposure = 0.0
        portfolio = self.bkt_optionset.get_straddle(
            self.moneyness, maturityDate, delta_exposure)
        k_call = portfolio.option_call.strike
        p_call = portfolio.option_call.option_price
        iv_call = portfolio.option_call.implied_vol
        spot = portfolio.option_call.underlying_price
        for delta_iv in [0.05,0.1,0.15]:
            senario_pcall = portfolio.option_call.senario_calculate_option_price(spot,iv_call+delta_iv)

"""Back Test Settings"""
start_date = datetime.date(2018, 4, 10)
end_date = datetime.date(2018, 4, 15)


calendar = ql.China()
daycounter = ql.ActualActual()

"""Collect Mkt Date"""
# df_events = get_eventsdata(start_date, end_date,1)

df_option_metrics = get_mktdata(start_date, end_date)

"""Run Backtest"""

bkt_strategy = AnalysisVolStrategy(df_option_metrics, option_invest_pct=0.2)
bkt_strategy.set_min_holding_days(15)


bkt_strategy.senario_vols()
