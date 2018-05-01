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
import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D

class BktStrategyEventVol(BktOptionStrategy):

    def __init__(self, df_option_metrics, df_events, option_invest_pct=0.1):

        BktOptionStrategy.__init__(self, df_option_metrics,rf = 0.0)
        self.df_events = df_events.sort_values(by='dt_impact_beg', ascending=True).reset_index()
        self.moneyness = 0
        self.option_invest_pct = option_invest_pct
        self.cash_reserve_pct = 0.05
        self.delta_neutral = False
        self.portfolio = None

    def options_straddle(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        # bkt2 = BktAccount()
        idx_event = 0
        print(self.df_events)
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            # dt_event = self.df_events.loc[idx_event, 'dt_impact_beg']
            # dt_event = self.df_events.loc[idx_event, 'dt_test']
            # dt_volpeak = self.df_events.loc[idx_event, 'dt_vol_peak']
            # dt_volpeak = self.df_events.loc[idx_event, 'dt_test2']
            # cd_trade_deriction = self.df_events.loc[idx_event, 'cd_trade_direction']
            # cd_open_position_time = self.df_events.loc[idx_event, 'cd_open_position_time']
            # cd_close_position_time = self.df_events.loc[idx_event, 'cd_close_position_time']
            # cd_open_position_time = 'morning_open_15min'
            # cd_close_position_time = 'afternoon_close_15min'

            # cd_close_position_time = 'daily_avg'
            # cd_close_position_time = None


            evalDate = bkt_optionset.eval_date
            strikes = bkt_optionset.df_daily_state['amt_strike'].tolist()
            black_var_surface = bkt_optionset.get_volsurface_squre('put')


            # vol_t1 = black_var_surface.blackVol(0.03, 2.8)
            # print(vol_t1)

            plt.rcParams['font.sans-serif'] = ['STKaiti']
            plt.rcParams.update({'font.size': 13})

            plot_years = np.arange(0.1, 0.4, 0.01)
            plot_strikes = np.arange(black_var_surface.minStrike()+0.001, black_var_surface.maxStrike()-0.001, 0.01)
            fig = plt.figure()
            ax = fig.gca(projection='3d')
            X, Y = np.meshgrid(plot_strikes, plot_years)
            Z = np.array([black_var_surface.blackVol(y, x)
                          for xr, yr in zip(X, Y)
                          for x, y in zip(xr, yr)]
                         ).reshape(len(X), len(X[0]))
            surf = ax.plot_surface(X, Y, Z, rstride=1, cstride=1, cmap=cm.coolwarm,
                                   linewidth=0.2)
            ax.set_xlabel('strikes')
            ax.set_ylabel('maturities')
            fig.colorbar(surf, shrink=0.5, aspect=5)
            plt.show()

            bkt_optionset.next()


"""Back Test Settings"""
start_date = datetime.date(2015, 8, 18)
end_date = datetime.date(2015, 8, 30)

calendar = ql.China()
daycounter = ql.ActualActual()
util = BktUtil()

"""Collect Mkt Date"""
df_events = get_eventsdata(start_date, end_date,1)

df_option_metrics = get_mktdata(start_date, end_date)

"""Run Backtest"""

bkt_strategy = BktStrategyEventVol(df_option_metrics, df_events, option_invest_pct=0.2)
bkt_strategy.set_min_holding_days(20)

bkt_strategy.options_straddle()
