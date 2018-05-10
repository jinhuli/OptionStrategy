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

class BktStrategyVolsurfae(BktOptionStrategy):

    def __init__(self, df_option_metrics, option_invest_pct=0.1):

        BktOptionStrategy.__init__(self, df_option_metrics,rf = 0.0)
        self.moneyness = 0
        self.option_invest_pct = option_invest_pct
        self.cash_reserve_pct = 0.05
        self.delta_neutral = False
        self.portfolio = None

    def vol_surface(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account
        # bkt2 = BktAccount()
        idx_event = 0
        while bkt_optionset.index < len(bkt_optionset.dt_list):

            evalDate = bkt_optionset.eval_date
            black_var_surface = bkt_optionset.get_volsurface_squre('call')

            plt.rcParams['font.sans-serif'] = ['STKaiti']
            plt.rcParams.update({'font.size': 13})

            plot_years = np.arange(0.1, 0.4, 0.01)
            k_min = min(black_var_surface.minStrike(), black_var_surface.maxStrike())
            k_max = max(black_var_surface.minStrike(), black_var_surface.maxStrike())
            plot_strikes = np.arange(k_min + 0.001, k_max - 0.001, 0.01)
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
            fig.savefig('../save_results/vol_call_'+str(evalDate)+'.png')

            black_var_surface = bkt_optionset.get_volsurface_squre('put')

            plt.rcParams['font.sans-serif'] = ['STKaiti']
            plt.rcParams.update({'font.size': 13})

            plot_years = np.arange(0.1, 0.4, 0.01)
            k_min = min(black_var_surface.minStrike(), black_var_surface.maxStrike())
            k_max = max(black_var_surface.minStrike(), black_var_surface.maxStrike())
            plot_strikes = np.arange(k_min+0.001, k_max-0.001, 0.01)
            fig1 = plt.figure()
            ax1 = fig1.gca(projection='3d')
            X, Y = np.meshgrid(plot_strikes, plot_years)
            Z = np.array([black_var_surface.blackVol(y, x)
                          for xr, yr in zip(X, Y)
                          for x, y in zip(xr, yr)]
                         ).reshape(len(X), len(X[0]))
            surf = ax1.plot_surface(X, Y, Z, rstride=1, cstride=1, cmap=cm.coolwarm,
                                   linewidth=0.2)
            ax1.set_xlabel('strikes')
            ax1.set_ylabel('maturities')
            fig1.colorbar(surf, shrink=0.5, aspect=5)
            fig1.savefig('../save_results/vol_put_'+str(evalDate)+'.png')
            plt.show()

            bkt_optionset.next()


"""Back Test Settings"""
start_date = datetime.date(2017, 11, 11)
end_date = datetime.date(2017, 12, 1)

calendar = ql.China()
daycounter = ql.ActualActual()
util = BktUtil()

"""Collect Mkt Date"""
# df_events = get_eventsdata(start_date, end_date,1)

df_option_metrics = get_mktdata(start_date, end_date)

"""Run Backtest"""

bkt_strategy = BktStrategyVolsurfae(df_option_metrics, option_invest_pct=0.2)
bkt_strategy.set_min_holding_days(20)

bkt_strategy.vol_surface()
