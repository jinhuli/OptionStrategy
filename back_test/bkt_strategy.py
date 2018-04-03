from back_test.bkt_account import BktAccount
from back_test.bkt_option_set import BktOptionSet
import QuantLib as ql
from back_test.bkt_util import BktUtil
from abc import ABCMeta, abstractmethod
import pandas as pd

class BktOptionStrategy(BktUtil):

    __metaclass__=ABCMeta


    def __init__(self, df_option_metrics, hp, money_utilization, init_fund, tick_size,
                 fee_rate,nbr_slippage, max_money_utilization):
        BktUtil.__init__(self)
        self.init_fund = init_fund
        self.money_utl = money_utilization
        self.holding_period = hp
        self.df_option_metrics = df_option_metrics
        self.calendar = ql.China()
        self.bkt_account = BktAccount(fee_rate=fee_rate, init_fund=init_fund)
        self.bkt_optionset = BktOptionSet('daily', df_option_metrics)
        self.option_type = None
        self.min_ttm = None
        self.max_ttm = None
        self.moneyness_type = None
        self.trade_type = None
        self.min_volume = None
        self.flag_trade = False

    def set_min_ttm(self, min_ttm):
        self.min_ttm = min_ttm

    def set_max_ttm(self, max_ttm):
        self.max_ttm = max_ttm

    def set_min_trading_volume(self, min_volume):
        self.min_volume = min_volume

    def set_option_type(self, option_type):
        self.option_type = option_type

    def set_trade_type(self, trade_type):
        self.trade_type = trade_type

    def set_moneyness_type(self, moneyness_type):
        self.moneyness_type = moneyness_type

    def get_candidate_set(self, eval_date, option_set):
        candidate_set = option_set.copy()

        if self.min_ttm != None:
            for option in option_set:
                if option not in candidate_set: continue
                min_maturity = self.to_dt_date(
                    self.calendar.advance(self.to_ql_date(eval_date), ql.Period(self.min_ttm, ql.Days)))
                if option.maturitydt < min_maturity:
                    candidate_set.remove(option)

        if self.max_ttm != None:
            for option in option_set:
                if option not in candidate_set: continue
                max_maturity = self.to_dt_date(
                    self.calendar.advance(self.to_ql_date(eval_date), ql.Period(self.max_ttm, ql.Days)))
                if option.maturitydt > max_maturity:
                    candidate_set.remove(option)

        if self.min_volume != None:
            for option in option_set:
                if option not in candidate_set: continue
                if option.get_trading_volume() < self.min_volume:
                    candidate_set.remove(option)

        if self.moneyness_type == 'atm':
            set_atm = set(self.bkt_optionset.bktoptionset_atm)
            candidate_set = candidate_set.intersection(set_atm)

        if self.moneyness_type == 'otm':
            set_otm = set(self.bkt_optionset.bktoptionset_otm)
            candidate_set = candidate_set.intersection(set_otm)

        return candidate_set

    """Construct a delta neutral long straddle strategy, 
        returning bkt_option objects to buy in a dataframe"""
    def long_straddle(self,moneyness,df_metrics_today,fund):
        df = pd.DataFrame()
        # moneymess：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档
        # if moneyness == 0:

        return df

    def strangle(self):
        return None

    @abstractmethod
    def get_ranked_options(self, eval_date):
        return


    @abstractmethod
    def get_long_short(self, df):
        return


    @abstractmethod
    def get_weighted_ls(self, invest_fund, df):
        return

    @abstractmethod
    def run(self):
        return

    def return_analysis(self):
        ar = 100 * self.bkt_account.calculate_annulized_return()
        mdd = 100 * self.bkt_account.calculate_max_drawdown()
        print('=' * 50)
        print("%20s %20s" % ('annulized_return(%)', 'max_drawdown(%)'))
        print("%20s %20s" % (round(ar, 4), round(mdd, 4)))
        print('-' * 50)
        self.bkt_account.plot_npv()
