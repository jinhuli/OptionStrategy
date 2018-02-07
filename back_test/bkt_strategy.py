from back_test.bkt_account import BktAccount
from back_test.bkt_option_set import BktOptionSet
import QuantLib as ql
from back_test.bkt_util import BktUtil
from abc import ABCMeta, abstractmethod


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
        self.bkt_optionset = BktOptionSet('daily', df_option_metrics, hp)
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


    @abstractmethod
    def get_ranked_options(self, eval_date):
        return


    @abstractmethod
    def get_long_short(self, df):
        return


    @abstractmethod
    def get_weighted_ls(self, invest_fund, df):
        return


    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account

        while bkt_optionset.index < len(bkt_optionset.dt_list):
            if bkt_optionset.index == 0:
                bkt_optionset.next()
                continue

            evalDate = bkt_optionset.eval_date
            hp_enddate = self.to_dt_date(
                self.calendar.advance(self.to_ql_date(evalDate), ql.Period(self.holding_period, ql.Days)))

            df_metrics_today = self.df_option_metrics[(self.df_option_metrics[self.col_date] == evalDate)]

            """回测期最后一天全部清仓"""
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate, df_metrics_today, self.col_close)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break

            """清仓到期头寸"""
            for bktoption in bkt.holdings:
                if bktoption.maturitydt == evalDate:
                    print('Liquidate position at maturity : ', evalDate, ' , ', bktoption.maturitydt)
                    bkt.close_position(evalDate, bktoption)

            """持有期holding_period满，进行调仓 """
            if (bkt_optionset.index - 1) % self.holding_period == 0 or not self.flag_trade:
                print('调仓 : ', evalDate)
                invest_fund = bkt.cash * self.money_utl
                df_option = self.get_ranked_options(evalDate)
                df_option = self.get_weighted_ls(invest_fund, df_option)
                df_buy = df_option[df_option['weight'] > 0]
                df_sell = df_option[df_option['weight'] < 0]

                """平仓：将手中头寸进行平仓，除非当前头寸在新一轮持有期中仍判断持有相同的方向，则不会先平仓再开仓"""
                for bktoption in bkt.holdings:
                    if bktoption.maturitydt <= hp_enddate:
                        bkt.close_position(evalDate, bktoption)
                    else:
                        if bktoption.trade_long_short == 1 and bktoption in df_buy['bktoption']: continue
                        if bktoption.trade_long_short == -1 and bktoption in df_sell['bktoption']: continue
                        bkt.close_position(evalDate, bktoption)

                """开仓：做多df_buy，做空df_sell"""
                if len(df_buy) + len(df_sell) == 0:
                    self.flag_trade = False
                else:

                    for (idx, row) in df_buy.iterrows():
                        bktoption = row['bktoption']
                        unit = row['unit']
                        if bktoption in bkt.holdings and bktoption.trade_flag_open:
                            bkt.rebalance_position(evalDate, bktoption, unit)
                        else:
                            bkt.open_long(evalDate, bktoption, unit)
                    for (idx, row) in df_sell.iterrows():
                        bktoption = row['bktoption']
                        unit = row['unit']
                        if bktoption in bkt.holdings and bktoption.trade_flag_open:
                            bkt.rebalance_position(evalDate, bktoption, unit)
                        else:
                            bkt.open_short(evalDate, bktoption, unit)
                    self.flag_trade = True

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt.mkm_update(evalDate, df_metrics_today, self.col_close)
            print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
            bkt_optionset.next()

    def return_analysis(self):
        ar = 100 * self.bkt_account.calculate_annulized_return()
        mdd = 100 * self.bkt_account.calculate_max_drawdown()
        print('=' * 50)
        print("%20s %20s" % ('annulized_return(%)', 'max_drawdown(%)'))
        print("%20s %20s" % (round(ar, 4), round(mdd, 4)))
        print('-' * 50)
        self.bkt_account.plot_npv()
