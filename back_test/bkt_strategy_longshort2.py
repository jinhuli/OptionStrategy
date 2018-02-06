from back_test.bkt_account import BktAccount
from back_test.bkt_option_set import BktOptionSet
import QuantLib as ql
from back_test.bkt_util import BktUtil
import pandas as pd


class BktStrategyLongShort(object):

    def __init__(self,df_option_metrics,hp,money_utilization=0.2,init_fund=1000000000.0,tick_size=0.0001,fee_rate=2.0/10000,
                 nbr_slippage=0,max_money_utilization=0.5,buy_ratio=0.5,sell_ratio=0.5,nbr_top_bottom=5
                 ):
        self.util = BktUtil()
        self.init_fund = init_fund
        self.money_utl = money_utilization
        self.holding_period = hp
        self.df_option_metrics = df_option_metrics
        self.buy_ratio = buy_ratio
        self.sell_ratio = sell_ratio
        self.nbr_top_bottom = nbr_top_bottom
        self.option_type = None
        self.min_ttm = None
        self.max_ttm = None
        self.moneyness_type = None
        self.trade_type = None
        self.min_volume = None
        self.flag_trade = False
        self.calendar = ql.China()
        self.bkt_account = BktAccount(fee_rate=fee_rate,init_fund=init_fund)
        self.bkt_optionset = BktOptionSet('daily', df_option_metrics, hp)


    def set_min_ttm(self,min_ttm):
        self.min_ttm = min_ttm


    def set_max_ttm(self,max_ttm):
        self.max_ttm = max_ttm

    def set_min_trading_volume(self,min_volume):
        self.min_volume = min_volume

    def set_option_type(self,option_type):
        self.option_type = option_type

    def set_trade_type(self,trade_type):
        self.trade_type = trade_type

    def set_moneyness_type(self, moneyness_type):
        self.moneyness_type = moneyness_type

    def get_candidate_set(self,eval_date,option_list):

        if self.min_ttm != None :
            list = []
            for option in option_list:
                min_maturity = self.util.to_dt_date(self.calendar.advance(self.util.to_ql_date(eval_date),ql.Period(self.min_ttm,ql.Days)))
                if option.maturitydt>=min_maturity:
                    list.append(option)
            option_list = list
        if self.max_ttm != None:
            list = []
            for option in option_list:
                max_maturity = self.util.to_dt_date(self.calendar.advance(self.util.to_ql_date(eval_date),ql.Period(self.max_ttm,ql.Days)))
                if  option.maturitydt<=max_maturity:
                    list.append(option)
            option_list = list
        if self.min_volume != None:
            list = []
            for option in option_list:
                if option.get_trading_volume() >= self.min_volume:
                    list.append(option)
            option_list = list
        if self.moneyness_type == 'atm':
            list = []
            set_atm = set(self.bkt_optionset.bktoption_list_atm)
            set_list = set(option_list)
            option_set = set_atm.intersection(set_list)
            for i in option_set:
                list.append(i)
            option_list = list
        if self.moneyness_type == 'otm':
            list = []
            set_atm = set(self.bkt_optionset.bktoption_list_otm)
            set_list = set(option_list)
            option_set = set_atm.intersection(set_list)
            for i in option_set:
                list.append(i)
            option_list = list
        return option_list

    def get_ranked_carries(self,eval_date):
        if self.option_type == None or self.option_type == 'all':
            option_list = self.get_candidate_set(eval_date,self.bkt_optionset.bktoption_list)
        elif self.option_type == self.util.type_call:
            option_list = self.get_candidate_set(eval_date,self.bkt_optionset.bktoption_list_call)
        elif self.option_type == self.util.type_put:
            option_list = self.get_candidate_set(eval_date,self.bkt_optionset.bktoption_list_put)
        else:
            print('option type not set')
            return
        df_carry = self.bkt_optionset.rank_by_carry2(option_list)
        return df_carry

    """ Ranked Weighted Market Value """
    def get_weighted_ls(self,invest_fund,df):
        if len(df) == 0:return df
        df = self.get_long_short(df,self.util.col_carry)
        sum_1 = 0.0
        for (idx, row) in df.iterrows():
            bktoption = row['bktoption']
            W = row['weight'] # weight
            V = bktoption.option_price*bktoption.multiplier # value or say premium
            if W > 0:
                M = V # money usage equals premiun
            else:
                M = bktoption.get_init_margin()-V # money usage equals margin - premium earned
            sum_1 += abs(W)*M
        if sum_1 <= 0: MV = invest_fund
        else: MV = invest_fund/sum_1
        for (idx,row) in df.iterrows():
            bktoption = row['bktoption']
            W = row['weight'] # weight
            mtm = abs(W)*MV
            unit = bktoption.get_unit_by_mtmv(mtm)
            df.loc[idx,'unit'] = unit
            df.loc[idx,'mtm1'] = mtm
            df.loc[idx,'mtm'] = unit*bktoption.option_price*bktoption.multiplier
        return df

    def get_long_short(self,df,col_name):
        df_copy = df.copy()
        if self.trade_type == self.util.long_top:
            df = df.loc[:, df.columns != self.util.col_carry].join(df[[self.util.col_carry]].rank(method='dense'))
        elif self.trade_type == self.util.long_bottom:
            df = df.loc[:, df.columns != self.util.col_carry].join(df[[self.util.col_carry]].rank(method='dense',ascending=False))
        else:
            print('Trade type not set!')
            return
        df = df.rename(columns={self.util.col_carry: 'rank'})
        df = df.join(df_copy.loc[:, df_copy.columns == self.util.col_carry])
        n = len(df)
        df['weight'] = df['rank'] - (n + 1) / 2
        c = sum(df[df['weight'] > 0]['weight'])
        df['weight'] = df['weight'] / c
        return df

    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt = self.bkt_account

        while bkt_optionset.index < len(bkt_optionset.dt_list):
            if bkt_optionset.index == 0:
                bkt_optionset.next()
                continue

            evalDate = bkt_optionset.eval_date
            hp_enddate = self.util.to_dt_date(
                self.calendar.advance(self.util.to_ql_date(evalDate), ql.Period(self.holding_period, ql.Days)))

            df_metrics_today = self.df_option_metrics[(self.df_option_metrics[self.util.col_date]==evalDate)]

            """回测期最后一天全部清仓"""
            if evalDate == bkt_optionset.end_date:
                print(' Liquidate all positions !!! ')
                bkt.liquidate_all(evalDate)
                bkt.mkm_update(evalDate, df_metrics_today, self.util.col_close)
                print(evalDate, ' , ', bkt.npv)  # npv是组合净值，期初为1
                break

            """清仓到期头寸"""
            for bktoption in bkt.holdings:
                if bktoption.maturitydt == evalDate:
                    print('Liquidate position at maturity : ', evalDate, ' , ', bktoption.maturitydt)
                    bkt.close_position(evalDate, bktoption)


            """持有期holding_period满，进行调仓 """
            if (bkt_optionset.index-1)%self.holding_period==0 or not self.flag_trade:
                print('调仓 : ', evalDate)
                invest_fund = bkt.cash * self.money_utl
                df_option = self.get_ranked_carries(evalDate)
                if len(df_option)==0:continue
                df_option = self.get_weighted_ls(invest_fund,df_option)
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
                if len(df_buy)+len(df_sell) == 0:
                    self.flag_trade = False
                else:
                    for (idx, row) in df_option.iterrows():
                        bktoption = row['bktoption']
                        unit = row['unit']
                        weight = row['weight']
                        if bktoption in bkt.holdings and bktoption.trade_flag_open:
                            bkt.rebalance_position(evalDate, bktoption, unit)
                        else:
                            if weight > 0:
                                bkt.open_long(evalDate, bktoption, unit)
                            else:
                                bkt.open_short(evalDate, bktoption, unit)
                    self.flag_trade = True

            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt.mkm_update(evalDate, df_metrics_today, self.util.col_close)
            print(evalDate,' , ' ,bkt.npv) # npv是组合净值，期初为1
            bkt_optionset.next()


    def return_analysis(self):
        ar = 100*self.bkt_account.calculate_annulized_return()
        mdd = 100*self.bkt_account.calculate_max_drawdown()
        print('='*50)
        print("%20s %20s" %('annulized_return(%)','max_drawdown(%)'))
        print("%20s %20s"%(round(ar,4),round(mdd,4)))
        print('-'*50)
        self.bkt_account.plot_npv()




























