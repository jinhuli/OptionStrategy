from back_test.bkt_account import BktAccount
from back_test.bkt_option_set import BktOptionSet
import QuantLib as ql
from back_test.bkt_util import BktUtil
import pandas as pd
import datetime




class BktStrategyLongShort(object):

    def __init__(self,df_option_metrics,hp,money_utilization=0.2,init_fund=1000000.0,tick_size=0.0001,fee_rate=2.0/10000,
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
        self.trade_type = None
        self.calendar = ql.China()
        self.bkt_account = BktAccount(fee_rate=fee_rate,init_fund=init_fund)
        self.bkt_optionset = BktOptionSet('daily', df_option_metrics, hp)


    def set_min_ttm(self,min_ttm):
        self.min_ttm = min_ttm


    def set_max_ttm(self,max_ttm):
        self.max_ttm = max_ttm


    def set_option_type(self,option_type):
        self.option_type = option_type


    def set_trade_type(self,trade_type):
        self.trade_type = trade_type


    def get_long_short(self,option_list):
        df_top, df_bottom = self.get_top_bottom(option_list)
        if self.trade_type==self.util.long_top or self.trade_type==None:
            df_long = df_top
            df_short = df_bottom
        else:
            df_long = df_bottom
            df_short = df_top
        return df_long, df_short


    def get_top_bottom(self,option_list):
        df_ranked = self.get_ranked(option_list)
        n = self.nbr_top_bottom
        if len(df_ranked)<=2*n:
            df_top = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
            df_bottom = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
        else:
            df_ranked = df_ranked[df_ranked[self.util.col_carry] != -999.0]
            df_top = df_ranked.loc[0:n-1]
            df_bottom = df_ranked.loc[len(df_ranked)-n:]
        return df_top,df_bottom


    def get_ranked(self,option_list):
        return self.bkt_optionset.rank_by_carry(option_list) # Ranked Descending


    def get_candidate_set(self,eval_date):
        if self.option_type == None:
            option_list = self.bkt_optionset.bktoption_list
        else:
            if self.option_type == self.util.type_call:
                option_list = self.bkt_optionset.bktoption_list_call
            else:
                option_list = self.bkt_optionset.bktoption_list_put

        if self.min_ttm != None and self.max_ttm != None:
            list = []
            for option in option_list:
                min_maturity = self.util.to_dt_date(self.calendar.advance(self.util.to_ql_date(eval_date),ql.Period(self.min_ttm,ql.Days)))
                max_maturity = self.util.to_dt_date(self.calendar.advance(self.util.to_ql_date(eval_date),ql.Period(self.max_ttm,ql.Days)))
                if option.maturitydt>=min_maturity and option.maturitydt<=max_maturity:
                    list.append(option)
            option_list = list
        return option_list

    """ 1 : Equal Long/Short Market Value """
    def get_fund_long_short_1(self,invest_fund,df_buy,df_sell):
        vl = 0.0
        vs = 0.0
        ml = 0.0
        ms = 0.0
        for (idx, row) in df_buy.iterrows():
            bktoption = row['bktoption']
            value_per_share = bktoption.option_price*bktoption.multiplier
            money_use_per_share = value_per_share
            vl += value_per_share
            ml += money_use_per_share
        for (idx,row) in df_sell.iterrows():
            bktoption = row['bktoption']
            value_per_share = bktoption.option_price * bktoption.multiplier
            money_use_per_share = bktoption.get_init_margin()
            vs += value_per_share
            ms += money_use_per_share
        """ vl*Xl = vs*Xs """
        """ ml*Xl + ms*Xs = invest_fund """
        Xs = invest_fund*vl/(ml*vs+ms*vl) # unit of long
        Xl = vs*Xs/vl # unit of short
        fund_long = Xl*vl
        fund_short = Xs*vs
        return fund_long,fund_short


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
            if (bkt_optionset.index-1)%self.holding_period == 0:
                print('调仓 : ', evalDate)
                option_list = self.get_candidate_set(evalDate)
                df_buy, df_sell = self.get_long_short(option_list)

                """平仓：将手中头寸进行平仓，除非当前头寸在新一轮持有期中仍判断持有相同的方向，则不会先平仓再开仓"""
                for bktoption in bkt.holdings:
                    if bktoption.maturitydt <= hp_enddate:
                        bkt.close_position(evalDate, bktoption)
                    else:
                        if bktoption.trade_long_short == 1 and bktoption in df_buy['bktoption']: continue
                        if bktoption.trade_long_short == -1 and bktoption in df_sell['bktoption']: continue
                        bkt.close_position(evalDate, bktoption)

                """开仓：等金额做多df_buy，等金额做空df_sell"""
                fund_buy = bkt.cash * self.money_utl * self.buy_ratio
                fund_sell = bkt.cash * self.money_utl * self.sell_ratio
                n1 = len(df_buy)
                n2 = len(df_sell)
                if n1 != 0:
                    for (idx, row) in df_buy.iterrows():
                        bktoption = row['bktoption']
                        if bktoption in bkt.holdings and bktoption.trade_flag_open:
                            bkt.rebalance_position(evalDate, bktoption, fund_buy/n1)
                        else:
                            bkt.open_long(evalDate, bktoption, fund_buy/n1)
                if n2 != 0:
                    for (idx, row) in df_sell.iterrows():
                        bktoption = row['bktoption']
                        if bktoption in bkt.holdings and bktoption.trade_flag_open:
                            bkt.rebalance_position(evalDate, bktoption, fund_sell/n2)
                        else:
                            bkt.open_short(evalDate, bktoption, fund_sell/n2)

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




























