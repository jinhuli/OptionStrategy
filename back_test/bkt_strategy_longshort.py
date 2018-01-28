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


    def get_long_short(self,eval_date):
        df_top, df_bottom = self.get_top_bottom(eval_date)
        if self.trade_type==self.util.long_top or self.trade_type==None:
            df_long = df_top
            df_short = df_bottom
        else:
            df_long = df_bottom
            df_short = df_top
        return df_long, df_short


    def get_top_bottom(self,eval_date):
        if self.option_type == None or self.option_type == 'all':
            option_call = self.bkt_optionset.bktoption_list_call
            option_put = self.bkt_optionset.bktoption_list_put
            option_call  =self.get_candidate_set(eval_date,option_call)
            option_put  =self.get_candidate_set(eval_date,option_put)
            df_ranked_call = self.get_ranked(option_call)
            df_ranked_put = self.get_ranked(option_put)
            df_ranked_call = df_ranked_call[df_ranked_call[self.util.col_carry] != -999.0]
            df_ranked_put = df_ranked_put[df_ranked_put[self.util.col_carry] != -999.0]
            n = self.nbr_top_bottom
            if len(df_ranked_call)<=2*n:
                df_top_call = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
                df_bottom_call = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
            else:
                df_top_call = df_ranked_call[0:n]
                df_bottom_call = df_ranked_call[-n:]
            if len(df_ranked_put)<=2*n:
                df_top_put = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
                df_bottom_put = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
            else:
                df_top_put = df_ranked_put[0:n]
                df_bottom_put = df_ranked_put[-n:]
            df_top = df_top_call.append(df_top_put,ignore_index=True)
            df_bottom = df_bottom_call.append(df_bottom_put,ignore_index=True)
            return df_top,df_bottom
        else:
            if self.option_type == self.util.type_call:
                option_list = self.bkt_optionset.bktoption_list_call
            else:
                option_list = self.bkt_optionset.bktoption_list_put
            option_list  =self.get_candidate_set(eval_date,option_list)
            df_ranked = self.get_ranked(option_list)
            df_ranked = df_ranked[df_ranked[self.util.col_carry] != -999.0]
            n = self.nbr_top_bottom
            if len(df_ranked)<=2*n:
                df_top = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
                df_bottom = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
            else:
                df_top = df_ranked[0:n]
                df_bottom = df_ranked[-n:]
            return df_top,df_bottom


    def get_ranked(self,option_list):
        return self.bkt_optionset.rank_by_carry(option_list) # Ranked Descending


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
        return option_list

    """ 1 : Equal Long/Short Market Value """
    def get_fund_long_short_1(self,invest_fund,df_buy,df_sell):
        m_v_long = 0.0
        m_v_short = 0.0
        for (idx, row) in df_buy.iterrows():
            bktoption = row['bktoption']
            premium = bktoption.option_price*bktoption.multiplier
            money = premium
            m_v = money/premium
            m_v_long += m_v
        for (idx,row) in df_sell.iterrows():
            bktoption = row['bktoption']
            premium = bktoption.option_price * bktoption.multiplier
            money = bktoption.get_init_margin()-premium
            m_v = money/premium
            m_v_short += m_v
        mtm = invest_fund/(m_v_long+m_v_short)
        for (idx,row) in df_buy.iterrows():
            bktoption = row['bktoption']
            unit = bktoption.get_unit_by_mtmv(mtm)
            df_buy.loc[idx,'unit'] = unit
            df_buy.loc[idx,'mtm'] = unit*bktoption.option_price*bktoption.multiplier
        for (idx,row) in df_sell.iterrows():
            bktoption = row['bktoption']
            unit = bktoption.get_unit_by_mtmv(mtm)
            df_sell.loc[idx,'unit'] = unit
            df_sell.loc[idx,'mtm'] = unit*bktoption.option_price*bktoption.multiplier
        return df_buy,df_sell


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
                # option_list = self.get_candidate_set(evalDate)
                df_buy, df_sell = self.get_long_short(evalDate)

                """平仓：将手中头寸进行平仓，除非当前头寸在新一轮持有期中仍判断持有相同的方向，则不会先平仓再开仓"""
                for bktoption in bkt.holdings:
                    if bktoption.maturitydt <= hp_enddate:
                        bkt.close_position(evalDate, bktoption)
                    else:
                        if bktoption.trade_long_short == 1 and bktoption in df_buy['bktoption']: continue
                        if bktoption.trade_long_short == -1 and bktoption in df_sell['bktoption']: continue
                        bkt.close_position(evalDate, bktoption)

                """开仓：等金额做多df_buy，等金额做空df_sell"""
                if len(df_buy)+len(df_sell) == 0:
                    self.flag_trade = False
                else:
                    invest_fund = bkt.cash * self.money_utl
                    df_buy, df_sell = self.get_fund_long_short_1(invest_fund, df_buy, df_sell)
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
                            bkt.open_short(evalDate, bktoption,unit)
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




























