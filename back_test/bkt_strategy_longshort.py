from back_test.bkt_strategy import BktOptionStrategy
import pandas as pd



class CarryLongShort_EW(BktOptionStrategy):


    def __init__(self, df_option_metrics, hp, money_utilization=0.2, init_fund=100000000.0, tick_size=0.0001,
                 fee_rate=2.0/10000,nbr_slippage=0, max_money_utilization=0.5, buy_ratio=0.5,
                 sell_ratio=0.5, nbr_top_bottom=5):

        BktOptionStrategy.__init__(self, df_option_metrics, hp, money_utilization, init_fund, tick_size,
                 fee_rate,nbr_slippage, max_money_utilization)
        self.buy_ratio = buy_ratio
        self.sell_ratio = sell_ratio
        self.nbr_top_bottom = nbr_top_bottom


    def get_ranked_options(self, eval_date):
        if self.option_type == None or self.option_type == 'all':
            option_call = self.bkt_optionset.bktoptionset_call
            option_put = self.bkt_optionset.bktoptionset_put
            option_call = self.get_candidate_set(eval_date, option_call)
            option_put = self.get_candidate_set(eval_date, option_put)
            df_ranked_call = self.bkt_optionset.rank_by_carry2(option_call)
            df_ranked_put = self.bkt_optionset.rank_by_carry2(option_put)
            n = self.nbr_top_bottom
            if len(df_ranked_call) <= 2 * n:
                df_top_call = pd.DataFrame(columns=[self.col_date, self.col_carry, self.bktoption])
                df_bottom_call = pd.DataFrame(columns=[self.col_date, self.col_carry, self.bktoption])
            else:
                df_top_call = df_ranked_call[0:n]
                df_bottom_call = df_ranked_call[-n:]
            if len(df_ranked_put) <= 2 * n:
                df_top_put = pd.DataFrame(columns=[self.col_date, self.col_carry, self.bktoption])
                df_bottom_put = pd.DataFrame(columns=[self.col_date, self.col_carry, self.bktoption])
            else:
                df_top_put = df_ranked_put[0:n]
                df_bottom_put = df_ranked_put[-n:]
            df_top = df_top_call.append(df_top_put, ignore_index=True)
            df_bottom = df_bottom_call.append(df_bottom_put, ignore_index=True)
        else:
            if self.option_type == self.type_call:
                option_list = self.bkt_optionset.bktoptionset_call
            elif self.option_type == self.type_put:
                option_list = self.bkt_optionset.bktoptionset_put
            else:
                print('option type not set')
                return
            option_list = self.get_candidate_set(eval_date, option_list)
            df_ranked = self.bkt_optionset.rank_by_carry2(option_list)
            n = self.nbr_top_bottom
            if len(df_ranked) <= 2 * n:
                df_top = pd.DataFrame(columns=[self.col_date, self.col_carry, self.bktoption])
                df_bottom = pd.DataFrame(columns=[self.col_date, self.col_carry, self.bktoption])
                print('option nbr not enough')
            else:
                df_top = df_ranked[0:n]
                df_bottom = df_ranked[-n:]
        df_top['weight'] = 1
        df_bottom['weight'] = -1
        df = df_top.append(df_bottom, ignore_index=True)
        return df


    def get_long_short(self, df):
        if self.trade_type == self.long_bottom:
            df['weight'] = df['weight'] * (-1)
        return df


    """ 1 : Equal Long/Short Market Value """
    def get_weighted_ls(self, invest_fund, df):
        if len(df) == 0: return df
        df = self.get_long_short(df)
        sum_1 = 0.0
        for (idx, row) in df.iterrows():
            bktoption = row['bktoption']
            W = row['weight']  # weight
            V = bktoption.option_price * bktoption.multiplier  # value or say premium
            if W > 0:
                M = V  # money usage equals premium
            else:
                M = bktoption.get_init_margin() - V  # money usage equals margin - premium earned
            sum_1 += M / V
        if sum_1 <= 0:
            mtm = invest_fund / len(df)
        else:
            mtm = invest_fund / sum_1
        for (idx, row) in df.iterrows():
            bktoption = row['bktoption']
            unit = bktoption.get_unit_by_mtmv(mtm)
            df.loc[idx, 'unit'] = unit
            df.loc[idx, 'mtm'] = unit * bktoption.option_price * bktoption.multiplier
        return df



class CarryLongShort_RW(BktOptionStrategy):


    def __init__(self, df_option_metrics, hp, money_utilization=0.2, init_fund=100000000.0, tick_size=0.0001,
                 fee_rate=2.0/10000,nbr_slippage=0, max_money_utilization=0.5, buy_ratio=0.5,
                 sell_ratio=0.5, nbr_top_bottom=5):

        BktOptionStrategy.__init__(self, df_option_metrics, hp, money_utilization, init_fund, tick_size,
                 fee_rate,nbr_slippage, max_money_utilization)
        self.buy_ratio = buy_ratio
        self.sell_ratio = sell_ratio
        self.nbr_top_bottom = nbr_top_bottom

    def get_ranked_options(self,eval_date):
        if self.option_type == None or self.option_type == 'all':
            option_list = self.get_candidate_set(eval_date,self.bkt_optionset.bktoptionset)
        elif self.option_type == self.type_call:
            option_list = self.get_candidate_set(eval_date,self.bkt_optionset.bktoptionset_call)
        elif self.option_type == self.type_put:
            option_list = self.get_candidate_set(eval_date,self.bkt_optionset.bktoptionset_put)
        else:
            print('option type not set')
            return
        n = self.nbr_top_bottom
        if len(option_list)<2*n:
            df = pd.DataFrame(columns=[self.col_date, self.col_carry, self.bktoption,'weight'])
        else:
            df_ranked = self.bkt_optionset.rank_by_carry2(option_list)
            df_top = df_ranked[0:n]
            df_bottom = df_ranked[-n:]
            df = df_top.append(df_bottom,ignore_index=True)
        return df

    """ Ranked Weighted Market Value """
    def get_weighted_ls(self,invest_fund,df):
        if len(df) == 0:return df
        df = self.get_long_short(df)
        sum_1 = 0.0
        for (idx, row) in df.iterrows():
            bktoption = row['bktoption']
            W = row['weight'] # weight
            V = bktoption.option_price*bktoption.multiplier # value or say premium
            if W > 0:
                M = V # money usage equals premium
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

    def get_long_short(self,df):
        df_copy = df.copy()
        if self.trade_type == self.long_top:
            df = df.loc[:, df.columns != self.col_carry].join(df[[self.col_carry]].rank(method='dense'))
        elif self.trade_type == self.long_bottom:
            df = df.loc[:, df.columns != self.col_carry].join(df[[self.col_carry]].rank(method='dense',ascending=False))
        else:
            print('Trade type not set!')
            return
        df = df.rename(columns={self.col_carry: 'rank'})
        df = df.join(df_copy.loc[:, df_copy.columns == self.col_carry])
        n = len(df)
        df['weight'] = df['rank'] - (n + 1) / 2
        c = sum(df[df['weight'] > 0]['weight'])
        df['weight'] = df['weight'] / c
        return df
