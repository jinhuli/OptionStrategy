from data_access.get_data import get_50option_mktdata as option_data
from back_test.BktOptionStrategy import BktOptionStrategy
import datetime
import math
import pandas as pd
class SkewIndexing(BktOptionStrategy):

    def __init__(self,start_date,end_date,min_holding):
        df_metrics = option_data(start_date,end_date)
        BktOptionStrategy.__init__(self, df_metrics,rf=0.01)
        self.set_min_holding_days(min_holding)

    def select_eligible_contracts(self, df_data):
        if df_data.empty: return
        df_metrics = self.util.get_duplicate_strikes_dropped(df_data)
        # TODO: add other criterion
        return df_metrics

    def fun_otm_quote(self, df):
        if df[self.util.col_strike] > df['atm_k']:
            quote = df['amt_call_quote']
        elif df[self.util.col_strike] < df['atm_k']:
            quote = df['amt_put_quote']
        else:
            quote = (df['amt_call_quote'] + df['amt_put_quote'])/2
        return quote

    def fun_for_p1(self, df):
        DK = df['amt_delta_k']
        Q = df['amt_otm_quote']
        K = df[self.util.col_strike]
        return -Q*DK/K**2

    def fun_for_p2(self, df):
        DK = df['amt_delta_k']
        Q = df['amt_otm_quote']
        K = df[self.util.col_strike]
        F = df['F']
        return 2*(1-math.log(K/F,math.e))*Q*DK/K**2

    def fun_for_p3(self, df):
        DK = df['amt_delta_k']
        Q = df['amt_otm_quote']
        K = df[self.util.col_strike]
        F = df['F']
        return 3*(2*math.log(K/F,math.e)-math.log(K/F,math.e)**2)*Q*DK/K**2

    def get_e1(self, F, K):
        return -(1 + math.log(F/K,math.e) - F/K)

    def get_e2(self, F, K):
        return 2*math.log(K/F, math.e)*(F/K-1) + 0.5*(math.log(K/F))**2

    def get_e3(self, F, K):
        return 3*((math.log(K/F, math.e))**2) * ((1.0/3.0)*math.log(K/F, math.e)+F/K-1)

    def get_S(self, p1, p2, p3):
        u = (p3-3*p1*p2+2*p1**3)
        x = p1**2
        y = p2-p1**2
        d = (p2-p1**2)**1.5 # TODO: 被开方数字为负
        return (p3-3*p1*p2+2*p1**3)/(p2-p1**2)**1.5

    def for_calculation(self, df_mdt, atm_k, F):
        df_mdt['atm_k'] = atm_k
        df_mdt['F'] = F
        df_mdt['amt_otm_quote'] = df_mdt.apply(self.fun_otm_quote,axis=1)
        return df_mdt

    def get_T_quotes(self, df_mdt,eval_date):
        df_call = df_mdt[df_mdt[self.util.col_option_type] == self.util.type_call].set_index(self.util.col_strike)
        df_put = df_mdt[df_mdt[self.util.col_option_type] == self.util.type_put].set_index(self.util.col_strike)
        df = pd.DataFrame()
        df['amt_cp_diff'] = abs(df_call[self.util.col_option_price] - df_put[self.util.col_option_price])
        df[self.util.col_strike] = df_call.index
        df['amt_call_quote'] = df_call[self.util.col_option_price]
        df['amt_put_quote'] = df_put[self.util.col_option_price]
        maturitydt = list(df_put[self.util.col_maturitydt])[0]
        df[self.util.col_maturitydt] = maturitydt
        df['amt_fv'] = math.exp(self.bkt_optionset.rf*((maturitydt-eval_date).days/365.0))
        df = df.sort_values(by=self.util.col_strike,ascending=True).reset_index(drop=True)
        delta_k = df[self.util.col_strike].diff().mean()
        df['amt_delta_k'] = df[self.util.col_strike].apply(lambda x: delta_k if list(df[self.util.col_strike])[0]<x<list(df[self.util.col_strike])[-1] else 2*delta_k)
        return df

    def forward_cboe(self,t_quotes,eval_date):
        df = t_quotes.set_index(self.util.col_strike)
        atm_k = df.sort_values(by='amt_cp_diff',ascending=True).index[0]
        p_call = df.loc[atm_k,'amt_call_quote']
        p_put = df.loc[atm_k,'amt_put_quote']
        forward = df.loc[atm_k,'amt_fv']*(p_call-p_put)+atm_k
        return atm_k,forward

    def calculate(self, df):
        atm_k = df.loc[0,'atm_k']
        F = df.loc[0,'F']
        e1 = self.get_e1(F, atm_k)
        e2 = self.get_e2(F, atm_k)
        e3 = self.get_e3(F, atm_k)
        df['for_p1'] = df.apply(self.fun_for_p1,axis=1)
        df['for_p2'] = df.apply(self.fun_for_p1,axis=1)
        df['for_p3'] = df.apply(self.fun_for_p1,axis=1)
        fv = df.loc[0, 'amt_fv']
        p1 = fv * df['for_p1'].sum() + e1
        p2 = fv * df['for_p2'].sum() + e2
        p3 = fv * df['for_p3'].sum() + e3
        S = self.get_S(p1, p2, p3)
        return S

    def skew(self, eval_date):
        df_daily_state = self.bkt_optionset.df_data[self.bkt_optionset.df_data[self.util.col_date]==eval_date].reset_index(drop=True)
        mdt1 = self.get_1st_eligible_maturity(eval_date)
        mdt2 = self.get_2nd_eligible_maturity(eval_date)
        df_mdt1 = self.select_eligible_contracts(self.util.get_df_by_mdt(df_daily_state,mdt1))
        df_mdt2 = self.select_eligible_contracts(self.util.get_df_by_mdt(df_daily_state,mdt2))
        t_quotes1 = self.get_T_quotes(df_mdt1,eval_date)
        atm_k1,F1 = self.forward_cboe(t_quotes1,eval_date)
        t_quotes1 = self.for_calculation(t_quotes1,atm_k1,F1)
        t_quotes2 = self.get_T_quotes(df_mdt2, eval_date)
        atm_k2, F2 = self.forward_cboe(t_quotes2, eval_date)
        t_quotes2 = self.for_calculation(t_quotes2, atm_k2, F2)
        S1 = self.calculate(t_quotes1)
        S2 = self.calculate(t_quotes2)
        day_30 = eval_date + datetime.timedelta(days=30)
        w = (mdt2-day_30).days/(mdt2-mdt1).days
        skew = 100 - 10*(w*S1+(1-w)*S2)
        return skew

start_date = datetime.date(2017,10,1)
end_date = datetime.date(2017,12,12)
skew_indexing = SkewIndexing(start_date,end_date,8)
skew_indexing.skew(datetime.date(2017,10,19))