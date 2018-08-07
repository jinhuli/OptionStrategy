from data_access.get_data import get_50option_mktdata as option_data
from back_test.model.base_option_set import BaseOptionSet
from back_test.model.constant import Util,OptionType, OptionUtil
from PricingLibrary.Util import PricingUtil
import datetime
import math
import pandas as pd


class SkewIndexing(BaseOptionSet):
    def __init__(self, start_date, end_date, min_holding):
        df_metrics = option_data(start_date, end_date)
        super().__init__(df_metrics, rf=0.03)
        # self.set_min_holding_days(min_holding)

    # def select_eligible_contracts(self, df_data, eval_date):
    #     if df_data.empty: return
    #     df_metrics = Util.get_duplicate_strikes_dropped(df_data,eval_date)
    #     # TODO: add other criterion
    #     return df_metrics

    def fun_otm_quote(self, df):
        if df[Util.AMT_APPLICABLE_STRIKE] > df['mid_k']:
            quote = df['amt_call_quote']
        elif df[Util.AMT_APPLICABLE_STRIKE] < df['mid_k']:
            quote = df['amt_put_quote']
        else:
            quote = (df['amt_call_quote'] + df['amt_put_quote']) / 2.0
        return quote

    def fun_for_p1(self, df):
        DK = df['amt_delta_k']
        Q = df['amt_otm_quote']
        K = df[Util.AMT_APPLICABLE_STRIKE]
        return Q * DK / K ** 2

    def fun_for_p2(self, df):
        DK = df['amt_delta_k']
        Q = df['amt_otm_quote']
        K = df[Util.AMT_APPLICABLE_STRIKE]
        F = df['F']
        return 2 * (1 - math.log(K / F, math.e)) * Q * DK / K ** 2

    def fun_for_p3(self, df):
        DK = df['amt_delta_k']
        Q = df['amt_otm_quote']
        K = df[Util.AMT_APPLICABLE_STRIKE]
        F = df['F']
        return 3 * (2 * math.log(K / F, math.e) - math.log(K / F, math.e) ** 2) * Q * DK / K ** 2

    def fun_for_sigma(self, df):
        DK = df['amt_delta_k']
        Q = df['amt_otm_quote']
        K = df[Util.AMT_APPLICABLE_STRIKE]
        return Q * DK / K ** 2

    def get_e1(self, F, K):
        return -(1 + math.log(F / K, math.e) - F / K)

    def get_e2(self, F, K):
        return 2 * math.log(K / F, math.e) * (F / K - 1) + 0.5 * math.log(K / F, math.e) ** 2

    def get_e3(self, F, K):
        return 3 * (math.log(K / F, math.e) ** 2) * ((1.0 / 3.0) * math.log(K / F, math.e) + F / K - 1)

    def get_S(self, p1, p2, p3):
        # u = (p3-3*p1*p2+2*p1**3)
        # x = p1**2
        # y = p2-p1**2
        # d = (p2-p1**2)**1.5
        res = (p3 - 3 * p1 * p2 + 2 * p1 ** 3) / (p2 - p1 ** 2) ** 1.5
        # print("S : ", res)
        return res

    def get_T_quotes(self, df_mdt, eval_date):
        df_call = df_mdt[df_mdt[Util.CD_OPTION_TYPE] == Util.STR_CALL].rename(
            columns={Util.AMT_CLOSE: 'amt_call_quote'})
        df_put = df_mdt[df_mdt[Util.CD_OPTION_TYPE] == Util.STR_PUT].rename(
            columns={Util.AMT_CLOSE: 'amt_put_quote'})
        df_call = df_call.drop_duplicates(Util.AMT_APPLICABLE_STRIKE).reset_index(drop=True)
        df_put = df_put.drop_duplicates(Util.AMT_APPLICABLE_STRIKE).reset_index(drop=True)

        df = pd.merge(df_call[['amt_call_quote', Util.AMT_APPLICABLE_STRIKE]],
                      df_put[['amt_put_quote', Util.AMT_APPLICABLE_STRIKE]],
                      how='inner', on=Util.AMT_APPLICABLE_STRIKE)
        # df = pd.concat([df_call[['amt_call_quote']], df_put[['amt_put_quote']]], axis=1, join='inner', verify_integrity=True)
        df[Util.AMT_UNDERLYING_CLOSE] = df_put[Util.AMT_UNDERLYING_CLOSE].values[0]
        df['amt_cp_diff'] = abs(df['amt_call_quote'] - df['amt_put_quote'])
        maturitydt = df_put[Util.DT_MATURITY].values[0]
        df[Util.DT_MATURITY] = maturitydt
        ttm = ((maturitydt - eval_date).total_seconds() / 60.0) / (365.0 * 1440)
        df['amt_ttm'] = ttm
        df['amt_fv'] = math.exp(self.rf * (ttm))
        df = df.sort_values(by=Util.AMT_APPLICABLE_STRIKE).reset_index(drop=True)
        df['amt_delta_k'] = df[Util.AMT_APPLICABLE_STRIKE].diff() / 2.0
        # delta_k = df[self.util.col_applicable_strike].diff().mean()
        df.loc[0, 'amt_delta_k'] = df.loc[1, 'amt_delta_k']*2
        df.loc[len(df) - 1, 'amt_delta_k'] = df.loc[len(df) - 2, 'amt_delta_k']*2
        # df['amt_delta_k'] = df[self.util.col_applicable_strike].apply(lambda x: delta_k*0.5 if list(df[self.util.col_applicable_strike])[0]<x<list(df[self.util.col_applicable_strike])[-1] else delta_k)
        return df

    def forward_cboe(self, t_quotes, eval_date):
        # ATM strike k0 -- First strike below the forward index level, F
        # Forward price F -- F, by identifying the strike price at which the absolute difference
        # between the call and put prices is smallest.
        df = t_quotes.set_index(Util.AMT_APPLICABLE_STRIKE)
        mid_k = df.sort_values(by='amt_cp_diff', ascending=True).index[0]
        p_call = df.loc[mid_k, 'amt_call_quote']
        p_put = df.loc[mid_k, 'amt_put_quote']
        F = df.loc[mid_k, 'amt_fv'] * (p_call - p_put) + mid_k
        df['k-f'] = df.index - F
        if len(df[df['k-f'] < 0]) > 0:
            K0 = df[df['k-f'] < 0].sort_values(by='amt_cp_diff', ascending=True).index[0]
        else:
            K0 = df.sort_values(by='amt_cp_diff', ascending=True).index[0]
        return mid_k, K0, F

    def for_calculation(self, df, eval_date):
        mid_k, atm_k, F = self.forward_cboe(df, eval_date)
        df['atm_k'] = atm_k
        df['mid_k'] = mid_k
        df['F'] = F
        df['amt_otm_quote'] = df.apply(self.fun_otm_quote, axis=1)
        return df

    def calculate_S_for_skew(self, df):
        atm_k = df.loc[0, 'atm_k']
        F = df.loc[0, 'F']
        e1 = self.get_e1(F, atm_k)
        e2 = self.get_e2(F, atm_k)
        e3 = self.get_e3(F, atm_k)
        df['for_p1'] = df.apply(self.fun_for_p1, axis=1)
        df['for_p2'] = df.apply(self.fun_for_p2, axis=1)
        df['for_p3'] = df.apply(self.fun_for_p3, axis=1)
        print(df['for_p1'])
        print(df['for_p2'])
        print(df['for_p3'])
        fv = df.loc[0, 'amt_fv']
        p1 = -fv * df['for_p1'].sum() + e1
        p2 = fv * df['for_p2'].sum() + e2
        p3 = fv * df['for_p3'].sum() + e3
        S = self.get_S(p1, p2, p3)
        return S

    def calculate_sigma_for_vix(self, df):
        df['for_sigma'] = df.apply(self.fun_for_sigma, axis=1)
        K = df.loc[0, 'atm_k']
        F = df.loc[0, 'F']
        T = df.loc[0, 'amt_ttm']
        fv = df.loc[0, 'amt_fv']
        v1 = (1.0 / T) * (F / K - 1) ** 2
        # v1 = (F / K - 1) ** 2
        sum = df['for_sigma'].sum()
        sigma = (2.0 / T) * fv * sum - v1
        # sigma = 2.0 * fv * sum - v1
        return sigma

    def calculate(self, eval_date):
        # df_daily_state = self.bkt_optionset.df_data[
        #     self.bkt_optionset.df_data[self.util.col_date] == eval_date].reset_index(drop=True)
        df_daily_state = self.get_current_state()
        mdt = self.get_maturities_list()[0]
        if (mdt - self.eval_date).days <= 8:
            mdt1 = self.get_maturities_list()[1]
            mdt2 = self.get_maturities_list()[2]
        else:
            mdt1 = self.get_maturities_list()[0]
            mdt2 = self.get_maturities_list()[1]
        df_mdt1 = OptionUtil.get_df_by_mdt(df_daily_state, mdt1)
        df_mdt2 = OptionUtil.get_df_by_mdt(df_daily_state, mdt2)
        t_quotes1 = self.get_T_quotes(df_mdt1, eval_date)
        t_quotes2 = self.get_T_quotes(df_mdt2, eval_date)
        t_quotes1.to_csv('t_quotes1.csv')
        t_quotes2.to_csv('t_quotes2.csv')
        calculate1 = self.for_calculation(t_quotes1, eval_date)
        calculate2 = self.for_calculation(t_quotes2, eval_date)
        # print(t_quotes1)
        S1 = self.calculate_S_for_skew(calculate1)
        S2 = self.calculate_S_for_skew(calculate2)
        sigma1 = self.calculate_sigma_for_vix(calculate1)
        sigma2 = self.calculate_sigma_for_vix(calculate2)
        T1 = calculate1.loc[0, 'amt_ttm']
        T2 = calculate2.loc[0, 'amt_ttm']
        NT1 = (mdt1 - eval_date).total_seconds() / 60.0
        NT2 = (mdt2 - eval_date).total_seconds() / 60.0
        N30 = 30 * 1440.0
        N365 = 365 * 1440.0
        w = (NT2 - N30) / (NT2 - NT1)
        skew = 100 - 10 * (w * S1 + (1 - w) * S2)
        # W1 = T1 * w
        # W2 = T2 * (1-w)
        # x0 = T1 * sigma1 * w + T2 * sigma2 * (1 - w)
        # x1 = (T1 * sigma1 * w + T2 * sigma2 * (1 - w)) * N365 / N30
        vix = 100 * math.sqrt((T1 * sigma1 * w + T2 * sigma2 * (1 - w)) * N365 / N30)
        # vix = 100 * math.sqrt((sigma1 * w + sigma2 * (1 - w)) * N365 / N30)
        return vix, skew

    def run(self):
        self.df_res = pd.DataFrame()
        self.df_data = self.df_data[self.df_data[Util.NBR_MULTIPLIER]==10000]
        while self.current_index < self.nbr_index:
            eval_date = self.eval_date
            try:
                vix, skew = self.calculate(eval_date)
                if skew is not None and skew > 50 and skew < 200:
                    self.df_res.loc[eval_date,'skew'] = skew
                print("%10s %20s" % (eval_date, skew))

            except:
                pass
            # vol_1M_call = self.get_interpolated_atm_1M(self.util.type_call)
            # vol_1M_put = self.bkt_optionset.get_interpolated_atm_1M(self.util.type_put)
            if not self.has_next():break
            self.next()



print('=' * 100)
print("%10s %20s %20s" % ('date', 'vix', 'skew'))
print('-' * 100)
start_date = datetime.date(2018, 8, 6)
end_date = datetime.date(2018, 8, 6)
skew_indexing = SkewIndexing(start_date, end_date, 8)
skew_indexing.init()
skew_indexing.run()
# skew_indexing.df_res.to_csv('../skew.csv')