from back_test.bkt_account import BktAccount
from back_test.bkt_option_set import BktOptionSet
from back_test.bkt_instrument import BktInstrument
import QuantLib as ql
from back_test.bkt_util import BktUtil
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np


class BktOptionStrategy(object):

    __metaclass__=ABCMeta


    def __init__(self, df_option_metrics,cd_open_price='close', cd_close_price='close',
                 money_utilization=0.2,
                 init_fund=100000000.0, tick_size=0.0001,fee_rate=2.0 / 10000,
                 nbr_slippage=0, max_money_utilization=0.5,rf = 0.03,
                 leverage=1.0, margin_rate=0.1,contract_multiplier=10000
                 ):
        self.util = BktUtil()
        self.init_fund = init_fund
        self.money_utl = money_utilization
        self.df_option_metrics = df_option_metrics
        self.calendar = ql.China()
        self.bkt_account = BktAccount(cd_open_price=cd_open_price,cd_close_price = cd_close_price, leverage=leverage,
                                      margin_rate=margin_rate, init_fund=init_fund, tick_size=tick_size,
                                      contract_multiplier=contract_multiplier, fee_rate=fee_rate, nbr_slippage=nbr_slippage,rf = rf)
        self.bkt_optionset = BktOptionSet('daily', df_option_metrics)
        self.option_type = None
        self.min_holding_days = 1
        self.max_holding_days = 252
        self.moneyness_type = None
        self.trade_type = None
        self.min_volume = None
        self.flag_trade = False

    def set_min_holding_days(self, min_holding_days):
        self.min_holding_days = min_holding_days

    def set_max_holding_days(self, max_holding_days):
        self.max_holding_days = max_holding_days

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

        if self.min_holding_days != None:
            for option in option_set:
                if option not in candidate_set: continue
                min_maturity = self.util.to_dt_date(
                    self.calendar.advance(self.util.to_ql_date(eval_date), ql.Period(self.min_holding_days, ql.Days)))
                if option.maturitydt < min_maturity:
                    candidate_set.remove(option)

        if self.max_holding_days != None:
            for option in option_set:
                if option not in candidate_set: continue
                max_maturity = self.util.to_dt_date(
                    self.calendar.advance(self.util.to_ql_date(eval_date), ql.Period(self.max_holding_days, ql.Days)))
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

    def get_mdt1_candidate_set(self,eval_date,option_set):
        candidate_set = option_set.copy()
        maturities = sorted(self.bkt_optionset.eligible_maturities)
        min_maturity = self.util.to_dt_date(
            self.calendar.advance(self.util.to_ql_date(eval_date), ql.Period(self.min_holding_days, ql.Days)))
        mdt = maturities[0]
        for mdt in maturities:
            if mdt > min_maturity: break
        for option in option_set:
            if option not in candidate_set: continue
            if option.maturitydt != mdt:
                candidate_set.remove(option)
        return candidate_set

    def get_1st_eligible_maturity(self,eval_date):
        maturities = sorted(self.bkt_optionset.eligible_maturities)
        min_maturity = self.util.to_dt_date(
            self.calendar.advance(self.util.to_ql_date(eval_date), ql.Period(self.min_holding_days, ql.Days)))
        mdt = None
        for mdt in maturities:
            if mdt >= min_maturity: break
        return mdt

    def get_2nd_eligible_maturity(self,eval_date):
        maturities = sorted(self.bkt_optionset.eligible_maturities)
        min_maturity = self.util.to_dt_date(
            self.calendar.advance(self.util.to_ql_date(eval_date), ql.Period(self.min_holding_days, ql.Days)))
        mdt = None
        for mdt in maturities:
            if mdt >= min_maturity: break
        maturities_new = maturities[maturities.index(mdt):]
        mdt = maturities_new[1]
        return mdt

    def get_moving_average_signal(self,df,cd_short='ma_3',cd_long = 'ma_20'):
        df_short = df[df['cd_period']==cd_short].set_index('dt_date')
        df_long= df[df['cd_period']==cd_long].set_index('dt_date')
        df_long['short_ma'] = df_short['amt_value']
        df_long['short_minus_long'] = df_short['amt_value']-df_long['amt_value']
        df_long = df_long.rename(columns={'amt_value':'long_ma'})
        df_long['signal'] = df_long['short_minus_long']\
            .apply(lambda x: self.util.long if x>=0 else self.util.short)
        # df_long.to_csv('../ma_index.csv')
        return df_long

    "Read from database"
    def get_bollinger_signal(self,df,cd_long = '20'):
        df_long = df[df['cd_period'] == 'ma_'+cd_long].set_index('dt_date')
        df_std = df[df['cd_period'] == 'std_'+cd_long].set_index('dt_date')
        df_long['lower_sigma1'] = df_long['amt_value'] - df_std['amt_value']
        df_long['lower_sigma2'] = df_long['amt_value'] - 2*df_std['amt_value']
        df_long['upper_sigma1'] = df_long['amt_value'] + df_std['amt_value']
        df_long['upper_sigma2'] = df_long['amt_value'] + 2*df_std['amt_value']
        df_long['ma_3'] = df[df['cd_period'] == 'ma_3']['amt_value']
        # df_long.rename(columns={'amt_value':'ma_'+cd_long})
        return df_long

    "calculate"
    def get_bollinger_signal_2(self,df,start_date):
        df['amt_value'] = df['amt_close'].rolling(window=60).mean()
        df['amt_std'] = df['amt_close'].rolling(window=60).std()
        df['lower_sigma1'] = df['amt_value'] - df['amt_std']
        df['lower_sigma2'] = df['amt_value'] - 2*df['amt_std']
        df['upper_sigma1'] = df['amt_value'] + df['amt_std']
        df['upper_sigma2'] = df['amt_value'] + 2*df['amt_std']
        df_res = df[df['dt_date']>=start_date].set_index('dt_date')
        return df_res

    def get_percentile_signal(self,df,start_date=None):
        df['percentile_90'] = df['amt_close'].rolling(window=60).quantile(0.9)
        df['percentile_75'] = df['amt_close'].rolling(window=60).quantile(0.75)
        df['percentile_50'] = df['amt_close'].rolling(window=60).quantile(0.5)
        df['percentile_25'] = df['amt_close'].rolling(window=60).quantile(0.25)
        df['percentile_10'] = df['amt_close'].rolling(window=60).quantile(0.1)
        df['amt_std'] = df['amt_close'].rolling(window=60).std()
        df_res = df
        if start_date!= None:
            df_res = df[df['dt_date'] >= start_date]
        df_res = df_res.set_index('dt_date')
        return df_res

    def ma_signal(self,flag_current,amt_close,df):
        if flag_current == self.util.long:
            if df['signal'] == self.util.long: signal = None
            # if amt_close >= df['upper_sigma1']: signal = None
            elif df['signal'] == self.util.short: signal = self.util.short
            else: signal = self.util.neutrual
        elif flag_current == self.util.neutrual:
            if df['signal'] == self.util.long: signal = self.util.long
            elif df['signal'] == self.util.short: signal = self.util.short
            else: signal = None
        else:
            if df['signal'] == self.util.long: signal = self.util.long
            # if amt_close <= df['lower_sigma1']: signal = None
            elif df['signal'] == self.util.short: signal = None
            else: signal = self.util.neutrual
        return signal

    "Revert at upper/lower line"
    def boll_signal(self,flag_current,df):
        status = self.boll_status(df)
        if flag_current == status:
            signal = None
        else:
            signal = status
        return status,signal

    "Revert at mean"
    def boll_signal_2(self,flag_current,df):
        amt_close = df['amt_close']
        status = self.boll_status(df)
        if flag_current == self.util.long:
            if amt_close > df['amt_value']: signal = None
            # if amt_close >= df['upper_sigma2']: signal = None
            elif amt_close <= df['lower_sigma2']: signal = self.util.short
            else: signal = self.util.neutrual
        elif flag_current == self.util.neutrual:
            if amt_close >= df['upper_sigma2']: signal = self.util.long
            elif amt_close <= df['lower_sigma2']: signal = self.util.short
            else: signal = None
        else: # short
            # if amt_close < df['amt_value']: signal = None
            if amt_close <= df['lower_sigma2']: signal = None
            elif amt_close >= df['upper_sigma2']: signal = self.util.long
            else: signal = self.util.neutrual
        return status,signal

    def percentile_signal(self,flag_current,df,upper='percentile_90',lower='percentile_10',
                          revert='percentile_50'):
        amt_close = df['amt_close']
        status = self.boll_status(df,upper=upper,lower=lower)
        if flag_current == self.util.long:
            if amt_close > df[revert]: signal = None
            elif amt_close <= df[lower]: signal = self.util.short
            else: signal = self.util.neutrual
        elif flag_current == self.util.neutrual:
            if amt_close >= df[upper]: signal = self.util.long
            elif amt_close <= df[lower]: signal = self.util.short
            else: signal = None
        else: # short
            if amt_close <= df[revert]: signal = None
            elif amt_close >= df[lower]: signal = self.util.long
            else: signal = self.util.neutrual
        return status, signal


    def boll_status(self,df,upper='upper_sigma1',lower='lower_sigma1'):
        if df['amt_close'] >= df[upper]: signal = self.util.long
        elif df['amt_close'] <= df[lower]: signal = self.util.short
        else: signal = self.util.neutrual
        return signal

    def vol_bolliger_signal(self,flag_current,df):
        amt = df['amt_close']
        mean = df['amt_value']
        upper = df['upper_sigma1']
        lower = df['lower_sigma1']
        if amt >= upper: status = 2
        elif amt >= mean: status = 1
        elif amt >= lower: status = -1
        else: status = -2
        if flag_current == status: signal = None
        elif status == -2 or status == 2:
            signal = status
        elif status > flag_current:
            signal = self.util.long
        elif status <=flag_current:
            signal = self.util.short
        else:
            return
        return status,signal


    def util1(self,x):
        if x[0] >= x[2]:
            s = self.util.long
        elif x[0] <= x[1]:
            s = self.util.short
        else:
            s = self.util.neutrual
        return s

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

    def return_analysis(self,benckmark=None):
        ar = 100 * self.bkt_account.calculate_annulized_return()
        mdd = 100 * self.bkt_account.calculate_max_drawdown()
        return_yr, volatility_yr, sharpe = self.bkt_account.analysis()
        print('=' * 50)
        print("%20s %20s %20s  %20s" % ('annulized_return(%)', 'max_drawdown(%)','annualized volatility(%)','sharpe ratio'))
        print("%20s %20s %20s %20s" % (round(ar, 4), round(mdd, 4),round(volatility_yr,4),round(sharpe, 4)))
        print('-' * 50)
        self.bkt_account.plot_npv(benckmark)
        self.bkt_account.plot_drawdown()

    "calculate volatility surface skew"
    def ivs_ranked_run(self):
        bkt_optionset = self.bkt_optionset
        df_skew = pd.DataFrame()
        while bkt_optionset.index < len(bkt_optionset.dt_list) - 1:
            evalDate = bkt_optionset.eval_date
            cd_underlying_price = 'close'
            mdt = self.get_1st_eligible_maturity(evalDate)
            option_by_moneyness = self.bkt_optionset.update_options_by_moneyness(cd_underlying_price)
            optionset = option_by_moneyness[mdt]
            options_call = optionset[self.util.type_call]
            options_put = optionset[self.util.type_put]
            m_call = list(options_call.keys())
            m_put = list(options_put.keys())
            iv_call = []
            iv_put = []
            dt = []
            mdts = []
            for m in m_call:
                iv = options_call[m].get_implied_vol()
                iv_call.append(iv)
                dt.append(evalDate)
                mdts.append(mdt)
            for m1 in m_put:
                iv = options_put[m1].get_implied_vol()
                iv_put.append(iv)

            ivset = pd.DataFrame(data={'dt':dt,'mdt':mdt,'m_call':m_call,'m_put':m_put,
                          'iv_call':iv_call,'iv_put':iv_put})
            df_ivcall = ivset[ivset['m_call']<=0].sort_values(by='m_call',ascending=False).reset_index(drop=True).query('index <= 4')
            if len(df_ivcall) <= 1:
                otm_skew_call = np.nan
            else:
                df_diffcall = df_ivcall['iv_call'].diff()
                otm_skew_call = df_diffcall.sum()/(len(df_diffcall)-1)
            df_ivput = ivset[ivset['m_put']<=0].sort_values(by='m_put',ascending=False).reset_index(drop=True).query('index <= 4')
            if len(df_ivput) <= 1:
                otm_skew_put = np.nan
            else:
                df_diffput = df_ivput['iv_put'].diff()
                otm_skew_put = df_diffput.sum()/(len(df_diffput)-1)
            ivskew = pd.DataFrame(data={'dt':[evalDate],'mdt':[mdt],'otm_skew_call':[otm_skew_call],
                                        'otm_skew_put':[otm_skew_put]})
            df_skew = df_skew.append(ivskew,ignore_index=True)
            bkt_optionset.next()
        df_skew.to_csv('../save_results/df_skew_otm.csv')



class BktOptionIndex(BktOptionStrategy):

    def __init__(self, df_option, df_index, money_utilization = 0.2,init_fund = 100000000.0):
        self.validate_data(df_option, df_index)
        BktOptionStrategy.__init__(self,self.df_option,money_utilization=money_utilization,
                                   init_fund=init_fund)
        self.bkt_index = BktInstrument('daily',self.df_index)

    def validate_data(self,df_option, df_index):
        self.util = BktUtil()
        dates1 = df_option[self.util.dt_date].unique()
        dates2 = df_index[self.util.dt_date].unique()
        for (idx,dt) in enumerate(dates1):
            if dt != dates2[idx]:
                print(' Recheck dates! option dates and index dates are not equal !')
                self.df_option = None
                self.df_index = None
                return
        self.df_option = df_option
        self.df_index = df_index

    def next(self):
        self.bkt_optionset.next()
        self.bkt_index.next()

