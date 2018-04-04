import pandas as pd
from back_test.bkt_option import BktOption
from back_test.bkt_util import BktUtil
import QuantLib as ql
import numpy as np
import datetime

class BktOptionSet(object):

    """
    Feature:

    To Collect BktOption Set
    To Calculate Vol Surface and Metrics
    To Manage Back Test State of all BktOption Objects
    #TODO: 目前只能处理日频数据，高频数据还需调整结构进一步完善（目前注释未删的部分）
    """

    def __init__(self, cd_frequency, df_option_metrics,flag_calculate_iv=True,min_ttm =2,col_date='dt_date',col_datetime='dt_datetime',
                 pricing_type='OptionPlainEuropean', engine_type='AnalyticEuropeanEngine'):
        self.util = BktUtil()
        tmp = df_option_metrics.loc[0,self.util.id_instrument]
        self.option_code = tmp[0:tmp.index('_')]
        self.frequency = cd_frequency
        self.df_metrics = df_option_metrics
        self.pricing_type = pricing_type
        self.engine_type = engine_type
        self.flag_calculate_iv = flag_calculate_iv
        if self.option_code =='sr': self.flag_calculate_iv = False
        # self.hp = hp
        self.min_ttm = min_ttm
        self.daycounter = ql.ActualActual()
        self.calendar = ql.China()
        self.bktoptionset = set()
        self.bktoptions_mdts = {}
        self.bktoptionset_mdt1 = {}
        self.bktoptionset_mdt2 = {}
        self.bktoptionset_mdt3 = {}
        self.bktoptionset_mdt4 = {}
        self.bktoptionset_call = set()
        self.bktoptionset_put = set()
        self.bktoptionset_atm = set()
        self.bktoptionset_otm = set()
        self.eligible_maturities = []
        self.atm_delta_min = 0.4
        self.atm_delta_max = 0.6
        self.index = 0
        self.start()


    def start(self):
        self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        self.start_date = self.dt_list[0] #0
        self.end_date = self.dt_list[-1] # len(self.dt_list)-1
        self.eval_date = self.start_date
        self.validate_data()
        self.update_multiplier_adjustment()
        self.update_current_daily_state()
        self.update_eligible_maturities()
        self.update_bktoptionset_mdts()
        self.update_bktoption()


    def next(self):
        if self.frequency in self.util.cd_frequency_low:
            self.update_eval_date()
            self.update_current_daily_state()
            self.update_eligible_maturities()
            self.update_bktoptionset_mdts()
        self.update_bktoption()


    def update_bktoption(self):

        if self.frequency in self.util.cd_frequency_low:
            bkt_ids = []
            bktoption_list = []
            bktoption_list_call = []
            bktoption_list_put = []
            # bktoption_list_atm = []
            # bktoption_list_otm = []
            # bktoption_list_mdt1 = []
            df_current = self.df_daily_state
            option_ids = df_current[self.util.col_id_instrument].unique()
            for bktoption in self.bktoptionset:
                if bktoption.id_instrument in option_ids :
                    bktoption.next()
                    bktoption_list.append(bktoption)
                    if bktoption.option_type == 'call' : bktoption_list_call.append(bktoption)
                    else : bktoption_list_put.append(bktoption)
                    # delta = bktoption.get_delta()
                    # if abs(delta) < self.atm_delta_max and abs(delta) > self.atm_delta_min:
                    #     bktoption_list_atm.append(bktoption)
                    # else:
                    #     bktoption_list_otm.append(bktoption)
                    bkt_ids.append(bktoption.id_instrument)
            for optionid in option_ids:
                if optionid in bkt_ids: continue
                df_option = self.df_metrics[self.df_metrics[self.util.col_id_instrument] == optionid].reset_index()
                bktoption = BktOption(self.frequency, df_option,self.flag_calculate_iv,id_instrument=optionid)
                bktoption.start()
                bktoption_list.append(bktoption)
                if bktoption.option_type == 'call':
                    bktoption_list_call.append(bktoption)
                else:
                    bktoption_list_put.append(bktoption)
                # delta = bktoption.get_delta()
                # if abs(delta) < self.atm_delta_max and abs(delta) > self.atm_delta_min:
                #     bktoption_list_atm.append(bktoption)
                # else:
                #     bktoption_list_otm.append(bktoption)
                bkt_ids.append(optionid)
            self.bktoptionset = set(bktoption_list)
            self.bktoptionset_call = set(bktoption_list_call)
            self.bktoptionset_put = set(bktoption_list_put)
            # self.bktoptionset_atm = set(bktoption_list_atm)
            # self.bktoptionset_otm = set(bktoption_list_otm)


    def update_bktoptionset_mdts(self):
        if len(self.eligible_maturities)==0:
            print(self.eval_date,' No eligible maturities')
            return
        dict = {}
        for mdt in self.eligible_maturities:
            options = []
            for option in self.bktoptionset:
                if option.maturitydt == mdt:
                    options.append(option)
            dict.update({mdt:options})
        self.bktoptions_mdts = dict



    def update_eval_date(self):
        self.index += 1
        self.eval_date = self.dt_list[self.dt_list.index(self.eval_date)+1]


    def update_current_daily_state(self):
        self.df_daily_state = self.df_metrics[self.df_metrics[self.util.col_date]==self.eval_date].reset_index()


    def validate_data(self):
        underlyingids = self.df_metrics[self.util.col_id_underlying].unique()
        for underlying_id in underlyingids:
            c = self.df_metrics[self.util.col_id_underlying] == underlying_id
            df_tmp = self.df_metrics[c]
            mdt = df_tmp[self.util.col_maturitydt].values[0]
            """Check Null Maturity"""
            if pd.isnull(mdt):
                m1 = int(underlying_id[-2:])
                y1 = int(str(20) + underlying_id[-4:-2])
                dt1 = ql.Date(1,m1,y1)
                if self.option_code == 'sr':
                    mdt = self.util.to_dt_date(self.calendar.advance(dt1,ql.Period(-5,ql.Days)))
                elif self.option_code == 'm':
                    tmp = self.calendar.advance(dt1,ql.Period(-1,ql.Months))
                    mdt = self.util.to_dt_date(self.calendar.advance(tmp,ql.Period(5,ql.Days)))
                self.df_metrics.loc[c,self.util.col_maturitydt] = mdt
        for (idx,row) in self.df_metrics.iterrows():
            """Check Null Option Type"""
            option_type = row[self.util.col_option_type]
            id_instrument = row[self.util.col_id_instrument]
            if pd.isnull(option_type):
                if self.option_code in ['sr', 'm']:
                    if id_instrument[-6] == 'c':
                        option_type = self.util.type_call
                    elif id_instrument[-6] == 'p':
                        option_type = self.util.type_put
                    else:
                        print(id_instrument,',',id_instrument[-6])
                        continue
                    self.df_metrics.loc[idx, self.util.col_option_type] = option_type
                else:
                    continue
            """Check Null Strike"""
            strike = row[self.util.col_strike]
            if pd.isnull(strike):
                if self.option_code in ['sr', 'm']:
                    strike = float(id_instrument[-4:])
                    self.df_metrics.loc[idx, self.util.col_strike] = strike
                else:
                    continue
            """Check Null Multuplier"""
            multiplier = row[self.util.col_multiplier]
            if pd.isnull(multiplier):
                if self.option_code in ['sr','m']:
                    multiplier = 10
                else:
                    multiplier = 10000
                self.df_metrics.loc[idx, self.util.col_multiplier] = multiplier


    def update_eligible_maturities(self): # n: 要求合约剩余期限大于n（天）
        underlyingids = self.df_daily_state[self.util.col_id_underlying].unique()
        maturities = self.df_daily_state[self.util.col_maturitydt].unique()
        maturity_dates2 = []

        if self.option_code in ['sr', 'm']:
            for underlying_id in underlyingids:
                m1 = int(underlying_id[-2:])
                if m1 not in [1, 5, 9]:
                    continue
                c = self.df_daily_state[self.util.col_id_underlying] == underlying_id
                df_tmp = self.df_daily_state[c]
                mdt = df_tmp[self.util.col_maturitydt].values[0]
                ttm = (mdt - self.eval_date).days
                if ttm > self.min_ttm:
                    maturity_dates2.append(mdt)
        else:
            for mdt in maturities:
                ttm = (mdt - self.eval_date).days
                if ttm > self.min_ttm:
                    maturity_dates2.append(mdt)
        self.eligible_maturities = maturity_dates2

    def update_multiplier_adjustment(self):
        if self.option_code == '50etf':
            self.df_metrics[self.util.col_adj_strike] = \
                round(self.df_metrics[self.util.col_strike]*self.df_metrics[self.util.col_multiplier]/10000, 2)
            self.df_metrics[self.util.col_adj_option_price] = \
                round(self.df_metrics[self.util.col_settlement]*self.df_metrics[self.util.col_multiplier]/10000, 2)
        else:
            self.df_metrics[self.util.col_adj_strike] = self.df_metrics[self.util.col_strike]
            self.df_metrics[self.util.col_adj_option_price] = self.df_metrics[self.util.col_settlement]

    def get_duplicate_strikes_dropped(self,df_metrics):
        maturities = sorted(df_metrics[self.util.col_maturitydt].unique())
        df = pd.DataFrame()
        for mdt in maturities:
            df_mdt_call = df_metrics[(df_metrics[self.util.col_maturitydt]==mdt) &
                                     (df_metrics[self.util.col_option_type]=='call')]\
                            .sort_values(by=self.util.col_trading_volume, ascending=False) \
                            .drop_duplicates(subset=[self.util.col_adj_strike])
            df_mdt_put = df_metrics[(df_metrics[self.util.col_maturitydt] == mdt) &
                                    (df_metrics[self.util.col_option_type] == 'put')]\
                            .sort_values(by=self.util.col_trading_volume, ascending=False) \
                            .drop_duplicates(subset=[self.util.col_adj_strike])
            df = df.append(df_mdt_call,ignore_index=True)
            df = df.append(df_mdt_put,ignore_index=True)
        return df

    def add_dtdate_column(self):
        if self.util.col_date not in self.df_metrics.columns.tolist():
            for (idx,row) in self.df_metrics.iterrows():
                self.df_metrics.loc[idx, self.util.col_date] = row[self.util.col_datetime].date()

    def get_straddle(self,moneyness_rank,mdt):
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档
        optionset = self.bktoptions_mdts[mdt]
        res = self.order_by_moneyness(optionset)
        option_atm_call = res[self.util.type_call][moneyness_rank]
        option_atm_put = res[self.util.type_put][moneyness_rank]
        delta_call = option_atm_call.get_delta()
        delta_put = option_atm_put.get_delta()
        res = [
            {self.util.id_instrument:option_atm_call.id_instrument,
               self.util.unit:1,
               self.util.bktoption:option_atm_call},
            {self.util.id_instrument: option_atm_put.id_instrument,
             self.util.unit: -delta_call/delta_put,
             self.util.bktoption: option_atm_put}
               ]
        df_delta0 = pd.DataFrame(res)
        return df_delta0

    def get_put(self,moneyness_rank,mdt):
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档
        optionset = self.bktoptions_mdts[mdt]
        res = self.order_by_moneyness(optionset)
        option_atm_put = res[self.util.type_put][moneyness_rank]
        res = [
            {self.util.id_instrument: option_atm_put.id_instrument,
             self.util.unit: 1,
             self.util.bktoption: option_atm_put}
               ]
        df_delta0 = pd.DataFrame(res)
        return df_delta0

    def get_call(self,moneyness_rank,mdt):
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档
        optionset = self.bktoptions_mdts[mdt]
        res = self.order_by_moneyness(optionset)
        option_atm_call = res[self.util.type_call][moneyness_rank]
        res = [
            {self.util.id_instrument: option_atm_call.id_instrument,
             self.util.unit: 1,
             self.util.bktoption: option_atm_call}
               ]
        df_delta0 = pd.DataFrame(res)
        return df_delta0

    """ Input optionset with the same maturity,get dictionary order by moneynesses as keys """
    def order_by_moneyness(self,optionset_mdt):
        dict_call = {}
        dict_put = {}
        res_call = {}
        res_put = {}
        atm_call = 1000
        atm_put = -1000
        spot = optionset_mdt[0].underlying_price
        for option in optionset_mdt:
            if option.option_type == self.util.type_call:
                k = option.strike
                m = round(k-spot,6)
                if m >=0 :
                    atm_call = min(atm_call,m)
                dict_call.update({m:option})
            else:
                k = option.strike
                m = round(k-spot,6)
                if m <=0:
                    atm_put = max(atm_put,m)
                dict_put.update({m:option})
        keys_call = sorted(dict_call)
        keys_put = sorted(dict_put)
        idx_call = keys_call.index(atm_call)
        idx_put = keys_put.index(atm_put)
        for (i,key) in enumerate(keys_call):
            res_call.update({i-idx_call:dict_call[key]})
        for (i,key) in enumerate(keys_put):
            res_put.update({i-idx_put:dict_put[key]})
        res_callput = {self.util.type_call:res_call,self.util.type_put:res_put}
        return res_callput

    """ Get Call/Put volatility surface separately"""
    def get_volsurface_squre(self,option_type):
        ql_maturities = []
        option_list = []
        for option in self.bktoptionset:
            mdt = option.maturitydt
            ttm = (mdt-self.eval_date).days
            cd_type = option.option_type
            if cd_type == option_type:
                option_list.append(option)
        df = self.get_duplicate_strikes_dropped(self.collect_implied_vol(option_list))
        df_mdt_list = []
        iv_name_list = []
        maturity_list = []
        for idx,mdt in enumerate(self.eligible_maturities):
            iv_rename = 'implied_vol_'+str(idx)
            df_mkt = df[(df[self.util.col_maturitydt]==mdt)] \
                .rename(columns={self.util.col_implied_vol: iv_rename}).set_index(self.util.col_adj_strike)
            if len(df_mkt) == 0: continue
            df_mdt_list.append(df_mkt)
            iv_name_list.append(iv_rename)
            maturity_list.append(mdt)
        df_vol = pd.concat(df_mdt_list, axis=1, join='inner')
        strikes = []
        for k in df_vol.index:
            strikes.append(float(k))
        volset = []
        for name in iv_name_list:
            volset.append(df_vol[name].tolist())
        for mdate in maturity_list:
            ql_maturities.append(ql.Date(mdate.day, mdate.month, mdate.year))
        vol_matrix = ql.Matrix(len(strikes), len(maturity_list))
        for i in range(vol_matrix.rows()):
            for j in range(vol_matrix.columns()):
                vol_matrix[i][j] = volset[j][i]
        ql_evalDate = self.util.to_ql_date(self.eval_date)
        black_var_surface = ql.BlackVarianceSurface(
            ql_evalDate, self.calendar, ql_maturities, strikes, vol_matrix, self.daycounter)
        return black_var_surface

    """ Get Integrate Volatility Surface by call/put mid vols"""
    def get_mid_volsurface_squre(self):
        ql_maturities = []
        call_list = []
        put_list = []
        df_mdt_list = []
        iv_name_list = []
        maturity_list = []
        for option in self.bktoptionset:
            if option.option_type == self.util.type_call:
                call_list.append(option)
            else:
                put_list.append(option)
        df_call = self.get_duplicate_strikes_dropped(self.collect_implied_vol(call_list))
        df_put = self.get_duplicate_strikes_dropped(self.collect_implied_vol(put_list))
        df_call['maturity_call'] = df_call[self.util.col_maturitydt]
        df_call['adj_strike_call'] = df_call[self.util.col_adj_strike]
        df_call = df_call.set_index([self.util.col_maturitydt,self.util.col_adj_strike])\
                .rename(columns={self.util.col_implied_vol: 'iv_call'})
        df_put = df_put.set_index([self.util.col_maturitydt, self.util.col_adj_strike]) \
                .rename(columns={self.util.col_implied_vol: 'iv_put'})
        df = df_call[['adj_strike_call','maturity_call','iv_call']]\
            .join(df_put[['iv_put']])
        df['mid_vol'] = (df['iv_call']+df['iv_put'])/2
        maturities = sorted(df['maturity_call'].unique())
        for idx,mdt in enumerate(maturities):
            if mdt <= self.eval_date: continue
            iv_rename = 'implied_vol_'+str(idx)
            df_mkt = df[(df['maturity_call']==mdt)] \
                .rename(columns={'mid_vol': iv_rename}).sort_values(by='adj_strike_call').set_index('adj_strike_call')
            if len(df_mkt) == 0: continue
            df_mdt_list.append(df_mkt)
            iv_name_list.append(iv_rename)
            maturity_list.append(mdt)
        df_vol = pd.concat(df_mdt_list, axis=1, join='inner')
        strikes = []
        for k in df_vol.index:
            strikes.append(float(k))
        volset = []
        for name in iv_name_list:
            volset.append(df_vol[name].tolist())
        for mdate in maturity_list:
            ql_maturities.append(ql.Date(mdate.day, mdate.month, mdate.year))
        vol_matrix = ql.Matrix(len(strikes), len(maturity_list))
        for i in range(vol_matrix.rows()):
            for j in range(vol_matrix.columns()):
                vol_matrix[i][j] = volset[j][i]
        ql_evalDate = self.util.to_ql_date(self.eval_date)
        black_var_surface = ql.BlackVarianceSurface(
                ql_evalDate, self.calendar, ql_maturities, strikes, vol_matrix, self.daycounter)
        return black_var_surface


    def collect_implied_vol(self,bktoption_list):
        df = pd.DataFrame()
        for idx,option in enumerate(bktoption_list):
            if self.frequency in self.util.cd_frequency_low:
                df.loc[idx, self.util.col_date] = self.eval_date
            df.loc[idx, self.util.col_id_instrument] = option.id_instrument
            df.loc[idx, self.util.col_adj_strike] = option.adj_strike
            df.loc[idx, self.util.col_option_type] = option.option_type
            df.loc[idx, self.util.col_maturitydt] = option.maturitydt
            iv = option.get_implied_vol()
            df.loc[idx, self.util.col_implied_vol] = iv
            df.loc[idx, self.util.col_trading_volume] = option.get_trading_volume()
        return df

    """Separate Call/Put Vol Surface"""
    def rank_by_carry1(self,bktoption_list,hp=20):

        df = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
        if len(bktoption_list)==0 : return df
        bvs_call = self.get_volsurface_squre('call')
        bvs_put = self.get_volsurface_squre('put')
        for idx,option in enumerate(bktoption_list):
            if option.maturitydt not in self.eligible_maturities: continue
            if option.option_type == self.util.type_call:
                carry, theta, vega, iv_roll_down = option.get_carry(bvs_call, hp)
            else:
                carry, theta, vega, iv_roll_down = option.get_carry(bvs_put, hp)
            if carry == None or np.isnan(carry): carry = -999.0
            if theta == None or  np.isnan(theta): theta = -999.0
            if vega == None or  np.isnan(vega): vega = -999.0
            if iv_roll_down == None or  np.isnan(iv_roll_down): iv_roll_down = -999.0
            if self.frequency in self.util.cd_frequency_low:
                df.loc[idx, self.util.col_date] = self.eval_date
            option.carry = carry
            option.theta = theta
            option.vega = vega
            option.iv_roll_down = iv_roll_down
            df.loc[idx, self.util.col_carry] = carry
            df.loc[idx,self.util.bktoption] = option
            df.loc[idx,self.util.col_adj_strike] = option.adj_strike
            df.loc[idx,self.util.col_maturitydt] = option.maturitydt
            df.loc[idx,self.util.col_option_type] = option.option_type
            df.loc[idx,self.util.col_trading_volume] = option.get_trading_volume()
        df = self.get_duplicate_strikes_dropped(df)
        df = df[df[self.util.col_carry] != -999.0]
        df = df.sort_values(by=self.util.col_carry,ascending=False)
        return df

    """Use Mid Call/Put Integrated Vol Surface"""
    def rank_by_carry2(self,bktoption_list,hp=20):
        df = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
        if len(bktoption_list)==0 : return df
        bvs = self.get_mid_volsurface_squre()
        for idx,option in enumerate(bktoption_list):
            if option.maturitydt not in self.eligible_maturities: continue
            carry, theta, vega, iv_roll_down = option.get_carry(bvs, hp)
            if carry == None or np.isnan(carry): carry = -999.0
            if theta == None or  np.isnan(theta): theta = -999.0
            if vega == None or  np.isnan(vega): vega = -999.0
            if iv_roll_down == None or  np.isnan(iv_roll_down): iv_roll_down = -999.0
            if self.frequency in self.util.cd_frequency_low:
                df.loc[idx, self.util.col_date] = self.eval_date
            option.carry = carry
            option.theta = theta
            option.vega = vega
            option.iv_roll_down = iv_roll_down
            delta = option.get_delta()
            df.loc[idx, self.util.col_carry] = carry
            df.loc[idx, self.util.col_delta] = delta
            df.loc[idx,self.util.bktoption] = option
            df.loc[idx,self.util.col_adj_strike] = option.adj_strike
            df.loc[idx,self.util.col_maturitydt] = option.maturitydt
            df.loc[idx,self.util.col_option_type] = option.option_type
            df.loc[idx,self.util.col_trading_volume] = option.get_trading_volume()
        df = self.get_duplicate_strikes_dropped(df)
        df = df[df[self.util.col_carry] != -999.0]
        df = df.sort_values(by=self.util.col_carry,ascending=False)
        return df

    def collect_option_metrics(self,hp=20):
        res = []
        df = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
        bktoption_list = self.bktoptionset
        if len(bktoption_list)==0 : return df
        bvs_call = self.get_volsurface_squre('call')
        bvs_put = self.get_volsurface_squre('put')
        for idx,option in enumerate(bktoption_list):
            if option.option_price > 0.0:
                if option.option_type == self.util.type_call:
                    carry, theta, vega, iv_roll_down = option.get_carry(bvs_call, hp)
                else:
                    carry, theta, vega, iv_roll_down = option.get_carry(bvs_put, hp)
                theta = option.get_theta()
                vega = option.get_vega()
                iv = option.get_implied_vol()
                delta = option.get_delta()
                rho = option.get_rho()
                gamma = option.get_gamma()
                if carry == None or np.isnan(carry): carry = -999.0
                if theta == None or np.isnan(theta): theta = -999.0
                if vega == None or np.isnan(vega): vega = -999.0
                if gamma == None or np.isnan(gamma): gamma = -999.0
                if iv == None or np.isnan(iv): iv = -999.0
                if delta == None or np.isnan(delta): delta = -999.0
                if rho == None or np.isnan(rho): rho = -999.0
            else:
                iv = option.get_implied_vol()
                carry =theta =vega =gamma= delta =rho = -999.0
                if iv == None or np.isnan(iv): iv = -999.0
            if self.flag_calculate_iv: datasource = 'calculated'
            else:
                if self.option_code == 'm':datasource = 'dce'
                else:datasource = 'czce'
            db_row = {
                self.util.col_date: self.eval_date,
                self.util.col_id_instrument: option.id_instrument,
                'datasource':datasource,
                'name_code':self.option_code,
                'id_underlying':option.id_underlying,
                'amt_strike':float(option.strike),
                self.util.col_code_instrument: option.code_instrument,
                self.util.col_option_type: option.option_type,
                self.util.col_maturitydt: option.maturitydt,
                self.util.col_implied_vol: float(iv),
                self.util.col_adj_strike: float(option.adj_strike),
                self.util.col_option_price: float(option.option_price),
                'amt_delta':float(delta),
                self.util.col_vega: float(vega),
                self.util.col_theta: float(theta),
                'amt_rho':float(rho),
                'amt_gamma':float(gamma),
                'amt_carry_1M': float(carry),
                'timestamp':datetime.datetime.today()
            }
            res.append(db_row)
        return res








































