import pandas as pd
from back_test.bkt_option import BktOption
from back_test.bkt_util import BktUtil
import QuantLib as ql
import numpy as np

class BktOptionSet(object):


    """
    Feature:

    To Collect BktOption Set
    To Calculate Vol Surface and Metrics
    To Manage Back Test State of all BktOption Objects

    """
    #TODO: 目前只能处理日频数据，高频数据还需调整结构进一步完善（目前注释未删的部分）


    def __init__(self, cd_frequency, df_option_metrics,hp,flag_calculate_iv=True,min_ttm =2,col_date='dt_date',col_datetime='dt_datetime',
                 pricing_type='OptionPlainEuropean', engine_type='AnalyticEuropeanEngine'):
        self.util = BktUtil()
        tmp = df_option_metrics.loc[0,self.util.id_instrument]
        self.option_code = tmp[0:tmp.index('_')]
        self.frequency = cd_frequency
        self.df_metrics = df_option_metrics
        self.pricing_type = pricing_type
        self.engine_type = engine_type
        self.flag_calculate_iv = flag_calculate_iv
        if self.option_code in ['sr', 'm']: self.flag_calculate_iv = False
        self.hp = hp
        self.min_ttm = min_ttm
        self.daycounter = ql.ActualActual()
        self.calendar = ql.China()
        self.bktoptionset = set()
        self.bktoptionset_call = []
        self.bktoptionset_put = []
        self.bktoptionset_atm = []
        self.bktoptionset_otm = []
        self.eligible_maturities = []
        self.atm_delta_min = 0.4
        self.atm_delta_max = 0.6
        self.index = 0
        self.update_multiplier_adjustment()
        self.start()


    def start(self):
        self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        self.start_date = self.dt_list[0] #0
        self.end_date = self.dt_list[-1] # len(self.dt_list)-1
        self.eval_date = self.start_date
        self.update_current_daily_state()
        self.update_eligible_maturities()
        self.update_bktoption()



    def next(self):
        if self.frequency in self.util.cd_frequency_low:
            self.update_eval_date()
            self.update_current_daily_state()
            self.update_eligible_maturities()
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


    def update_eval_date(self):
        self.index += 1
        self.eval_date = self.dt_list[self.dt_list.index(self.eval_date)+1]


    def update_current_daily_state(self):
        self.df_daily_state = self.df_metrics[self.df_metrics[self.util.col_date]==self.eval_date].reset_index()


    def update_eligible_maturities(self): # n: 要求合约剩余期限大于n（天）
        maturity_dates = self.df_daily_state[self.util.col_maturitydt].unique()
        maturity_dates2 = []
        for mdt in maturity_dates:
            if self.option_code in ['sr', 'm'] and mdt.month not in [1, 5, 9]: continue
            ttm = (mdt-self.eval_date).days
            if ttm > self.min_ttm : maturity_dates2.append(mdt)
        self.eligible_maturities = maturity_dates2

    def update_multiplier_adjustment(self):
        if self.option_code == '50etf':
            self.df_metrics[self.util.col_adj_strike] = \
                round(self.df_metrics[self.util.col_strike]*self.df_metrics[self.util.col_multiplier]/10000, 2)
            self.df_metrics[self.util.col_adj_option_price] = \
                round(self.df_metrics[self.util.col_close]*self.df_metrics[self.util.col_multiplier]/10000, 2)
        else:
            self.df_metrics[self.util.col_adj_strike] = self.df_metrics[self.util.col_strike]
            self.df_metrics[self.util.col_adj_option_price] = self.df_metrics[self.util.col_close]



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

    # def collect_delta(self,bktoption_list):
    #     df = pd.DataFrame()
    #     for idx,option in enumerate(bktoption_list):
    #         if self.frequency in self.util.cd_frequency_low:
    #             df.loc[idx, self.util.col_date] = self.eval_date
    #         df.loc[idx, self.util.col_id_instrument] = option.id_instrument
    #         df.loc[idx, self.util.col_adj_strike] = option.adj_strike
    #         df.loc[idx, self.util.col_option_type] = option.option_type
    #         df.loc[idx, self.util.col_maturitydt] = option.maturitydt
    #         df.loc[idx, self.util.col_implied_vol] = option.get_implied_vol()
    #         df.loc[idx, self.util.col_delta] = option.get_delta()
    #     return df
    #
    # def collect_theta(self,bktoption_list):
    #     df = pd.DataFrame()
    #     for idx,option in enumerate(bktoption_list):
    #         if self.frequency in self.util.cd_frequency_low:
    #             df.loc[idx, self.util.col_date] = self.eval_date
    #         df.loc[idx, self.util.col_id_instrument] = option.id_instrument
    #         df.loc[idx, self.util.col_adj_strike] = option.adj_strike
    #         df.loc[idx, self.util.col_option_type] = option.option_type
    #         df.loc[idx, self.util.col_maturitydt] = option.maturitydt
    #         df.loc[idx, self.util.col_implied_vol] = option.get_implied_vol()
    #
    #         df.loc[idx, self.util.col_theta] = option.get_theta()
    #     return df
    #
    # def collect_vega(self,bktoption_list):
    #     df = pd.DataFrame()
    #     for idx,option in enumerate(bktoption_list):
    #         if self.frequency in self.util.cd_frequency_low:
    #             df.loc[idx, self.util.col_date] = self.eval_date
    #         df.loc[idx, self.util.col_id_instrument] = option.id_instrument
    #         df.loc[idx, self.util.col_adj_strike] = option.adj_strike
    #         df.loc[idx, self.util.col_option_type] = option.option_type
    #         df.loc[idx, self.util.col_maturitydt] = option.maturitydt
    #         df.loc[idx, self.util.col_implied_vol] = option.get_implied_vol()
    #         df.loc[idx, self.util.col_vega] = option.get_vega()
    #     return df
    #
    #
    # def collect_carry(self,bktoption_list):
    #     df = pd.DataFrame()
    #     bvs_call = self.get_volsurface_squre('call')
    #     bvs_put = self.get_volsurface_squre('put')
    #     res = []
    #     for idx,option in enumerate(bktoption_list):
    #         if option.maturitydt not in self.eligible_maturities: continue
    #         iv = option.get_implied_vol()
    #         if option.option_type == self.util.type_call:
    #             carry, theta, vega, iv_roll_down = option.get_carry(bvs_call, self.hp)
    #         else:
    #             carry, theta, vega, iv_roll_down = option.get_carry(bvs_put, self.hp)
    #         if np.isnan(carry): carry = -999.0
    #         if np.isnan(theta): theta = -999.0
    #         if np.isnan(vega): vega = -999.0
    #         if np.isnan(iv_roll_down): iv_roll_down = -999.0
    #         if self.frequency in self.util.cd_frequency_low:
    #             df.loc[idx, self.util.col_date] = self.eval_date
    #         df.loc[idx, self.util.col_id_instrument] = option.id_instrument
    #         df.loc[idx, self.util.col_code_instrument] = option.code_instrument
    #         df.loc[idx, self.util.col_adj_strike] = option.adj_strike
    #         df.loc[idx, self.util.col_option_type] = option.option_type
    #         df.loc[idx, self.util.col_maturitydt] = option.maturitydt
    #         df.loc[idx, self.util.col_implied_vol] = iv
    #         df.loc[idx, self.util.col_option_price] = option.option_price
    #         df.loc[idx, self.util.col_carry] = carry
    #         db_row = {
    #             self.util.col_date:self.eval_date,
    #             self.util.col_id_instrument:option.id_instrument,
    #             self.util.nbr_invest_days:self.hp,
    #             self.util.col_code_instrument:option.code_instrument,
    #             self.util.col_adj_strike:float(option.adj_strike),
    #             self.util.col_option_type:option.option_type,
    #             self.util.col_maturitydt:option.maturitydt,
    #             self.util.col_implied_vol:float(iv),
    #             self.util.col_option_price:float(option.option_price),
    #             self.util.col_carry:float(carry),
    #             self.util.col_theta:float(theta),
    #             self.util.col_vega:float(vega),
    #             self.util.col_iv_roll_down:float(iv_roll_down)
    #         }
    #         res.append(db_row)
    #     return df,res

    """Separate Call/Put Vol Surface"""
    def rank_by_carry1(self,bktoption_list):

        df = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
        if len(bktoption_list)==0 : return df
        bvs_call = self.get_volsurface_squre('call')
        bvs_put = self.get_volsurface_squre('put')
        for idx,option in enumerate(bktoption_list):
            if option.maturitydt not in self.eligible_maturities: continue
            if option.option_type == self.util.type_call:
                carry, theta, vega, iv_roll_down = option.get_carry(bvs_call, self.hp)
            else:
                carry, theta, vega, iv_roll_down = option.get_carry(bvs_put, self.hp)
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
    def rank_by_carry2(self,bktoption_list):
        df = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
        if len(bktoption_list)==0 : return df
        bvs = self.get_mid_volsurface_squre()
        for idx,option in enumerate(bktoption_list):
            if option.maturitydt not in self.eligible_maturities: continue
            carry, theta, vega, iv_roll_down = option.get_carry(bvs, self.hp)
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



    def collect_option_metrics(self):
        res = []
        df = pd.DataFrame(columns=[self.util.col_date,self.util.col_carry,self.util.bktoption])
        bktoption_list = self.bktoptionset
        if len(bktoption_list)==0 : return df
        bvs_call = self.get_volsurface_squre('call')
        bvs_put = self.get_volsurface_squre('put')
        for idx,option in enumerate(bktoption_list):
            if option.option_price > 0.0:
                if option.option_type == self.util.type_call:
                    carry, theta, vega, iv_roll_down = option.get_carry(bvs_call, self.hp)
                else:
                    carry, theta, vega, iv_roll_down = option.get_carry(bvs_put, self.hp)
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

            db_row = {
                self.util.col_date: self.eval_date,
                self.util.col_id_instrument: option.id_instrument,
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
            }
            res.append(db_row)
        return res









































