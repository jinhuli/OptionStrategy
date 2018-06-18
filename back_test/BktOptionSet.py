import pandas as pd
from back_test.BktOption import BktOption
import QuantLib as ql
import datetime
from back_test.OptionPortfolio import *


class BktOptionSet(object):
    """
    Feature:

    To Collect BktOption Set
    To Calculate Vol Surface and Metrics
    To Manage Back Test State of all BktOption Objects
    """

    def __init__(self, df_option_metrics, cd_frequency='daily', flag_calculate_iv=True, min_ttm=2,
                 pricing_type='OptionPlainEuropean', engine_type='AnalyticEuropeanEngine', rf=0.03):
        self.util = BktUtil()
        tmp = df_option_metrics.loc[0, self.util.id_instrument]
        self.option_code = tmp[0: tmp.index('_')]
        self.frequency = cd_frequency
        self.df_data = df_option_metrics
        self.pricing_type = pricing_type
        self.engine_type = engine_type
        self.flag_calculate_iv = flag_calculate_iv
        self.min_ttm = min_ttm
        self.rf = rf
        if self.option_code == 'sr': self.flag_calculate_iv = False
        self.daycounter = ql.ActualActual()
        self.calendar = ql.China()
        self.bktoptionset = set()
        self.eligible_maturities = []
        self.index = 0
        self.start()

    def start(self):
        self.dt_list = sorted(self.df_data[self.util.col_date].unique())
        self.start_date = self.dt_list[0]  # 0
        self.end_date = self.dt_list[-1]  # len(self.dt_list)-1
        self.eval_date = self.start_date
        self.validate_data()
        # self.add_bktoption_column()
        self.df_last_state = pd.DataFrame()
        self.update_adjustment()
        self.update_current_daily_state()
        self.update_eligible_maturities()
        self.update_bktoption()


    def next(self):
        if self.frequency in self.util.cd_frequency_low:
            self.df_last_state = self.df_daily_state
            self.update_eval_date()
            self.update_current_daily_state()
            self.update_eligible_maturities()
        self.update_bktoption()

    def update_eval_date(self):
        self.index += 1
        self.eval_date = self.dt_list[self.dt_list.index(self.eval_date) + 1]

    def update_current_daily_state(self):
        self.df_daily_state = self.df_data[self.df_data[self.util.col_date] == self.eval_date].reset_index(drop=True)

    """ Update bktoption in daily state """

    def update_bktoption(self):
        if self.frequency in self.util.cd_frequency_low:
            df_last_state = self.df_last_state
            ids_last = []
            if not df_last_state.empty:
                ids_last = df_last_state[self.util.id_instrument].tolist()
            for (idx, row) in self.df_daily_state.iterrows():
                id_inst = row[self.util.id_instrument]
                if id_inst in ids_last:
                    bktoption = \
                        df_last_state[df_last_state[self.util.id_instrument] == id_inst][self.util.bktoption].values[0]
                    bktoption.next()
                    self.df_daily_state.loc[idx, self.util.bktoption] = bktoption
                else:
                    df_option = self.df_data[self.df_data[self.util.col_id_instrument] == id_inst].reset_index(
                        drop=True)
                    bktoption = BktOption(df_option, self.flag_calculate_iv, rf=self.rf)
                    self.df_daily_state.loc[idx, self.util.bktoption] = bktoption
            self.bktoptionset = set(self.df_daily_state[self.util.bktoption].tolist())


    def validate_data(self):
        self.df_data[self.util.col_option_price] = self.df_data.apply(self.util.fun_option_price, axis=1)
        # underlyingids = self.df_data[self.util.col_id_underlying].unique()
        # for underlying_id in underlyingids:
        #     c = self.df_data[self.util.col_id_underlying] == underlying_id
        #     df_tmp = self.df_data[c]
        #     mdt = df_tmp[self.util.col_maturitydt].values[0]
        #     """Check Null Maturity"""
        #     if pd.isnull(mdt):
        #         m1 = int(underlying_id[-2:])
        #         y1 = int(str(20) + underlying_id[-4:-2])
        #         dt1 = ql.Date(1, m1, y1)
        #         if self.option_code == 'sr':
        #             mdt = self.util.to_dt_date(self.calendar.advance(dt1, ql.Period(-5, ql.Days)))
        #         elif self.option_code == 'm':
        #             tmp = self.calendar.advance(dt1, ql.Period(-1, ql.Months))
        #             mdt = self.util.to_dt_date(self.calendar.advance(tmp, ql.Period(5, ql.Days)))
        #         self.df_data.loc[c, self.util.col_maturitydt] = mdt
        # for (idx, row) in self.df_data.iterrows():
        #     """Check Null Option Type"""
        #     option_type = row[self.util.col_option_type]
        #     id_instrument = row[self.util.col_id_instrument]
        #     if pd.isnull(option_type):
        #         if self.option_code in ['sr', 'm']:
        #             if id_instrument[-6] == 'c':
        #                 option_type = self.util.type_call
        #             elif id_instrument[-6] == 'p':
        #                 option_type = self.util.type_put
        #             else:
        #                 print(id_instrument, ',', id_instrument[-6])
        #                 continue
        #             self.df_data.loc[idx, self.util.col_option_type] = option_type
        #         else:
        #             continue
        #     """Check Null Strike"""
        #     strike = row[self.util.col_strike]
        #     if pd.isnull(strike):
        #         if self.option_code in ['sr', 'm']:
        #             strike = float(id_instrument[-4:])
        #             self.df_data.loc[idx, self.util.col_strike] = strike
        #         else:
        #             continue
        #     """Check Null Multuplier"""
        #     multiplier = row[self.util.col_multiplier]
        #     if pd.isnull(multiplier):
        #         if self.option_code in ['sr', 'm']:
        #             multiplier = 10
        #         else:
        #             multiplier = 10000
        #         self.df_data.loc[idx, self.util.col_multiplier] = multiplier


    def update_eligible_maturities(self):  # n: 要求合约剩余期限大于n（天）
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
        self.eligible_maturities = sorted(maturity_dates2)

    """ 主要针对50ETF期权分红 """

    def update_adjustment(self):
        self.update_multiplier_adjustment()
        self.update_applicable_strikes()

    def update_multiplier_adjustment(self):
        if self.option_code == '50etf':
            self.df_data[self.util.col_adj_strike] = \
                round(self.df_data[self.util.col_strike] * self.df_data[self.util.col_multiplier] / 10000, 2)
            self.df_data[self.util.col_adj_option_price] = \
                round(self.df_data[self.util.col_settlement] * self.df_data[self.util.col_multiplier] / 10000, 2)

        else:
            self.df_data[self.util.col_adj_strike] = self.df_data[self.util.col_strike]
            self.df_data[self.util.col_adj_option_price] = self.df_data[self.util.col_settlement]

    def update_applicable_strikes(self):
        if self.option_code == '50etf':
            self.df_data = self.util.get_applicable_strike_df(self.df_data)

    def add_dtdate_column(self):
        if self.util.col_date not in self.df_data.columns.tolist():
            for (idx, row) in self.df_data.iterrows():
                self.df_data.loc[idx, self.util.col_date] = row[self.util.col_datetime].date()


    def get_put(self, moneyness_rank, mdt, cd_long_short, cd_underlying_price='open'):
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档
        options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
        res_dict = options_by_moneyness[mdt][self.util.type_put]
        if res_dict == {}:
            print('bkt_option_set--get put failed,option dict is empty!')
            return pd.DataFrame()
        if moneyness_rank not in res_dict.keys():
            print('bkt_option_set--get put failed,given moneyness rank not exit!')
            return pd.DataFrame()
        option_put = res_dict[moneyness_rank]
        portfolio = Puts(self.eval_date, [option_put], cd_long_short)
        return portfolio

    def get_call(self, moneyness_rank, mdt, cd_long_short, cd_underlying_price='close', cd_moneyness_method=None):
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档
        options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price, cd_moneyness_method)
        res_dict = options_by_moneyness[mdt][self.util.type_call]
        if res_dict == {}:
            print('bkt_option_set--get_call failed,option dict is empty!')
            return pd.DataFrame()
        if moneyness_rank not in res_dict.keys():
            print('bkt_option_set--get_call failed,given moneyness rank not exit!')
            return pd.DataFrame()
        option_call = res_dict[moneyness_rank]
        portfolio = Calls(self.eval_date, [option_call], cd_long_short)
        return portfolio

    """moneyness =0 : 跨式策略，moneyness = -1/-2 : 宽跨式策略"""

    def get_straddle(self, moneyness_rank, mdt, delta_exposure,long_short, cd_underlying_price='close'):
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值行权价往实值方向移一档
        options_by_moneyness = self.get_moneyness_iv_by_mdt(mdt, cd_underlying_price)
        if moneyness_rank not in options_by_moneyness[self.util.type_call].keys() or \
                        moneyness_rank not in options_by_moneyness[self.util.type_put].keys():
            return
        else:
            option_call = options_by_moneyness[self.util.type_call][moneyness_rank]
            option_put = options_by_moneyness[self.util.type_put][moneyness_rank]
        straddle = Straddle(self.eval_date, option_call, option_put, delta_exposure, long_short)
        return straddle

    """Calendar Spread: Long far month and short near month;'option_type=None' means both call and put are included"""

    def get_calendar_spread_long(self, moneyness_rank, mdt1, mdt2, option_type, cd_underlying_price='close'):
        if mdt1 > mdt2:
            print('get_calendar_spread_call : mdt1 > mdt2')
            return
        options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
        option_mdt1 = options_by_moneyness[mdt1][option_type][moneyness_rank]  # short
        option_mdt2 = options_by_moneyness[mdt2][option_type][moneyness_rank]  # long
        cs = CalandarSpread(self.eval_date, option_mdt1, option_mdt2, option_type)
        return cs

    """Back Spread: Long small delta(atm), short large delta(otm)"""

    def get_backspread(self, option_type, mdt, moneyness1=0, moneyness2=-2, cd_underlying_price='close'):
        options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
        if moneyness2 not in options_by_moneyness[mdt][option_type].keys():
            moneyness2 += 1
            moneyness1 += 1
            print(self.eval_date, ' Move moneyness rank for lack of stikes')
        option_long = options_by_moneyness[mdt][option_type][moneyness2]
        option_short = options_by_moneyness[mdt][option_type][moneyness1]
        bs = BackSpread(self.eval_date, option_long, option_short, option_type)
        return bs

    def get_collar2(self, mdt, underlying, moneyness_call=-2, moneyness_put=-2, cd_underlying_price='close'):
        options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
        if moneyness_call not in options_by_moneyness[mdt][self.util.type_call].keys():
            write_call = None
        else:
            write_call = options_by_moneyness[mdt][self.util.type_call][moneyness_call]
        if moneyness_put not in options_by_moneyness[mdt][self.util.type_put].keys():
            buy_put = None
        else:
            buy_put = options_by_moneyness[mdt][self.util.type_put][moneyness_put]
        collar = Collar(self.eval_date, buy_put=buy_put, write_call=write_call, underlying=underlying)
        return collar

    def get_collar(self, mdt_call, mdt_put, underlying, moneyness_call=-2, moneyness_put=-2,
                   cd_underlying_price='close', flag_protect=False):
        options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
        while moneyness_call < 0:
            if moneyness_call not in options_by_moneyness[mdt_call][self.util.type_call].keys():
                moneyness_call += 1
            else:
                break
        while moneyness_put <= 0:
            if moneyness_put not in options_by_moneyness[mdt_put][self.util.type_put].keys():
                moneyness_put += 1
            else:
                break
        buy_put = options_by_moneyness[mdt_put][self.util.type_put][moneyness_put]
        write_call = options_by_moneyness[mdt_call][self.util.type_call][moneyness_call]
        "No 1st otm put/call (moneyness=-1) to by/write, stop collar strategy"
        if moneyness_call >= 0:
            write_call = None
            buy_put = None
        if moneyness_put >= 0:
            write_call = None
            # if not flag_protect:
            #     buy_put = None
        collar = Collar(self.eval_date, buy_put=buy_put, write_call=write_call, underlying=underlying)
        return collar

    def update_options_by_moneyness(self, cd_underlying_price='open', cd_moneyness_method=None):
        if cd_moneyness_method == None or cd_moneyness_method == self.util.method_1:
            res = self.update_options_by_moneyness_1(cd_underlying_price)
        else:
            res = self.update_options_by_moneyness_2(cd_underlying_price)
        return res

    """ Input optionset with the same maturity,get dictionary order by moneynesses as keys 
        * ATM defined as FIRST OTM  """

    def update_options_by_moneyness_1(self, cd_underlying_price):
        df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
        options_by_moneyness = {}
        for mdt in self.eligible_maturities:
            df_call = self.util.get_df_call_by_mdt(df, mdt)
            df_put = self.util.get_df_put_by_mdt(df, mdt)
            optionset_call = df_call[self.util.bktoption]
            optionset_put = df_put[self.util.bktoption]
            dict_call = {}
            dict_put = {}
            res_call = {}
            res_put = {}
            atm_call = 1000
            atm_put = -1000
            if cd_underlying_price == 'close':
                spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
            else:
                spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
            m_call = []
            m_put = []
            for option in optionset_call:
                k = option.strike()  # TODO: why not adj_strike?
                m = round(k - spot, 6)
                if m >= 0:
                    atm_call = min(atm_call, m)
                dict_call.update({m: option})
                m_call.append(m)
            for option in optionset_put:
                k = option.strike()
                m = round(k - spot, 6)
                if m <= 0:
                    atm_put = max(atm_put, m)
                dict_put.update({m: option})
                m_put.append(m)
            keys_call = sorted(dict_call)
            keys_put = sorted(dict_put)
            if atm_call == 1000: atm_call = max(m_call)
            if atm_put == -1000: atm_put = min(m_put)
            idx_call = keys_call.index(atm_call)
            for (i, key) in enumerate(keys_call):
                res_call.update({idx_call - i: dict_call[key]})
            idx_put = keys_put.index(atm_put)
            for (i, key) in enumerate(keys_put):
                res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
            res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
            options_by_moneyness.update({mdt: res_callput})
        return options_by_moneyness

    """ Input optionset with the same maturity,get dictionary order by moneynesses as keys 
        * ATM defined as THAT WITH STRIKE CLOSEST WITH UNDERLYING PRICE """

    def update_options_by_moneyness_2(self, cd_underlying_price):
        df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
        options_by_moneyness = {}
        for mdt in self.eligible_maturities:
            df_call = self.util.get_df_call_by_mdt(df, mdt)
            df_put = self.util.get_df_put_by_mdt(df, mdt)
            optionset_call = df_call[self.util.bktoption]
            optionset_put = df_put[self.util.bktoption]
            dict_call = {}
            dict_put = {}
            res_call = {}
            res_put = {}
            if cd_underlying_price == 'close':
                spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
            else:
                spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
            dict_m = {}
            for option in optionset_call:
                k = option.strike()
                m = round(k - spot, 6)
                dict_call.update({m: option})
                dict_m.update({abs(m): m})
            for option in optionset_put:
                k = option.strike()
                m = round(k - spot, 6)
                dict_put.update({m: option})
            atm = dict_m[min(dict_m.keys())]
            keys_call = sorted(dict_call)
            keys_put = sorted(dict_put)
            idx_call = keys_call.index(atm)
            for (i, key) in enumerate(keys_call):
                res_call.update({idx_call - i: dict_call[key]})
            idx_put = keys_put.index(atm)
            for (i, key) in enumerate(keys_put):
                res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
            res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
            options_by_moneyness.update({mdt: res_callput})
        return options_by_moneyness

    def get_moneyness_iv_by_mdt(self, mdt, cd_underlying_price='open', cd_moneyness_method=None):
        if cd_moneyness_method == None or cd_moneyness_method == self.util.method_1:
            res = self.get_moneyness_iv_by_mdt_1(mdt, cd_underlying_price)
        else:
            res = self.get_moneyness_iv_by_mdt_2(mdt, cd_underlying_price)
        return res

    """ Given target maturity date, get call and put iv sorted by moneyness rank 
        * ATM defined as FIRST OTM  """

    def get_moneyness_iv_by_mdt_1(self, mdt, cd_underlying_price):
        df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
        df_call = self.util.get_df_call_by_mdt(df, mdt)
        df_put = self.util.get_df_put_by_mdt(df, mdt)
        optionset_call = df_call[self.util.bktoption]
        optionset_put = df_put[self.util.bktoption]
        dict_call = {}
        dict_put = {}
        res_call = {}
        res_put = {}
        atm_call = 1000
        atm_put = -1000
        if cd_underlying_price == 'close':
            spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
        else:
            spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
        m_call = []
        m_put = []
        for option in optionset_call:
            k = option.strike()
            m = round(k - spot, 6)
            if m >= 0:
                atm_call = min(atm_call, m)
            dict_call.update({m: option})
            m_call.append(m)
        for option in optionset_put:
            k = option.strike()
            m = round(k - spot, 6)
            if m <= 0:
                atm_put = max(atm_put, m)
            dict_put.update({m: option})
            m_put.append(m)
        keys_call = sorted(dict_call)
        keys_put = sorted(dict_put)
        if atm_call == 1000: atm_call = max(m_call)
        if atm_put == -1000: atm_put = min(m_put)
        idx_call = keys_call.index(atm_call)
        for (i, key) in enumerate(keys_call):
            res_call.update({idx_call - i: dict_call[key]})
        idx_put = keys_put.index(atm_put)
        for (i, key) in enumerate(keys_put):
            res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
        res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
        return res_callput

    """ Given target maturity date, get call and put iv sorted by moneyness rank 
        * ATM defined as THAT WITH STRIKE CLOSEST WITH UNDERLYING PRICE """

    def get_moneyness_iv_by_mdt_2(self, mdt, cd_underlying_price):
        df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
        df_call = self.util.get_df_call_by_mdt(df, mdt)
        df_put = self.util.get_df_put_by_mdt(df, mdt)
        optionset_call = df_call[self.util.bktoption]
        optionset_put = df_put[self.util.bktoption]
        dict_call = {}
        dict_put = {}
        res_call = {}
        res_put = {}
        if cd_underlying_price == 'close':
            spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
        else:
            spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
        dict_m = {}
        for option in optionset_call:
            k = option.strike()
            m = round(k - spot, 6)
            dict_call.update({m: option})
            dict_m.update({abs(m): m})
        for option in optionset_put:
            k = option.strike()
            m = round(k - spot, 6)
            dict_put.update({m: option})
        atm = dict_m[min(dict_m.keys())]
        keys_call = sorted(dict_call)
        keys_put = sorted(dict_put)
        idx_call = keys_call.index(atm)
        for (i, key) in enumerate(keys_call):
            res_call.update({idx_call - i: dict_call[key]})
        idx_put = keys_put.index(atm)
        for (i, key) in enumerate(keys_put):
            res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
        res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
        return res_callput


    def get_mdt_keyvols(self, option_type):
        keyvols_mdts = {}
        df_data = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
        for mdt in self.eligible_maturities:
            df_mdt = self.util.get_df_by_mdt_type(df_data, mdt, option_type)
            df = self.calculate_implied_vol(df_mdt).sort_values(by=[self.util.col_applicable_strike])
            spot = df_mdt[self.util.bktoption].values[0].underlying_close()
            strikes = []
            vols = []
            for (idx, row) in df.iterrows():
                strike = row[self.util.col_applicable_strike]
                iv = row[self.util.col_implied_vol]
                # if iv > 0:
                strikes.append(float(strike))
                vols.append(iv)
                # else:
                #     continue
            volset = [vols]
            m_list = [[mdt]]
            vol_matrix = ql.Matrix(len(strikes), len(m_list))
            for i in range(vol_matrix.rows()):
                for j in range(vol_matrix.columns()):
                    vol_matrix[i][j] = volset[j][i]
            ql_evalDate = self.util.to_ql_date(self.eval_date)
            ql_maturities = [ql.Date(mdt.day, mdt.month, mdt.year)]
            try:
                black_var_surface = ql.BlackVarianceSurface(
                    ql_evalDate, self.calendar, ql_maturities, strikes, vol_matrix, self.daycounter)
                keyvols_mdt = {}
                try:
                    if min(strikes) > spot:
                        s = min(strikes)
                    elif max(strikes) < spot:
                        s = max(strikes)
                    else:
                        s = spot
                    vol_100 = black_var_surface.blackVol(ql_maturities[0], s)
                    keyvols_mdt.update({100: vol_100})
                except Exception as e:
                    print(e)
                    pass
                try:
                    vol_110 = black_var_surface.blackVol(ql_maturities[0], spot * 1.1)
                    keyvols_mdt.update({110: vol_110})
                except Exception as e:
                    pass
                try:
                    vol_105 = black_var_surface.blackVol(ql_maturities[0], spot * 1.05)
                    keyvols_mdt.update({105: vol_105})
                except Exception as e:
                    pass
                try:
                    vol_90 = black_var_surface.blackVol(ql_maturities[0], spot * 0.9)
                    keyvols_mdt.update({90: vol_90})
                except Exception as e:
                    pass
                try:
                    vol_95 = black_var_surface.blackVol(ql_maturities[0], spot * 0.95)
                    keyvols_mdt.update({95: vol_95})
                except Exception as e:
                    pass
                keyvols_mdts.update({mdt: keyvols_mdt})
            except Exception as e:
                print(e)
        return keyvols_mdts


    """ Get 1M atm vol by liner interpolation """

    def get_interpolated_atm_1M(self, option_type):
        keyvols_mdts = self.get_mdt_keyvols(option_type)
        ql_evalDate = self.util.to_ql_date(self.eval_date)
        mdt_1m = self.util.to_dt_date(self.calendar.advance(ql_evalDate, ql.Period(1, ql.Months)))
        d0 = datetime.date(1999, 1, 1)
        mdt_1m_num = (mdt_1m - d0).days
        maturities_num = []
        atm_vols = []
        for m in self.eligible_maturities:
            maturities_num.append((m - d0).days)
            atm_vols.append(keyvols_mdts[m][100])  # atm vol : skrike is 100% spot
        try:
            x = np.interp(mdt_1m_num, maturities_num, atm_vols)
        except Exception as e:
            print(e)
            return
        return x


    """ Get Call/Put volatility surface separately"""

    def get_volsurface_squre(self, df):
        ql_maturities = []

        df = self.calculate_implied_vol(df)
        df_mdt_list = []
        iv_name_list = []
        maturity_list = []
        for idx, mdt in enumerate(self.eligible_maturities):
            iv_rename = 'implied_vol_' + str(idx)
            df_mkt = df[(df[self.util.col_maturitydt] == mdt)] \
                .rename(columns={self.util.col_implied_vol: iv_rename}) \
                .set_index(self.util.col_applicable_strike).sort_index()
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
        df_call = self.util.get_duplicate_strikes_dropped(self.util.get_df_by_type(self.df_daily_state, self.util.type_call))
        df_put = self.util.get_duplicate_strikes_dropped(self.util.get_df_by_type(self.df_daily_state, self.util.type_put))
        df_call = self.calculate_implied_vol(df_call)
        df_put = self.calculate_implied_vol(df_put)
        df_call['maturity_call'] = df_call[self.util.col_maturitydt]
        df_call['adj_strike_call'] = df_call[self.util.col_adj_strike]
        df_call = df_call.set_index([self.util.col_maturitydt, self.util.col_adj_strike]) \
            .rename(columns={self.util.col_implied_vol: 'iv_call'})
        df_put = df_put.set_index([self.util.col_maturitydt, self.util.col_adj_strike]) \
            .rename(columns={self.util.col_implied_vol: 'iv_put'})
        df = df_call[['adj_strike_call', 'maturity_call', 'iv_call']] \
            .join(df_put[['iv_put']])
        df['mid_vol'] = (df['iv_call'] + df['iv_put']) / 2
        maturities = sorted(df['maturity_call'].unique())
        for idx, mdt in enumerate(maturities):
            if mdt <= self.eval_date: continue
            iv_rename = 'implied_vol_' + str(idx)
            df_mkt = df[(df['maturity_call'] == mdt)] \
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


    def calculate_implied_vol(self, df):
        for (idx, row) in df.iterrows():
            option = row[self.util.bktoption]
            iv = option.get_implied_vol()
            df.loc[idx, self.util.col_implied_vol] = iv
        return df


    def collect_option_metrics(self, hp=30):
        res = []
        df = pd.DataFrame(columns=[self.util.col_date, self.util.col_carry, self.util.bktoption])
        bktoption_list = self.bktoptionset
        if len(bktoption_list) == 0: return df
        if self.option_code == '50etf':
            df_data = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
        else:
            df_data = self.df_daily_state
        df_data_call = self.util.get_df_by_type(df_data, self.util.type_call)
        df_data_put = self.util.get_df_by_type(df_data, self.util.type_put)

        bvs_put = self.get_volsurface_squre(df_data_put)
        for idx, option in enumerate(bktoption_list):
            if option.option_price() > 0.0:
                iv = option.get_implied_vol()
                if option.option_type == self.util.type_call:
                    carry = option.get_carry(bvs_call, hp)
                else:
                    carry = option.get_carry(bvs_put, hp)
                theta = option.get_theta()
                vega = option.get_vega()

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
                carry = theta = vega = gamma = delta = rho = -999.0
                if iv == None or np.isnan(iv): iv = -999.0
            if self.flag_calculate_iv:
                datasource = 'calculated'
            else:
                if self.option_code == 'm':
                    datasource = 'dce'
                else:
                    datasource = 'czce'
            db_row = {
                self.util.col_date: self.eval_date,
                self.util.col_id_instrument: option.id_instrument(),
                'datasource': datasource,
                'name_code': self.option_code,
                'id_underlying': option.id_underlying(),
                'amt_strike': float(option.strike()),
                self.util.col_code_instrument: option.code_instrument(),
                self.util.col_option_type: option.option_type(),
                self.util.col_maturitydt: option.maturitydt(),
                self.util.col_implied_vol: float(iv),
                self.util.col_adj_strike: float(option.adj_strike()),
                self.util.col_option_price: float(option.option_price()),
                'amt_delta': float(delta),
                self.util.col_vega: float(vega),
                self.util.col_theta: float(theta),
                'amt_rho': float(rho),
                'amt_gamma': float(gamma),
                'amt_carry_1M': float(carry),
                'timestamp': datetime.datetime.today()
            }
            res.append(db_row)
        return res
