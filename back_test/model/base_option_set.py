import datetime
from collections import deque
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
import math
from back_test.model.abstract_base_product_set import AbstractBaseProductSet
from back_test.model.base_option import BaseOption
from back_test.model.constant import FrequentType, Util, OptionFilter, OptionType, OptionUtil, Option50ETF, OptionExerciseType, LongShort
from back_test.model.trade import Order
from PricingLibrary.EngineQuantlib import QlBinomial,QlBlackFormula

class BaseOptionSet(AbstractBaseProductSet):
    """
    Feature:

    To Collect BktOption Set
    To Calculate Vol Surface and Metrics
    To Manage Back Test State of all BktOption Objects
    """

    def __init__(self, df_data: pd.DataFrame,
                 df_daily_data: pd.DataFrame = None,
                 df_underlying: pd.DataFrame = None,
                 frequency: FrequentType = FrequentType.DAILY,
                 flag_calculate_iv: bool = True,
                 rf: float = 0.03):
        super().__init__()
        self._name_code: str = df_data.loc[0, Util.ID_INSTRUMENT].split('_')[0]
        self.df_data: pd.DataFrame = df_data
        if frequency in Util.LOW_FREQUENT:
            self.df_daily_data = df_data
        else:
            self.df_daily_data = df_daily_data
        self.df_underlying = df_underlying  # df_underlying should have the same frequency with df_data.
        self.frequency: FrequentType = frequency
        self.flag_calculate_iv: bool = flag_calculate_iv
        self.option_dict: Dict[datetime.date, List(BaseOption)] = {}
        self.rf: float = rf
        self.size: int = 0
        self.eval_date: datetime.date = None
        self.eval_datetime: datetime.datetime = None
        self.current_index = -1
        self.eligible_options = deque()
        self.update_contract_month_maturity_table()  # _generate_required_columns_if_missing的时候就要用到
        self.eligible_maturities: List(datetime.date) = None # To be fulled in NEXT() method
        self.OptionUtilClass = OptionUtil.get_option_util_class(self._name_code)
        if self._name_code in ['m', 'sr']:
            self.exercise_type = OptionExerciseType.AMERICAN
        else:
            self.exercise_type = OptionExerciseType.EUROPEAN

    def init(self) -> None:
        self._generate_required_columns_if_missing()  # 补充行权价等关键信息（high frequency data就可能没有）
        print('start preprosess ', datetime.datetime.now())
        self.pre_process()
        print('end preprosess ', datetime.datetime.now())
        self.next()

    def _generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.OPTION_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if column not in columns:
                self.df_data[column] = None
            # if column not in columns:
            #     # STRIKE
            #     if column == Util.AMT_STRIKE:
            #         self.df_data[Util.AMT_STRIKE] = self.df_data[Util.ID_INSTRUMENT].apply(lambda x: float(x.split('_')[3]))
            #     # NAME_CONTRACT_MONTH
            #     elif column == Util.NAME_CONTRACT_MONTH:
            #         self.df_data[Util.NAME_CONTRACT_MONTH] = self.df_data[Util.ID_INSTRUMENT].apply(
            #             lambda x: x.split('_')[1])
            #     # OPTION_TYPE
            #     elif column == Util.CD_OPTION_TYPE:
            #         self.df_data[Util.CD_OPTION_TYPE] = self.df_data[Util.ID_INSTRUMENT].apply(
            #             OptionFilter.fun_option_type_split)
            #     else:
            #         self.df_data[column] = None
        # DT_MATURITY -> datetime.date : 通过contract month查找
        if self.df_data.loc[0, Util.DT_MATURITY] is None or pd.isnull(self.df_data.loc[0, Util.DT_MATURITY]):
            # self.df_data[Util.DT_MATURITY] = self.df_data.apply(OptionFilter.fun_option_maturity, axis=1)
            self.df_data[Util.DT_MATURITY] = self.df_data.apply(
                lambda x: OptionFilter.dict_maturities[x[Util.ID_UNDERLYING]] if pd.isnull(x[Util.DT_MATURITY]) else x[Util.DT_MATURITY], axis=1)
        # STRIKE -> float
        if self.df_data.loc[0, Util.AMT_STRIKE] is None or pd.isnull(self.df_data.loc[0, Util.AMT_STRIKE]):
            self.df_data[Util.AMT_STRIKE] = self.df_data.apply(
                lambda x: float(x[Util.ID_INSTRUMENT].split('_')[3]) if pd.isnull(x[Util.AMT_STRIKE]) else x[Util.AMT_STRIKE], axis=1)
        # NAME_CONTRACT_MONTH -> String
        if self.df_data.loc[0, Util.NAME_CONTRACT_MONTH] is None or pd.isnull(self.df_data.loc[0, Util.NAME_CONTRACT_MONTH]):
            self.df_data[Util.NAME_CONTRACT_MONTH] = self.df_data.apply(
                lambda x: float(x[Util.ID_INSTRUMENT].split('_')[1]) if pd.isnull(x[Util.NAME_CONTRACT_MONTH]) else x[
                    Util.NAME_CONTRACT_MONTH], axis=1)
        # OPTION_TYPE -> String
        if self.df_data.loc[0, Util.CD_OPTION_TYPE] is None or pd.isnull(self.df_data.loc[0, Util.CD_OPTION_TYPE]):
            self.df_data[Util.CD_OPTION_TYPE] = self.df_data.apply(OptionFilter.fun_option_type_split, axis=1)
        # MULTIPLIER -> int: 50etf期权的multiplier跟id_instrument有关，需补充该列实际值。（商品期权multiplier是固定的）
        if self._name_code == Util.STR_50ETF:
            if self.df_data.loc[0, Util.NBR_MULTIPLIER] is None or np.isnan(self.df_data.loc[0, Util.NBR_MULTIPLIER]):
                self.df_data = self.df_data.drop(Util.NBR_MULTIPLIER, axis=1).join(
                    self.get_id_multiplier_table().set_index(Util.ID_INSTRUMENT),
                    how='left', on=Util.ID_INSTRUMENT
                )
        # ID_UNDERLYING : 通过name code 与 contract month补充
        if self.df_data.loc[0, Util.ID_UNDERLYING] is None or pd.isnull(self.df_data.loc[0, Util.ID_UNDERLYING]):
            if self._name_code == Util.STR_50ETF:
                self.df_data.loc[:, Util.ID_UNDERLYING] = Util.STR_INDEX_50ETF
            else:
                self.df_data.loc[:, Util.ID_UNDERLYING] = self._name_code + self.df_data.loc[:, Util.NAME_CONTRACT_MONTH]

    def get_id_multiplier_table(self):
        df_id_multiplier = self.df_daily_data.drop_duplicates(
            Util.ID_INSTRUMENT)[[Util.ID_INSTRUMENT, Util.NBR_MULTIPLIER]]
        return df_id_multiplier

    def pre_process(self) -> None:

        if self.frequency in Util.LOW_FREQUENT:
            self.date_list: List[datetime.date] = sorted(self.df_data[Util.DT_DATE].unique())
            self.nbr_index = len(self.date_list)
        else:
            mask = self.df_data.apply(Util.filter_invalid_data, axis=1)
            self.df_data = self.df_data[mask].reset_index(drop=True)
            self.datetime_list: List[datetime.datetime] = sorted(self.df_data[Util.DT_DATETIME].unique())
            self.nbr_index = len(self.datetime_list)
            if self.df_daily_data is None:
                # TODO: Rise error if no daily data in high frequency senario.
                return
        # TODO: """ new added """
        # self.df_data[Util.AMT_STRIKE_BEFORE_ADJ] = self.df_data.apply(Option50ETF.fun_strike_before_adj, axis=1)
        self.df_data[Util.AMT_APPLICABLE_STRIKE] = self.df_data.apply(Option50ETF.fun_applicable_strike, axis=1)
        groups = self.df_data.groupby([Util.ID_INSTRUMENT])
        if self.df_daily_data is not None:
            groups_daily = self.df_daily_data.groupby([Util.ID_INSTRUMENT])
        else:
            groups_daily = None
        for key in groups.groups.keys():
            # manage minute data and daily data.
            df_option = groups.get_group(key).reset_index(drop=True)
            # print(key, ' , ', len(df_option), ' , ', datetime.datetime.now())
            if self.df_daily_data is not None:
                df_option_daily = groups_daily.get_group(key).reset_index(drop=True)
            else:
                df_option_daily = None
            option = BaseOption(df_option, df_option_daily, self.frequency, self.flag_calculate_iv,
                                self.rf)
            option.init()
            l = self.option_dict.get(option.eval_date)
            if l is None:
                l = []
                self.option_dict.update({option.eval_date: l})
            l.append(option)
            self.size += 1

    def next(self) -> None:
        start = datetime.datetime.now()
        # Update index and time,
        self.current_index += 1
        if self.frequency in Util.LOW_FREQUENT:
            self.eval_date = self.date_list[self.current_index]
        else:
            self.eval_datetime = pd.to_datetime(self.datetime_list[self.current_index])
            if self.eval_date != self.eval_datetime.date():
                self.eval_date = self.eval_datetime.date()
        # Update existing deque
        size = len(self.eligible_options)
        eligible_maturities = []
        for i in range(size):
            option = self.eligible_options.popleft()
            if not option.has_next():
                continue
            option.next()
            if option.is_valid_option():
                self.add_option(option)
                if option.maturitydt() not in eligible_maturities:
                    eligible_maturities.append(option.maturitydt())
        for option in self.option_dict.pop(self.eval_date, []):
            if option.is_valid_option():
                self.add_option(option)
                if option.maturitydt() not in eligible_maturities:
                    eligible_maturities.append(option.maturitydt())
        self.eligible_maturities = sorted(eligible_maturities)
        # Check option data quality.
        if self.frequency not in Util.LOW_FREQUENT:
            for option in self.eligible_options:
                if self.eval_datetime != option.eval_datetime:
                    print("Option datetime does not match, id : {0}, dt_optionset:{1}, dt_option:{2}".format(
                        option.id_instrument(), self.eval_datetime, option.eval_datetime))
        end = datetime.datetime.now()
        # print("OptionSet.NEXT iter {0}, option_set length:{1}, time cost{2}".format(self.eval_date, len(self.eligible_options),
        #                                                              (end - start).total_seconds()))
        return None

    def __repr__(self) -> str:
        return 'BaseOptionSet(evalDate:{0}, totalSize: {1})' \
            .format(self.eval_date, self.size)

    def add_option(self, option: BaseOption) -> None:
        self.eligible_options.append(option)

    def has_next(self) -> bool:
        return self.current_index < self.nbr_index - 1

    def get_current_state(self) -> pd.DataFrame:
        df_current_state = self.df_data[self.df_data[Util.DT_DATE] == self.eval_date].reset_index(drop=True)
        return df_current_state

    # 期权到期日与期权和月份的对应关系表
    def update_contract_month_maturity_table(self):
        self.df_maturity_and_contract_months = self.df_daily_data.drop_duplicates(Util.NAME_CONTRACT_MONTH) \
            .sort_values(by=Util.NAME_CONTRACT_MONTH).reset_index(drop=True) \
            [[Util.NAME_CONTRACT_MONTH, Util.DT_MATURITY]]


    def get_maturities_list(self) -> List[datetime.date]:
        list_maturities = []
        for option in self.eligible_options:
            maturitydt = option.maturitydt()
            if maturitydt not in list_maturities: list_maturities.append(maturitydt)
        list_maturities = sorted(list_maturities)
        return list_maturities

    # get Dictionary <contract month, List[option]>
    def get_dict_options_by_contract_months(self):
        dic = {}
        for option in self.eligible_options:
            if option.contract_month() in dic.keys():
                dic[option.contract_month()].append(option)
            else:
                dic.update({option.contract_month(): [option]})
        return dic

    # get Dictionary <maturitydt, List[option]>
    def get_dict_options_by_maturities(self):
        dic = {}
        for option in self.eligible_options:
            if option.maturitydt() in dic.keys():
                dic[option.maturitydt()].append(option)
            else:
                dic.update({option.maturitydt(): [option]})
        return dic

    # 根据到期日或合约月份查找标的价格，需重新计算（暂未用到）
    def get_underlying_close(self, contract_month=None, maturitydt=None):
        # 对于商品期权，underlying要从对应的月份合约中找。
        if self._name_code == Util.STR_50ETF:
            spot = self.eligible_options[0].underlying_close()
        else:
            if contract_month is not None:
                option_list = self.get_dict_options_by_contract_months()[contract_month]
                spot = option_list[0].underlying_close()
            elif maturitydt is not None:
                option_list = self.get_dict_options_by_maturities()[maturitydt]
                spot = option_list[0].underlying_close()
            else:
                print('No contract month or maturity specified for commodity option.')
                maturitydt = sorted(self.get_dict_options_by_maturities().keys())[0]
                option_list = self.get_dict_options_by_maturities()[maturitydt]
                spot = option_list[0].underlying_close()
        return spot


    def get_T_quotes(self, nbr_maturity:int=0):
        dt_maturity = self.get_maturities_list()[nbr_maturity]
        df_current = self.get_current_state()
        df_mdt = df_current[df_current[Util.DT_MATURITY]==dt_maturity].reset_index(drop=True)
        df_call = df_mdt[df_mdt[Util.CD_OPTION_TYPE] == Util.STR_CALL].rename(
            columns={Util.AMT_CLOSE: Util.AMT_CALL_QUOTE, Util.AMT_TRADING_VOLUME:Util.AMT_CALL_TRADING_VOLUME})
        df_put = df_mdt[df_mdt[Util.CD_OPTION_TYPE] == Util.STR_PUT].rename(
            columns={Util.AMT_CLOSE: Util.AMT_PUT_QUOTE, Util.AMT_TRADING_VOLUME:Util.AMT_PUT_TRADING_VOLUME})
        df_call = df_call.drop_duplicates(Util.AMT_APPLICABLE_STRIKE).reset_index(drop=True)
        df_put = df_put.drop_duplicates(Util.AMT_APPLICABLE_STRIKE).reset_index(drop=True)
        df = pd.merge(df_call[[Util.DT_DATE, Util.AMT_CALL_QUOTE, Util.AMT_APPLICABLE_STRIKE, Util.AMT_STRIKE,
                               Util.DT_MATURITY, Util.AMT_UNDERLYING_CLOSE, Util.AMT_CALL_TRADING_VOLUME]],
                      df_put[[Util.AMT_PUT_QUOTE, Util.AMT_APPLICABLE_STRIKE,Util.AMT_PUT_TRADING_VOLUME]],
                      how='inner', on=Util.AMT_APPLICABLE_STRIKE)
        df[Util.AMT_TRADING_VOLUME] = df[Util.AMT_CALL_TRADING_VOLUME] + df[Util.AMT_PUT_TRADING_VOLUME]
        ttm = ((dt_maturity - self.eval_date).total_seconds() / 60.0) / (365.0 * 1440)
        df['amt_ttm'] = ttm
        return df

    # def get_implied_rf_vwpcr(self,nbr_maturity):
    #     t_qupte = self.get_T_quotes(nbr_maturity)
    #     t_qupte[Util.AMT_HTB_RATE] = t_qupte.apply(self.fun_implied_rf, axis=1)
    #     implied_rf = (t_qupte.loc[:, Util.AMT_HTB_RATE] * t_qupte.loc[:, Util.AMT_TRADING_VOLUME]).sum() \
    #            / t_qupte.loc[:,Util.AMT_TRADING_VOLUME].sum()
    #     return implied_rf
    #
    # def get_implied_rf_mink_pcr(self,nbr_maturity):
    #     t_qupte = self.get_T_quotes(nbr_maturity)
    #     min_k_series = t_qupte.loc[t_qupte[Util.AMT_APPLICABLE_STRIKE].idxmin()]
    #     implied_rf = self.fun_implied_rf(min_k_series)
    #     return implied_rf
    #
    # def fun_implied_rf(self,df_series):
    #     rf = math.log(df_series[Util.AMT_APPLICABLE_STRIKE] /
    #                   (df_series[Util.AMT_UNDERLYING_CLOSE] + df_series[Util.AMT_PUT_QUOTE]
    #                    - df_series[Util.AMT_CALL_QUOTE]), math.e) / df_series[Util.AMT_TTM]
    #     return rf

    def get_iv_by_otm_iv_curve(self, nbr_maturiy,strike):
        df = self.get_otm_implied_vol_curve(nbr_maturiy)
        iv = df[df[Util.AMT_APPLICABLE_STRIKE]==strike][Util.PCT_IV_OTM_BY_HTBR].values[0]
        return iv

    def get_otm_implied_vol_curve(self, nbr_maturity):
        t_qupte = self.get_T_quotes(nbr_maturity)
        t_qupte.loc[:, 'diff'] = abs(
            t_qupte.loc[:, Util.AMT_APPLICABLE_STRIKE] - t_qupte.loc[:, Util.AMT_UNDERLYING_CLOSE])
        atm_series = t_qupte.loc[t_qupte['diff'].idxmin()]
        htb_r = self.fun_htb_rate(atm_series, self.rf)
        t_qupte[Util.PCT_IV_CALL_BY_HTBR] = t_qupte.apply(lambda x:self.fun_htb_rate_adjusted_iv(x,OptionType.CALL,htb_r),axis=1)
        t_qupte[Util.PCT_IV_PUT_BY_HTBR] = t_qupte.apply(lambda x:self.fun_htb_rate_adjusted_iv(x,OptionType.PUT,htb_r),axis=1)
        t_qupte[Util.PCT_IV_OTM_BY_HTBR] = t_qupte.apply(self.fun_otm_iv,axis=1)
        return t_qupte[[Util.AMT_APPLICABLE_STRIKE,Util.AMT_UNDERLYING_CLOSE,Util.DT_MATURITY,Util.PCT_IV_OTM_BY_HTBR]]

    def get_call_implied_vol_curve(self, nbr_maturity):
        t_qupte = self.get_T_quotes(nbr_maturity)
        t_qupte[Util.PCT_IMPLIED_VOL] = t_qupte.apply(lambda x:self.fun_iv(x,OptionType.CALL),axis=1)
        return t_qupte[[Util.AMT_APPLICABLE_STRIKE,Util.AMT_UNDERLYING_CLOSE,Util.DT_MATURITY,Util.PCT_IMPLIED_VOL]]

    def get_put_implied_vol_curve(self, nbr_maturity):
        t_qupte = self.get_T_quotes(nbr_maturity)
        t_qupte[Util.PCT_IMPLIED_VOL] = t_qupte.apply(lambda x:self.fun_iv(x,OptionType.PUT),axis=1)
        return t_qupte[[Util.AMT_APPLICABLE_STRIKE,Util.AMT_UNDERLYING_CLOSE,Util.DT_MATURITY,Util.PCT_IMPLIED_VOL]]

    def fun_otm_iv(self,df_series):
        K = df_series[Util.AMT_APPLICABLE_STRIKE]
        S = df_series[Util.AMT_UNDERLYING_CLOSE]
        if K <= S:
            return df_series[Util.PCT_IV_PUT_BY_HTBR]
        else:
            return df_series[Util.PCT_IV_CALL_BY_HTBR]

    def fun_htb_rate_adjusted_iv(self,df_series: pd.DataFrame, option_type: OptionType, htb_r: float):
        ttm = df_series[Util.AMT_TTM]
        K = df_series[Util.AMT_APPLICABLE_STRIKE]
        S = df_series[Util.AMT_UNDERLYING_CLOSE] * math.exp(-htb_r * ttm)
        dt_eval = df_series[Util.DT_DATE]
        dt_maturity = df_series[Util.DT_MATURITY]
        if option_type == OptionType.CALL:
            C = df_series[Util.AMT_CALL_QUOTE]
            if self.exercise_type == OptionExerciseType.EUROPEAN:
                pricing_engine = QlBlackFormula(dt_eval, dt_maturity, OptionType.CALL, S, K, self.rf)
                # black_call = BlackFormula(dt_eval,dt_maturity,c.OptionType.CALL,S,K,C,rf=rf)
            else:
                pricing_engine = QlBinomial(dt_eval,dt_maturity,OptionType.CALL,OptionExerciseType.AMERICAN,S,K,rf=self.rf)
            iv = pricing_engine.estimate_vol(C)
        else:
            P = df_series[Util.AMT_PUT_QUOTE]
            if self.exercise_type == OptionExerciseType.EUROPEAN:
                pricing_engine = QlBlackFormula(dt_eval, dt_maturity, OptionType.PUT, S, K, self.rf)
                # black_call = BlackFormula(dt_eval,dt_maturity,c.OptionType.CALL,S,K,C,rf=rf)
            else:
                pricing_engine = QlBinomial(dt_eval,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,S,K,rf=self.rf)
            iv = pricing_engine.estimate_vol(P)
        return iv

    def get_htb_rate(self, nbr_maturity):
        t_qupte = self.get_T_quotes(nbr_maturity)
        t_qupte.loc[:, 'diff'] = abs(
            t_qupte.loc[:, Util.AMT_APPLICABLE_STRIKE] - t_qupte.loc[:, Util.AMT_UNDERLYING_CLOSE])
        atm_series = t_qupte.loc[t_qupte['diff'].idxmin()]
        htb_r = self.fun_htb_rate(atm_series, self.rf)
        return htb_r

    def fun_htb_rate(self,df_series, rf):
        r = -math.log((df_series[Util.AMT_CALL_QUOTE] - df_series[Util.AMT_PUT_QUOTE]
                       + df_series[Util.AMT_APPLICABLE_STRIKE] * math.exp(-rf * df_series[Util.AMT_TTM]))
                      / df_series[Util.AMT_UNDERLYING_CLOSE]) / df_series[Util.AMT_TTM]
        return r

    def fun_iv(self, df_series: pd.DataFrame, option_type: OptionType):
        K = df_series[Util.AMT_APPLICABLE_STRIKE]
        S = df_series[Util.AMT_UNDERLYING_CLOSE]
        dt_eval = df_series[Util.DT_DATE]
        dt_maturity = df_series[Util.DT_MATURITY]
        if option_type == OptionType.CALL:
            black_call = QlBlackFormula(dt_eval, dt_maturity, OptionType.CALL, S, K, self.rf)
            C = df_series[Util.AMT_CALL_QUOTE]
            iv = black_call.estimate_vol(C)
        else:
            black_put = QlBlackFormula(dt_eval, dt_maturity, OptionType.PUT, S, K, self.rf)
            P = df_series[Util.AMT_PUT_QUOTE]
            iv = black_put.estimate_vol(P)
        return iv

    def get_option_moneyness(self,base_option:BaseOption):
        maturity = base_option.maturitydt()
        mdt_calls, mdt_puts = self.get_orgnized_option_dict_for_moneyness_ranking()
        if base_option.option_type() == OptionType.CALL:
            mdt_options_dict = mdt_calls.get(maturity)
        else:
            mdt_options_dict = mdt_puts.get(maturity)
        spot = list(mdt_options_dict.values())[0][0].underlying_close()
        moneyness = self.OptionUtilClass.get_moneyness_of_a_strike_by_nearest_strike(spot, base_option.strike(),
                                                                                 list(mdt_options_dict.keys()),
                                                                                 base_option.option_type())
        return moneyness

    # 行权价最低的put
    def get_deepest_otm_put_list(self,maturity: datetime.date):
        mdt_calls, mdt_puts = self.get_orgnized_option_dict_for_moneyness_ranking()
        mdt_options_dict = mdt_puts.get(maturity)
        min_k = min(mdt_options_dict.keys())
        put_list = mdt_options_dict[min_k]
        return put_list

    """
    get_orgnized_option_dict_for_moneyness_ranking : 
    Dictionary <maturity-<nearest strike - List[option]>> to retrieve call and put List[option] by maturity date.
    call_mdt_option_dict:
    {
        '2017-05-17':{
            "2.8": [option1,option2],
            "2.85": [option1,option2],
        },
        '2017-06-17':{
            "2.8": [option1,option2],
            "2.85": [option1,option2],
        },
    }
    """

    def get_orgnized_option_dict_for_moneyness_ranking(self) -> \
            Tuple[Dict[datetime.date, Dict[float, List[BaseOption]]], Dict[datetime.date, Dict[float, List[BaseOption]]]]:
        call_ret = {}
        put_ret = {}
        for option in self.eligible_options:
            if option.option_type() == OptionType.CALL:
                ret = call_ret
            else:
                ret = put_ret
            d = ret.get(option.maturitydt())
            if d is None:
                d = {}
                ret.update({option.maturitydt(): d})
            l = d.get(option.nearest_strike())
            if l is None:
                l = []
                d.update({option.nearest_strike(): l})
            l.append(option)
        # 返回的option放在list里，是因为可能有相邻行权价的期权同时处于一个nearest strike
        return call_ret, put_ret


    """ Mthd1: Determine atm option as the NEAREST strike from spot. 
        Get option maturity dictionary from all maturities by given moneyness rank. """

    def get_options_dict_by_mdt_moneyness_mthd1(
            self, moneyness_rank: int) -> List[Dict[datetime.date, List[BaseOption]]]:
        mdt_calls, mdt_puts = self.get_orgnized_option_dict_for_moneyness_ranking()
        call_mdt_dict = {}
        put_mdt_dict = {}
        for mdt in mdt_calls.keys():
            mdt_options_dict = mdt_calls.get(mdt)
            spot = list(mdt_options_dict.values())[0][0].underlying_close()
            idx = self.OptionUtilClass.get_strike_by_monenyes_rank_nearest_strike(spot, moneyness_rank,
                                                                            list(mdt_options_dict.keys()), OptionType.CALL)
            call_mdt_dict.update({mdt: mdt_options_dict.get(idx)})
        for mdt in mdt_puts.keys():
            mdt_options_dict = mdt_puts.get(mdt)
            spot = list(mdt_options_dict.values())[0][0].underlying_close()
            idx = self.OptionUtilClass.get_strike_by_monenyes_rank_nearest_strike(spot, moneyness_rank,
                                                                            list(mdt_options_dict.keys()), OptionType.PUT)
            put_mdt_dict.update({mdt: mdt_options_dict.get(idx)})
        return [call_mdt_dict, put_mdt_dict]


    """ Mthd1: Determine atm option as the NEAREST strike from spot. 
        Get options by given moneyness rank and maturity date. """

    # 返回的option放在list里，是因为50ETF option可能有相近行权价的期权同时处于一个nearest strike
    def get_options_list_by_moneyness_mthd1(
            self, moneyness_rank: int, maturity: datetime.date) \
            -> List[List[BaseOption]]:
        mdt_calls, mdt_puts = self.get_orgnized_option_dict_for_moneyness_ranking()
        mdt_options_dict = mdt_calls.get(maturity)
        spot = list(mdt_options_dict.values())[0][0].underlying_close()
        k_call = self.OptionUtilClass.get_strike_by_monenyes_rank_nearest_strike(spot, moneyness_rank,
                                                                        list(mdt_options_dict.keys()), OptionType.CALL)
        call_list = mdt_options_dict.get(k_call)
        mdt_options_dict = mdt_puts.get(maturity)
        k_put = self.OptionUtilClass.get_strike_by_monenyes_rank_nearest_strike(spot, moneyness_rank,
                                                                        list(mdt_options_dict.keys()), OptionType.PUT)
        put_list = mdt_options_dict.get(k_put)
        return [call_list, put_list]

    """ Mthd2: Determine atm option as the nearest OTM strike from spot. 
        Get option maturity dictionary from all maturities by given moneyness rank. 
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档 """

    def get_options_dict_by_mdt_moneyness_mthd2(
            self, moneyness_rank: int) -> List[Dict[datetime.date, List[BaseOption]]]:
        mdt_calls, mdt_puts = self.get_orgnized_option_dict_for_moneyness_ranking()
        call_mdt_dict = {}
        put_mdt_dict = {}
        for mdt in mdt_calls.keys():
            mdt_options_dict = mdt_calls.get(mdt)
            spot = list(mdt_options_dict.values())[0][0].underlying_close()
            idx = self.OptionUtilClass.get_strike_by_monenyes_rank_otm_strike(spot, moneyness_rank,
                                                                        list(mdt_options_dict.keys()), OptionType.CALL)
            call_mdt_dict.update({mdt: mdt_options_dict.get(idx)})
        for mdt in mdt_puts.keys():
            mdt_options_dict = mdt_puts.get(mdt)
            spot = list(mdt_options_dict.values())[0][0].underlying_close()
            idx = self.OptionUtilClass.get_strike_by_monenyes_rank_otm_strike(spot, moneyness_rank,
                                                                        list(mdt_options_dict.keys()), OptionType.PUT)
            put_mdt_dict.update({mdt: mdt_options_dict.get(idx)})
        return [call_mdt_dict, put_mdt_dict]

    """ Mthd2: Determine atm option as the nearest OTM strike from spot. 
        Get options by given moneyness rank and maturity date. 
        # moneyness_rank：
        # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
        # -1：虚值level1：平值行权价往虚值方向移一档
        # 1: 实值level1： 平值新全价往实值方向移一档 """


    def get_options_list_by_moneyness_mthd2(self, moneyness_rank: int, maturity: datetime.date) \
            -> List[List[BaseOption]]:
        mdt_calls, mdt_puts = self.get_orgnized_option_dict_for_moneyness_ranking()
        mdt_options_dict = mdt_calls.get(maturity)
        spot = list(mdt_options_dict.values())[0][0].underlying_close()
        idx_call = self.OptionUtilClass.get_strike_by_monenyes_rank_otm_strike(spot, moneyness_rank,
                                                                    list(mdt_options_dict.keys()), OptionType.CALL)
        call_list = mdt_options_dict.get(idx_call)
        mdt_options_dict = mdt_puts.get(maturity)
        idx_put = self.OptionUtilClass.get_strike_by_monenyes_rank_otm_strike(spot, moneyness_rank,
                                                                    list(mdt_options_dict.keys()), OptionType.PUT)
        put_list = mdt_options_dict.get(idx_put)
        return [call_list,put_list]


    def select_maturity_date(self, nbr_maturity, min_holding : int=1):
        maturities = self.get_maturities_list()
        idx_start = 0
        if (maturities[idx_start]-self.eval_date).days <= min_holding:
            idx_start += 1
        idx_maturity = idx_start + nbr_maturity
        if idx_maturity > len(maturities) -1 :
            return
        else:
            return maturities[idx_maturity]

    # TODO: MOVE TO CONSTENT
    def select_higher_volume(self, options:List[BaseOption]) -> BaseOption:
        volume0 = 0.0
        res_option = None
        for option in options:
            volume = option.trading_volume()
            if volume >= volume0: res_option = option
            volume0 = volume
        return res_option

    # TODO: USE TOLYER'S EXPANSION.
    def yield_decomposition(self):
        return



    # """ Input optionset with the same maturity,get dictionary order by moneynesses as keys
    #     * ATM defined as FIRST OTM  """
    #
    # def update_options_by_moneyness_1(self, cd_underlying_price):
    #     c, p = self.get_maturities_option_dict()
    #     mdt_option_dict = self.get_maturities_option_dict()
    #     for mdt in mdt_option_dict.keys():
    #         option_by_mdt = mdt_option_dict.get(mdt)
    #     df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
    #     options_by_moneyness = {}
    #     for mdt in self.eligible_maturities:
    #         df_call = self.util.get_df_call_by_mdt(df, mdt)
    #         df_put = self.util.get_df_put_by_mdt(df, mdt)
    #         optionset_call = df_call[self.util.bktoption]
    #         optionset_put = df_put[self.util.bktoption]
    #         dict_call = {}
    #         dict_put = {}
    #         res_call = {}
    #         res_put = {}
    #         atm_call = 1000
    #         atm_put = -1000
    #         if cd_underlying_price == 'close':
    #             spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
    #         else:
    #             spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
    #         m_call = []
    #         m_put = []
    #         for option in optionset_call:
    #             k = option.strike()  # TODO: why not adj_strike?
    #             m = round(k - spot, 6)
    #             if m >= 0:
    #                 atm_call = min(atm_call, m)
    #             dict_call.update({m: option})
    #             m_call.append(m)
    #         for option in optionset_put:
    #             k = option.strike()
    #             m = round(k - spot, 6)
    #             if m <= 0:
    #                 atm_put = max(atm_put, m)
    #             dict_put.update({m: option})
    #             m_put.append(m)
    #         keys_call = sorted(dict_call)
    #         keys_put = sorted(dict_put)
    #         if atm_call == 1000: atm_call = max(m_call)
    #         if atm_put == -1000: atm_put = min(m_put)
    #         idx_call = keys_call.index(atm_call)
    #         for (i, key) in enumerate(keys_call):
    #             res_call.update({idx_call - i: dict_call[key]})
    #         idx_put = keys_put.index(atm_put)
    #         for (i, key) in enumerate(keys_put):
    #             res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
    #         res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
    #         options_by_moneyness.update({mdt: res_callput})
    #     return options_by_moneyness
    #
    # """ Input optionset with the same maturity,get dictionary order by moneynesses as keys
    #     * ATM defined as THAT WITH STRIKE CLOSEST WITH UNDERLYING PRICE """
    #
    # def update_options_by_moneyness_2(self, cd_underlying_price):
    #     df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
    #     options_by_moneyness = {}
    #     for mdt in self.eligible_maturities:
    #         df_call = self.util.get_df_call_by_mdt(df, mdt)
    #         df_put = self.util.get_df_put_by_mdt(df, mdt)
    #         optionset_call = df_call[self.util.bktoption]
    #         optionset_put = df_put[self.util.bktoption]
    #         dict_call = {}
    #         dict_put = {}
    #         res_call = {}
    #         res_put = {}
    #         if cd_underlying_price == 'close':
    #             spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
    #         else:
    #             spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
    #         dict_m = {}
    #         for option in optionset_call:
    #             k = option.strike()
    #             m = round(k - spot, 6)
    #             dict_call.update({m: option})
    #             dict_m.update({abs(m): m})
    #         for option in optionset_put:
    #             k = option.strike()
    #             m = round(k - spot, 6)
    #             dict_put.update({m: option})
    #         atm = dict_m[min(dict_m.keys())]
    #         keys_call = sorted(dict_call)
    #         keys_put = sorted(dict_put)
    #         idx_call = keys_call.index(atm)
    #         for (i, key) in enumerate(keys_call):
    #             res_call.update({idx_call - i: dict_call[key]})
    #         idx_put = keys_put.index(atm)
    #         for (i, key) in enumerate(keys_put):
    #             res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
    #         res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
    #         options_by_moneyness.update({mdt: res_callput})
    #     return options_by_moneyness

    #     def start(self):
    #         self.dt_list = sorted(self.df_data[self.util.col_date].unique())
    #         self.start_date = self.dt_list[0]  # 0
    #         self.end_date = self.dt_list[-1]  # len(self.dt_list)-1
    #         self.eval_date = self.start_date
    #         self.validate_data()
    #         # self.add_bktoption_column()
    #         self.df_last_state = pd.DataFrame()
    #         self.update_adjustment()
    #         self.update_current_daily_state()
    #         self.update_eligible_maturities()
    #         self.update_bktoption()
    # .
    #     def next(self):
    #         if self.frequency in self.util.cd_frequency_low:
    #             self.df_last_state = self.df_daily_state
    #             self.update_eval_date()
    #             self.update_current_daily_state()
    #             self.update_eligible_maturities()
    #         self.update_bktoption()
    #
    #     def update_eval_date(self):
    #         self.index += 1
    #         self.eval_date = self.dt_list[self.dt_list.index(self.eval_date) + 1]
    #
    #     def update_current_daily_state(self):
    #         self.df_daily_state = self.df_data[self.df_data[self.util.col_date] == self.eval_date].reset_index(drop=True)
    #
    #     """ Update bktoption in daily state """
    #
    #     def update_bktoption(self):
    #         if self.frequency in self.util.cd_frequency_low:
    #             df_last_state = self.df_last_state
    #             ids_last = []
    #             if not df_last_state.empty:
    #                 ids_last = df_last_state[self.util.id_instrument].tolist()
    #             for (idx, row) in self.df_daily_state.iterrows():
    #                 id_inst = row[self.util.id_instrument]
    #                 if id_inst in ids_last:
    #                     bktoption = \
    #                         df_last_state[df_last_state[self.util.id_instrument] == id_inst][self.util.bktoption].values[0]
    #                     bktoption.next()
    #                     self.df_daily_state.loc[idx, self.util.bktoption] = bktoption
    #                 else:
    #                     df_option = self.df_data[self.df_data[self.util.col_id_instrument] == id_inst].reset_index(
    #                         drop=True)
    #                     bktoption = BktOption(df_option, self.flag_calculate_iv, rf=self.rf)
    #                     self.df_daily_state.loc[idx, self.util.bktoption] = bktoption
    #             self.bktoptionset = set(self.df_daily_state[self.util.bktoption].tolist())
    #
    #     def validate_data(self):
    #         self.df_data[self.util.col_option_price] = self.df_data.apply(self.util.fun_option_price, axis=1)
    #
    #     def update_eligible_maturities(self):  # n: 要求合约剩余期限大于n（天）
    #         underlyingids = self.df_daily_state[self.util.col_id_underlying].unique()
    #         maturities = self.df_daily_state[self.util.col_maturitydt].unique()
    #         maturity_dates2 = []
    #         if self.option_code in ['sr', 'm']:
    #             for underlying_id in underlyingids:
    #                 m1 = int(underlying_id[-2:])
    #                 if m1 not in [1, 5, 9]:
    #                     continue
    #                 c = self.df_daily_state[self.util.col_id_underlying] == underlying_id
    #                 df_tmp = self.df_daily_state[c]
    #                 mdt = df_tmp[self.util.col_maturitydt].values[0]
    #                 ttm = (mdt - self.eval_date).days
    #                 if ttm > self.min_ttm:
    #                     maturity_dates2.append(mdt)
    #         else:
    #             for mdt in maturities:
    #                 ttm = (mdt - self.eval_date).days
    #                 if ttm > self.min_ttm:
    #                     maturity_dates2.append(mdt)
    #         self.eligible_maturities = sorted(maturity_dates2)
    #
    #     """ 主要针对50ETF期权分红 """
    #
    #     def update_adjustment(self):
    #         self.update_multiplier_adjustment()
    #         self.update_applicable_strikes()
    #
    #     def update_multiplier_adjustment(self):
    #         if self.option_code == '50etf':
    #             self.df_data[self.util.col_adj_strike] = \
    #                 round(self.df_data[self.util.col_strike] * self.df_data[self.util.col_multiplier] / 10000, 2)
    #             self.df_data[self.util.col_adj_option_price] = \
    #                 round(self.df_data[self.util.col_settlement] * self.df_data[self.util.col_multiplier] / 10000, 2)
    #
    #         else:
    #             self.df_data[self.util.col_adj_strike] = self.df_data[self.util.col_strike]
    #             self.df_data[self.util.col_adj_option_price] = self.df_data[self.util.col_settlement]
    #
    #     def update_applicable_strikes(self):
    #         if self.option_code == '50etf':
    #             self.df_data = self.util.get_applicable_strike_df(self.df_data)
    #
    #     def add_dtdate_column(self):
    #         if self.util.col_date not in self.df_data.columns.tolist():
    #             for (idx, row) in self.df_data.iterrows():
    #                 self.df_data.loc[idx, self.util.col_date] = row[self.util.col_datetime].date()
    #
    #     def get_put(self, moneyness_rank, mdt, cd_long_short, cd_underlying_price='open'):
    #         # moneyness_rank：
    #         # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
    #         # -1：虚值level1：平值行权价往虚值方向移一档
    #         # 1: 实值level1： 平值新全价往实值方向移一档
    #         options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
    #         res_dict = options_by_moneyness[mdt][self.util.type_put]
    #         if res_dict == {}:
    #             print('bkt_option_set--get put failed,option dict is empty!')
    #             return pd.DataFrame()
    #         if moneyness_rank not in res_dict.keys():
    #             print('bkt_option_set--get put failed,given moneyness rank not exit!')
    #             return pd.DataFrame()
    #         option_put = res_dict[moneyness_rank]
    #         portfolio = Puts(self.eval_date, [option_put], cd_long_short)
    #         return portfolio
    #
    #     def get_call(self, moneyness_rank, mdt, cd_long_short, cd_underlying_price='close', cd_moneyness_method=None):
    #         # moneyness_rank：
    #         # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
    #         # -1：虚值level1：平值行权价往虚值方向移一档
    #         # 1: 实值level1： 平值新全价往实值方向移一档
    #         options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price, cd_moneyness_method)
    #         res_dict = options_by_moneyness[mdt][self.util.type_call]
    #         if res_dict == {}:
    #             print('bkt_option_set--get_call failed,option dict is empty!')
    #             return pd.DataFrame()
    #         if moneyness_rank not in res_dict.keys():
    #             print('bkt_option_set--get_call failed,given moneyness rank not exit!')
    #             return pd.DataFrame()
    #         option_call = res_dict[moneyness_rank]
    #         portfolio = Calls(self.eval_date, [option_call], cd_long_short)
    #         return portfolio
    #
    #     """moneyness =0 : 跨式策略，moneyness = -1/-2 : 宽跨式策略"""
    #
    #     def get_straddle(self, moneyness_rank, mdt, delta_exposure, long_short, cd_underlying_price='close'):
    #         # moneyness_rank：
    #         # 0：平值: call strike=大于spot值的最小行权价; put strike=小于spot值的最大行权价
    #         # -1：虚值level1：平值行权价往虚值方向移一档
    #         # 1: 实值level1： 平值行权价往实值方向移一档
    #         options_by_moneyness = self.get_moneyness_iv_by_mdt(mdt, cd_underlying_price)
    #         if moneyness_rank not in options_by_moneyness[self.util.type_call].keys() or \
    #                         moneyness_rank not in options_by_moneyness[self.util.type_put].keys():
    #             return
    #         else:
    #             option_call = options_by_moneyness[self.util.type_call][moneyness_rank]
    #             option_put = options_by_moneyness[self.util.type_put][moneyness_rank]
    #         straddle = Straddle(self.eval_date, option_call, option_put, delta_exposure, long_short)
    #         return straddle
    #
    #     """Calendar Spread: Long far month and short near month;'option_type=None' means both call and put are included"""
    #
    #     def get_calendar_spread_long(self, moneyness_rank, mdt1, mdt2, option_type, cd_underlying_price='close'):
    #         if mdt1 > mdt2:
    #             print('get_calendar_spread_call : mdt1 > mdt2')
    #             return
    #         options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
    #         option_mdt1 = options_by_moneyness[mdt1][option_type][moneyness_rank]  # short
    #         option_mdt2 = options_by_moneyness[mdt2][option_type][moneyness_rank]  # long
    #         cs = CalandarSpread(self.eval_date, option_mdt1, option_mdt2, option_type)
    #         return cs
    #
    #     """Back Spread: Long small delta(atm), short large delta(otm)"""
    #
    #     def get_backspread(self, option_type, mdt, moneyness1=0, moneyness2=-2, cd_underlying_price='close'):
    #         options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
    #         if moneyness2 not in options_by_moneyness[mdt][option_type].keys():
    #             moneyness2 += 1
    #             moneyness1 += 1
    #             print(self.eval_date, ' Move moneyness rank for lack of stikes')
    #         option_long = options_by_moneyness[mdt][option_type][moneyness2]
    #         option_short = options_by_moneyness[mdt][option_type][moneyness1]
    #         bs = BackSpread(self.eval_date, option_long, option_short, option_type)
    #         return bs
    #
    #     def get_collar2(self, mdt, underlying, moneyness_call=-2, moneyness_put=-2, cd_underlying_price='close'):
    #         options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
    #         if moneyness_call not in options_by_moneyness[mdt][self.util.type_call].keys():
    #             write_call = None
    #         else:
    #             write_call = options_by_moneyness[mdt][self.util.type_call][moneyness_call]
    #         if moneyness_put not in options_by_moneyness[mdt][self.util.type_put].keys():
    #             buy_put = None
    #         else:
    #             buy_put = options_by_moneyness[mdt][self.util.type_put][moneyness_put]
    #         collar = Collar(self.eval_date, buy_put=buy_put, write_call=write_call, underlying=underlying)
    #         return collar
    #
    #     def get_collar(self, mdt_call, mdt_put, underlying, moneyness_call=-2, moneyness_put=-2,
    #                    cd_underlying_price='close', flag_protect=False):
    #         options_by_moneyness = self.update_options_by_moneyness(cd_underlying_price)
    #         while moneyness_call < 0:
    #             if moneyness_call not in options_by_moneyness[mdt_call][self.util.type_call].keys():
    #                 moneyness_call += 1
    #             else:
    #                 break
    #         while moneyness_put <= 0:
    #             if moneyness_put not in options_by_moneyness[mdt_put][self.util.type_put].keys():
    #                 moneyness_put += 1
    #             else:
    #                 break
    #         buy_put = options_by_moneyness[mdt_put][self.util.type_put][moneyness_put]
    #         write_call = options_by_moneyness[mdt_call][self.util.type_call][moneyness_call]
    #         "No 1st otm put/call (moneyness=-1) to by/write, stop collar strategy"
    #         if moneyness_call >= 0:
    #             write_call = None
    #             buy_put = None
    #         if moneyness_put >= 0:
    #             write_call = None
    #             # if not flag_protect:
    #             #     buy_put = None
    #         collar = Collar(self.eval_date, buy_put=buy_put, write_call=write_call, underlying=underlying)
    #         return collar
    #
    #     def update_options_by_moneyness(self, cd_underlying_price='open', cd_moneyness_method=None):
    #         if cd_moneyness_method == None or cd_moneyness_method == self.util.method_1:
    #             res = self.update_options_by_moneyness_1(cd_underlying_price)
    #         else:
    #             res = self.update_options_by_moneyness_2(cd_underlying_price)
    #         return res
    #
    #     """ Input optionset with the same maturity,get dictionary order by moneynesses as keys
    #         * ATM defined as FIRST OTM  """
    #
    #     def update_options_by_moneyness_1(self, cd_underlying_price):
    #         df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
    #         options_by_moneyness = {}
    #         for mdt in self.eligible_maturities:
    #             df_call = self.util.get_df_call_by_mdt(df, mdt)
    #             df_put = self.util.get_df_put_by_mdt(df, mdt)
    #             optionset_call = df_call[self.util.bktoption]
    #             optionset_put = df_put[self.util.bktoption]
    #             dict_call = {}
    #             dict_put = {}
    #             res_call = {}
    #             res_put = {}
    #             atm_call = 1000
    #             atm_put = -1000
    #             if cd_underlying_price == 'close':
    #                 spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
    #             else:
    #                 spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
    #             m_call = []
    #             m_put = []
    #             for option in optionset_call:
    #                 k = option.strike()  # TODO: why not adj_strike?
    #                 m = round(k - spot, 6)
    #                 if m >= 0:
    #                     atm_call = min(atm_call, m)
    #                 dict_call.update({m: option})
    #                 m_call.append(m)
    #             for option in optionset_put:
    #                 k = option.strike()
    #                 m = round(k - spot, 6)
    #                 if m <= 0:
    #                     atm_put = max(atm_put, m)
    #                 dict_put.update({m: option})
    #                 m_put.append(m)
    #             keys_call = sorted(dict_call)
    #             keys_put = sorted(dict_put)
    #             if atm_call == 1000: atm_call = max(m_call)
    #             if atm_put == -1000: atm_put = min(m_put)
    #             idx_call = keys_call.index(atm_call)
    #             for (i, key) in enumerate(keys_call):
    #                 res_call.update({idx_call - i: dict_call[key]})
    #             idx_put = keys_put.index(atm_put)
    #             for (i, key) in enumerate(keys_put):
    #                 res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
    #             res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
    #             options_by_moneyness.update({mdt: res_callput})
    #         return options_by_moneyness
    #
    #     """ Input optionset with the same maturity,get dictionary order by moneynesses as keys
    #         * ATM defined as THAT WITH STRIKE CLOSEST WITH UNDERLYING PRICE """
    #
    #     def update_options_by_moneyness_2(self, cd_underlying_price):
    #         df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
    #         options_by_moneyness = {}
    #         for mdt in self.eligible_maturities:
    #             df_call = self.util.get_df_call_by_mdt(df, mdt)
    #             df_put = self.util.get_df_put_by_mdt(df, mdt)
    #             optionset_call = df_call[self.util.bktoption]
    #             optionset_put = df_put[self.util.bktoption]
    #             dict_call = {}
    #             dict_put = {}
    #             res_call = {}
    #             res_put = {}
    #             if cd_underlying_price == 'close':
    #                 spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
    #             else:
    #                 spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
    #             dict_m = {}
    #             for option in optionset_call:
    #                 k = option.strike()
    #                 m = round(k - spot, 6)
    #                 dict_call.update({m: option})
    #                 dict_m.update({abs(m): m})
    #             for option in optionset_put:
    #                 k = option.strike()
    #                 m = round(k - spot, 6)
    #                 dict_put.update({m: option})
    #             atm = dict_m[min(dict_m.keys())]
    #             keys_call = sorted(dict_call)
    #             keys_put = sorted(dict_put)
    #             idx_call = keys_call.index(atm)
    #             for (i, key) in enumerate(keys_call):
    #                 res_call.update({idx_call - i: dict_call[key]})
    #             idx_put = keys_put.index(atm)
    #             for (i, key) in enumerate(keys_put):
    #                 res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
    #             res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
    #             options_by_moneyness.update({mdt: res_callput})
    #         return options_by_moneyness
    #
    #     def get_moneyness_iv_by_mdt(self, mdt, cd_underlying_price='open', cd_moneyness_method=None):
    #         if cd_moneyness_method == None or cd_moneyness_method == self.util.method_1:
    #             res = self.get_moneyness_iv_by_mdt_1(mdt, cd_underlying_price)
    #         else:
    #             res = self.get_moneyness_iv_by_mdt_2(mdt, cd_underlying_price)
    #         return res
    #
    #     """ Given target maturity date, get call and put iv sorted by moneyness rank
    #         * ATM defined as FIRST OTM  """
    #
    #     def get_moneyness_iv_by_mdt_1(self, mdt, cd_underlying_price):
    #         df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
    #         df_call = self.util.get_df_call_by_mdt(df, mdt)
    #         df_put = self.util.get_df_put_by_mdt(df, mdt)
    #         optionset_call = df_call[self.util.bktoption]
    #         optionset_put = df_put[self.util.bktoption]
    #         dict_call = {}
    #         dict_put = {}
    #         res_call = {}
    #         res_put = {}
    #         atm_call = 1000
    #         atm_put = -1000
    #         if cd_underlying_price == 'close':
    #             spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
    #         else:
    #             spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
    #         m_call = []
    #         m_put = []
    #         for option in optionset_call:
    #             k = option.strike()
    #             m = round(k - spot, 6)
    #             if m >= 0:
    #                 atm_call = min(atm_call, m)
    #             dict_call.update({m: option})
    #             m_call.append(m)
    #         for option in optionset_put:
    #             k = option.strike()
    #             m = round(k - spot, 6)
    #             if m <= 0:
    #                 atm_put = max(atm_put, m)
    #             dict_put.update({m: option})
    #             m_put.append(m)
    #         keys_call = sorted(dict_call)
    #         keys_put = sorted(dict_put)
    #         if atm_call == 1000: atm_call = max(m_call)
    #         if atm_put == -1000: atm_put = min(m_put)
    #         idx_call = keys_call.index(atm_call)
    #         for (i, key) in enumerate(keys_call):
    #             res_call.update({idx_call - i: dict_call[key]})
    #         idx_put = keys_put.index(atm_put)
    #         for (i, key) in enumerate(keys_put):
    #             res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
    #         res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
    #         return res_callput
    #
    #     """ Given target maturity date, get call and put iv sorted by moneyness rank
    #         * ATM defined as THAT WITH STRIKE CLOSEST WITH UNDERLYING PRICE """
    #
    #     def get_moneyness_iv_by_mdt_2(self, mdt, cd_underlying_price):
    #         df = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
    #         df_call = self.util.get_df_call_by_mdt(df, mdt)
    #         df_put = self.util.get_df_put_by_mdt(df, mdt)
    #         optionset_call = df_call[self.util.bktoption]
    #         optionset_put = df_put[self.util.bktoption]
    #         dict_call = {}
    #         dict_put = {}
    #         res_call = {}
    #         res_put = {}
    #         if cd_underlying_price == 'close':
    #             spot = optionset_call[0].underlying_close()  # Use underlying OPEN close as spot
    #         else:
    #             spot = optionset_call[0].underlying_open_price()  # Use underlying OPEN close as spot
    #         dict_m = {}
    #         for option in optionset_call:
    #             k = option.strike()
    #             m = round(k - spot, 6)
    #             dict_call.update({m: option})
    #             dict_m.update({abs(m): m})
    #         for option in optionset_put:
    #             k = option.strike()
    #             m = round(k - spot, 6)
    #             dict_put.update({m: option})
    #         atm = dict_m[min(dict_m.keys())]
    #         keys_call = sorted(dict_call)
    #         keys_put = sorted(dict_put)
    #         idx_call = keys_call.index(atm)
    #         for (i, key) in enumerate(keys_call):
    #             res_call.update({idx_call - i: dict_call[key]})
    #         idx_put = keys_put.index(atm)
    #         for (i, key) in enumerate(keys_put):
    #             res_put.update({i - idx_put: dict_put[key]})  # moneyness : option
    #         res_callput = {self.util.type_call: res_call, self.util.type_put: res_put}
    #         return res_callput

    """ get key volatility points """
    # def get_mdt_keyvols(self, option_type):
    #     keyvols_mdts = {}
    #     df_data = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
    #     for mdt in self.eligible_maturities:
    #         df_mdt = self.util.get_df_by_mdt_type(df_data, mdt, option_type)
    #         df = self.calculate_implied_vol(df_mdt).sort_values(by=[self.util.col_applicable_strike])
    #         spot = df_mdt[self.util.bktoption].values[0].underlying_close()
    #         strikes = []
    #         vols = []
    #         for (idx, row) in df.iterrows():
    #             strike = row[self.util.col_applicable_strike]
    #             iv = row[self.util.col_implied_vol]
    #             # if iv > 0:
    #             strikes.append(float(strike))
    #             vols.append(iv)
    #             # else:
    #             #     continue
    #         volset = [vols]
    #         m_list = [[mdt]]
    #         vol_matrix = ql.Matrix(len(strikes), len(m_list))
    #         for i in range(vol_matrix.rows()):
    #             for j in range(vol_matrix.columns()):
    #                 vol_matrix[i][j] = volset[j][i]
    #         ql_evalDate = self.util.to_ql_date(self.eval_date)
    #         ql_maturities = [ql.Date(mdt.day, mdt.month, mdt.year)]
    #         try:
    #             black_var_surface = ql.BlackVarianceSurface(
    #                 ql_evalDate, self.calendar, ql_maturities, strikes, vol_matrix, self.daycounter)
    #             keyvols_mdt = {}
    #             try:
    #                 if min(strikes) > spot:
    #                     s = min(strikes)
    #                 elif max(strikes) < spot:
    #                     s = max(strikes)
    #                 else:
    #                     s = spot
    #                 vol_100 = black_var_surface.blackVol(ql_maturities[0], s)
    #                 keyvols_mdt.update({100: vol_100})
    #             except Exception as e:
    #                 print(e)
    #                 pass
    #             try:
    #                 vol_110 = black_var_surface.blackVol(ql_maturities[0], spot * 1.1)
    #                 keyvols_mdt.update({110: vol_110})
    #             except Exception as e:
    #                 pass
    #             try:
    #                 vol_105 = black_var_surface.blackVol(ql_maturities[0], spot * 1.05)
    #                 keyvols_mdt.update({105: vol_105})
    #             except Exception as e:
    #                 pass
    #             try:
    #                 vol_90 = black_var_surface.blackVol(ql_maturities[0], spot * 0.9)
    #                 keyvols_mdt.update({90: vol_90})
    #             except Exception as e:
    #                 pass
    #             try:
    #                 vol_95 = black_var_surface.blackVol(ql_maturities[0], spot * 0.95)
    #                 keyvols_mdt.update({95: vol_95})
    #             except Exception as e:
    #                 pass
    #             keyvols_mdts.update({mdt: keyvols_mdt})
    #         except Exception as e:
    #             print(e)
    #     return keyvols_mdts

    """ Get 1M atm vol by liner interpolation """

    # def get_interpolated_atm_1M(self, option_type):
    #     keyvols_mdts = self.get_mdt_keyvols(option_type)
    #     ql_evalDate = self.util.to_ql_date(self.eval_date)
    #     mdt_1m = self.util.to_dt_date(self.calendar.advance(ql_evalDate, ql.Period(1, ql.Months)))
    #     d0 = datetime.date(1999, 1, 1)
    #     mdt_1m_num = (mdt_1m - d0).days
    #     maturities_num = []
    #     atm_vols = []
    #     for m in self.eligible_maturities:
    #         maturities_num.append((m - d0).days)
    #         atm_vols.append(keyvols_mdts[m][100])  # atm vol : skrike is 100% spot
    #     try:
    #         x = np.interp(mdt_1m_num, maturities_num, atm_vols)
    #     except Exception as e:
    #         print(e)
    #         return
    #     return x

    """ Get Call/Put volatility surface separately"""

    # def get_volsurface_squre(self, df):
    #     ql_maturities = []
    #     df = self.calculate_implied_vol(df)
    #     df_mdt_list = []
    #     iv_name_list = []
    #     maturity_list = []
    #     for idx, mdt in enumerate(self.eligible_maturities):
    #         iv_rename = 'implied_vol_' + str(idx)
    #         if self.option_code == '50etf':
    #             col_strike = self.util.col_applicable_strike
    #         else:
    #             col_strike = self.util.col_strike
    #         df_mkt = df[(df[self.util.col_maturitydt] == mdt)] \
    #             .rename(columns={self.util.col_implied_vol: iv_rename}) \
    #             .set_index(col_strike).sort_index()
    #         if len(df_mkt) == 0: continue
    #         df_mdt_list.append(df_mkt)
    #         iv_name_list.append(iv_rename)
    #         maturity_list.append(mdt)
    #     df_vol = pd.concat(df_mdt_list, axis=1, join='inner')
    #     strikes = []
    #     for k in df_vol.index:
    #         strikes.append(float(k))
    #     volset = []
    #     for name in iv_name_list:
    #         volset.append(df_vol[name].tolist())
    #     for mdate in maturity_list:
    #         ql_maturities.append(ql.Date(mdate.day, mdate.month, mdate.year))
    #     vol_matrix = ql.Matrix(len(strikes), len(maturity_list))
    #     for i in range(vol_matrix.rows()):
    #         for j in range(vol_matrix.columns()):
    #             vol_matrix[i][j] = volset[j][i]
    #     ql_evalDate = self.util.to_ql_date(self.eval_date)
    #     black_var_surface = ql.BlackVarianceSurface(
    #         ql_evalDate, self.calendar, ql_maturities, strikes, vol_matrix, self.daycounter)
    #     return black_var_surface

    """ Get Integrate Volatility Surface by call/put mid vols"""

    # def get_mid_volsurface_squre(self):
    #     ql_maturities = []
    #     call_list = []
    #     put_list = []
    #     df_mdt_list = []
    #     iv_name_list = []
    #     maturity_list = []
    #     for option in self.bktoptionset:
    #         if option.option_type == self.util.type_call:
    #             call_list.append(option)
    #         else:
    #             put_list.append(option)
    #     df_call = self.util.get_duplicate_strikes_dropped(self.util.get_df_by_type(self.df_daily_state, self.util.type_call))
    #     df_put = self.util.get_duplicate_strikes_dropped(self.util.get_df_by_type(self.df_daily_state, self.util.type_put))
    #     df_call = self.calculate_implied_vol(df_call)
    #     df_put = self.calculate_implied_vol(df_put)
    #     df_call['maturity_call'] = df_call[self.util.col_maturitydt]
    #     df_call['adj_strike_call'] = df_call[self.util.col_adj_strike]
    #     df_call = df_call.set_index([self.util.col_maturitydt, self.util.col_adj_strike]) \
    #         .rename(columns={self.util.col_implied_vol: 'iv_call'})
    #     df_put = df_put.set_index([self.util.col_maturitydt, self.util.col_adj_strike]) \
    #         .rename(columns={self.util.col_implied_vol: 'iv_put'})
    #     df = df_call[['adj_strike_call', 'maturity_call', 'iv_call']] \
    #         .join(df_put[['iv_put']])
    #     df['mid_vol'] = (df['iv_call'] + df['iv_put']) / 2
    #     maturities = sorted(df['maturity_call'].unique())
    #     for idx, mdt in enumerate(maturities):
    #         if mdt <= self.eval_date: continue
    #         iv_rename = 'implied_vol_' + str(idx)
    #         df_mkt = df[(df['maturity_call'] == mdt)] \
    #             .rename(columns={'mid_vol': iv_rename}).sort_values(by='adj_strike_call').set_index('adj_strike_call')
    #         if len(df_mkt) == 0: continue
    #         df_mdt_list.append(df_mkt)
    #         iv_name_list.append(iv_rename)
    #         maturity_list.append(mdt)
    #     df_vol = pd.concat(df_mdt_list, axis=1, join='inner')
    #     strikes = []
    #     for k in df_vol.index:
    #         strikes.append(float(k))
    #     volset = []
    #     for name in iv_name_list:
    #         volset.append(df_vol[name].tolist())
    #     for mdate in maturity_list:
    #         ql_maturities.append(ql.Date(mdate.day, mdate.month, mdate.year))
    #     vol_matrix = ql.Matrix(len(strikes), len(maturity_list))
    #     for i in range(vol_matrix.rows()):
    #         for j in range(vol_matrix.columns()):
    #             vol_matrix[i][j] = volset[j][i]
    #     ql_evalDate = self.util.to_ql_date(self.eval_date)
    #     black_var_surface = ql.BlackVarianceSurface(
    #         ql_evalDate, self.calendar, ql_maturities, strikes, vol_matrix, self.daycounter)
    #     return black_var_surface

    # def calculate_implied_vol(self, df):
    #     for (idx, row) in df.iterrows():
    #         option = row[self.util.bktoption]
    #         iv = option.get_implied_vol()
    #         df.loc[idx, self.util.col_implied_vol] = iv
    #     return df

        # def collect_option_metrics(self, hp=30):
        #     res = []
        #     df = pd.DataFrame(columns=[self.util.col_date, self.util.col_carry, self.util.bktoption])
        #     bktoption_list = self.bktoptionset
        #     if len(bktoption_list) == 0: return df
        #     if self.option_code == '50etf':
        #         df_data = self.util.get_duplicate_strikes_dropped(self.df_daily_state)
        #     else:
        #         df_data = self.df_daily_state
        #     df_data_call = self.util.get_df_by_type(df_data, self.util.type_call)
        #     df_data_put = self.util.get_df_by_type(df_data, self.util.type_put)
        #     bvs_call = self.get_volsurface_squre(df_data_call)
        #     bvs_put = self.get_volsurface_squre(df_data_put)
        #     for idx, option in enumerate(bktoption_list):
        #         if option.option_price() > 0.0:
        #             iv = option.get_implied_vol()
        #             if option.option_type == self.util.type_call:
        #                 carry = option.get_carry(bvs_call, hp)
        #             else:
        #                 carry = option.get_carry(bvs_put, hp)
        #             theta = option.get_theta()
        #             vega = option.get_vega()
        #
        #             delta = option.get_delta()
        #             rho = option.get_rho()
        #             gamma = option.get_gamma()
        #             if carry == None or np.isnan(carry): carry = -999.0
        #             if theta == None or np.isnan(theta): theta = -999.0
        #             if vega == None or np.isnan(vega): vega = -999.0
        #             if gamma == None or np.isnan(gamma): gamma = -999.0
        #             if iv == None or np.isnan(iv): iv = -999.0
        #             if delta == None or np.isnan(delta): delta = -999.0
        #             if rho == None or np.isnan(rho): rho = -999.0
        #         else:
        #             iv = option.get_implied_vol()
        #             carry = theta = vega = gamma = delta = rho = -999.0
        #             if iv == None or np.isnan(iv): iv = -999.0
        #         if self.flag_calculate_iv:
        #             datasource = 'calculated'
        #         else:
        #             if self.option_code == 'm':
        #                 datasource = 'dce'
        #             else:
        #                 datasource = 'czce'
        #         db_row = {
        #             self.util.col_date: self.eval_date,
        #             self.util.col_id_instrument: option.id_instrument(),
        #             'datasource': datasource,
        #             'name_code': self.option_code,
        #             'id_underlying': option.id_underlying(),
        #             'amt_strike': float(option.strike()),
        #             self.util.col_code_instrument: option.code_instrument(),
        #             self.util.col_option_type: option.option_type(),
        #             self.util.col_maturitydt: option.maturitydt(),
        #             self.util.col_implied_vol: float(iv),
        #             self.util.col_adj_strike: float(option.adj_strike()),
        #             self.util.col_option_price: float(option.option_price()),
        #             'amt_delta': float(delta),
        #             self.util.col_vega: float(vega),
        #             self.util.col_theta: float(theta),
        #             'amt_rho': float(rho),
        #             'amt_gamma': float(gamma),
        #             'amt_carry_1M': float(carry),
        #             'timestamp': datetime.datetime.today()
        #         }
        #         res.append(db_row)
        #     return res
