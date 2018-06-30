from OptionStrategyLib.OptionPricing.Options import OptionPlainEuropean
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
import QuantLib as ql
import datetime
from pandas import DataFrame, Series
import numpy as np
from back_test.model.constant import FrequentType, Util, PricingType, EngineType
from back_test.model.abstract_base_product import AbstractBaseProduct


class BaseOption(AbstractBaseProduct):
    """ Contain metrics and trading position info as attributes """

    def __init__(self, df_data: DataFrame, id_instrument: str, flag_calculate_iv: bool = False, rf: float = 0.03,
                 frequency: FrequentType = FrequentType.DAILY, calendar=None, daycounter=None,
                 pricing_type=PricingType.OptionPlainEuropean, engine_type=EngineType.AnalyticEuropeanEngine):
        super().__init__()
        self.frequency: FrequentType = frequency
        self.df_data: DataFrame = df_data
        self.id_instrument = id_instrument
        # TODO maybe use enum is better
        self.name_code = id_instrument.split('_')[0]
        self.nbr_index: int = df_data.shape[0]
        self.current_index: int = -1
        self.current_state: Series = None
        # TODO why this property?
        # self.dt_list = sorted(self.df_metrics[self.util.col_date].unique())
        self.eval_date: datetime.date = None
        self.rf = rf
        self.flag_calculate_iv = flag_calculate_iv
        self.pricing_type = pricing_type
        self.engine_type = engine_type
        self.daycounter = daycounter
        self.calendar = calendar
        self.implied_vol = None
        self.evaluation = None
        self.pre_process()
        self.update_current_state()

    def next(self):
        self.implied_vol = None
        self.update_current_state()
        # self.update_pricing_metrics()

    def pre_process(self) -> None:
        # filter function to filter out ivalid data from dataframe
        def filter_invalid_data(x):
            cur_date = x[Util.DT_DATE]
            if x[Util.DT_DATETIME] >= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 9, 30, 00) and \
                    x[
                        Util.DT_DATETIME] <= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 11, 30,
                                                               00):
                return True
            if x[Util.DT_DATETIME] >= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 13, 00, 00) and \
                    x[
                        Util.DT_DATETIME] <= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 15, 00,
                                                               00):
                return True
            return False

        if self.frequency not in Util.LOW_FREQUENT:
            # overwrite date col based on data in datetime col.
            self.df_data[Util.DT_DATE] = self.df_data[Util.DT_DATETIME].apply(lambda x: x.date())
            mask = self.df_data.apply(filter_invalid_data, axis=1)
            self.df_data = self.df_data[mask].reset_index(drop=True)
        self.generate_required_columns_if_missing()

    def generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.OPTION_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if not columns.contains(column):
                print("{} missing column {}", self.__repr__(), column)
                self.df_data[column] = None

    def update_current_state(self) -> None:
        self.current_index += 1
        self.current_state = self.df_data.loc[self.current_index]
        self.eval_date = self.current_state[Util.DT_DATE]

    def get_current_state(self) -> Series:
        return self.current_state

    def __repr__(self) -> str:
        return 'BaseOption(id_instrument: {0},eval_date: {1},current_index: {2},frequency: {3})' \
            .format(self.id_instrument, self.eval_date, self.current_index, self.frequency)

    # TODO: maybe pass a datetime to Evalueation to get avoid of Quantlib library. Plus, what's evaluation for?
    def update_evaluation(self):
        ql_evalDate = ql.Date(self.eval_date.day, self.eval_date.month, self.eval_date.year)
        evaluation = Evaluation(ql_evalDate, self.daycounter, self.calendar)
        self.evaluation = evaluation

    def update_pricing_metrics(self):
        # TODO put this logic in evaluation might be better
        if self.evaluation is not None and self.evaluation.evalDate == \
                ql.Date(self.eval_date.day, self.eval_date.month, self.eval_date.year):
            return
        self.update_evaluation()
        if self.pricing_metrics is None:
            if self.pricing_type == PricingType.OptionPlainEuropean:
                ql_maturitydt = ql.Date(self.maturitydt().day,
                                        self.maturitydt().month,
                                        self.maturitydt().year)
                if self.option_type() == Util.TYPE_CALL:
                    ql_optiontype = ql.Option.Call
                elif self.option_type() == Util.TYPE_PUT:
                    ql_optiontype = ql.Option.Put
                else:
                    print('No option type!')
                    return
                # strike = self.util.get_applicable_strike(self)
                if self.name_code() == '50etf':
                    strike = self.applicable_strike()
                else:
                    strike = self.strike()
                option = OptionPlainEuropean(strike, ql_maturitydt, ql_optiontype)

            else:
                print('Unsupported Option Type !')
                option = None
            self.pricing_metrics = OptionMetrics(option, self.rf, self.engine_type).set_evaluation(self.evaluation)
        else:
            self.pricing_metrics.set_evaluation(self.evaluation)

    def contract_month(self):
        return self.current_state[Util.NAME_CONTRACT_MONTH]

    def strike(self):
        return self.current_state[Util.AMT_STRIKE]

    def adj_strike(self):
        return self.current_state[Util.AMT_ADJ_STRIKE]

    """ 应对分红调整，为metrics计算实际使用的行权价 """

    def applicable_strike(self):
        if self.current_state[Util.AMT_APPLICABLE_STRIKE] is None:
            print('use origin strike')
            return self.current_state[Util.AMT_STRIKE]
        else:
            return self.current_state[Util.AMT_APPLICABLE_STRIKE]

    def maturitydt(self):
        return self.current_state[Util.DT_MATURITY]

    def option_type(self):
        return self.current_daily_state[Util.CD_OPTION_TYPE]

    """ 如果close price为空，使用settlement price作为option price """

    def option_price(self):
        return self.current_state[Util.AMT_OPTION_PRICE]
        # try:
        #     settle = self.current_state[self.util.col_settlement]
        #     close = self.current_state[self.util.col_close]
        #     if close != self.util.nan_value:
        #         option_price = close
        #     elif settle != self.util.nan_value:
        #         option_price = settle
        #     else:
        #         print(self.id_instrument, ' : amt_close and amt_settlement are null!', )
        #         option_price = None
        # except Exception as e:
        #     print('Option close price and settlement price are both nan. ', e)
        #     option_price = None

    def adj_option_price(self):
        ret = self.current_state[Util.AMT_ADJ_OPTION_PRICE]
        if ret is not None and ret >= 0:
            return ret
        else:
            return None
        # try:
        #     adj_option_price = self.current_state[self.util.col_adj_option_price]
        #     if adj_option_price < 0: return None
        # except:
        #     adj_option_price = None
        # return adj_option_price

    def id_underlying(self):
        return self.current_state[Util.ID_UNDERLYING]

    def underlying_close(self):
        ret = self.current_state[Util.AMT_UNDERLYING_CLOSE]
        if ret is None or ret == Util.NAN_VALUE:
            return None
        return ret
        # try:
        #     underlying_price = self.current_state[self.util.col_underlying_close]
        #     if underlying_price == self.util.nan_value: return None
        # except:
        #     underlying_price = None
        # return underlying_price

    """ last bar/state, not necessarily daily"""

    def underlying_last_close(self):
        if self.current_index > 0:
            ret = self.df_data.loc[self.current_index - 1][Util.AMT_UNDERLYING_CLOSE]
        else:
            ret = self.current_state[Util.AMT_UNDERLYING_OPEN_PRICE]
        if ret is None or ret == Util.NAN_VALUE:
            return None
        return ret
        # try:
        #     if self.current_index > 0:
        #         last_state = self.df_metrics.loc[self.current_index - 1]
        #         underlying_last_price = last_state[self.util.col_underlying_close]
        #         if underlying_last_price == self.util.nan_value: return None
        #     else:
        #         """ if no previous date, use OPEN price """
        #         underlying_last_price = self.current_state[self.util.col_underlying_open_price]
        # except Exception as e:
        #     print(e)
        #     underlying_last_price = None
        # return underlying_last_price

    def underlying_open_price(self):
        ret = self.current_state[Util.AMT_UNDERLYING_OPEN_PRICE]
        if ret is None or ret == Util.NAN_VALUE:
            return None
        return ret
        # try:
        #     underlying_open_price = self.current_state[self.util.col_underlying_open_price]
        #     if underlying_open_price == self.util.nan_value: return None
        # except:
        #     underlying_open_price = None
        # return underlying_open_price

    def implied_vol_given(self):
        return self.current_state[Util.PCT_IMPLIED_VOL]

    def multiplier(self):
        return self.current_state[Util.NBR_MULTIPLIER]

    """ last settlement, daily"""
    def mktprice_last_settlement(self):
        ret = self.current_state[Util.AMT_LAST_SETTLEMENT]
        # TODO: Do we consider -999 case?
        if ret is None or np.isnan(ret):
        amt = None
        # tmp = pd.DataFrame(self.current_daily_state)
        # if self.util.col_last_settlement in self.current_daily_state.index.values:
        #     amt = self.current_daily_state.loc[self.util.col_last_settlement]
        # if amt == None or amt == np.nan:
        #     try:
        #         idx_date = self.dt_list.index(self.eval_date)
        #         if idx_date == 0:
        #             return
        #         dt_last = self.dt_list[self.dt_list.index(self.eval_date) - 1]
        #         df_last_state = self.df_daily_metrics.loc[dt_last]
        #         amt = df_last_state[self.util.col_settlement]
        #     except Exception as e:
        #         print(e)
        return amt

    # TODO: investigate logic here
    def update_implied_vol(self, spot=None, option_price=None):
        if spot == None:
            spot = self.underlying_close()
        if option_price == None:
            option_price = self.option_price()
        try:
            if self.flag_calculate_iv:
                self.update_pricing_metrics()
                implied_vol = self.pricing_metrics.implied_vol(spot, option_price)
            else:
                implied_vol = self.implied_vol_given() / 100.0
        except Exception as e:
            print(e)
            implied_vol = None
        self.implied_vol = implied_vol

    # TODO: investigate logic here
    def get_implied_vol(self):
        if self.implied_vol == None: self.update_implied_vol()
        return self.implied_vol

    # TODO: investigate logic here
    def get_delta(self, iv=None):
        self.update_pricing_metrics()
        try:
            if iv == None:
                if self.implied_vol == None: self.update_implied_vol()
                delta = self.pricing_metrics.delta(self.underlying_close(), self.implied_vol)
            else:
                delta = self.pricing_metrics.delta(self.underlying_close(), iv)
        except Exception as e:
            print(e)
            delta = None
        return delta

    # TODO: investigate logic here
    def get_theta(self):
        self.update_pricing_metrics()
        try:
            if self.implied_vol == None: self.update_implied_vol()
            theta = self.pricing_metrics.theta(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            theta = None
        return theta

    # TODO: investigate logic here
    def get_vega(self):
        self.update_pricing_metrics()
        if self.implied_vol == None: self.update_implied_vol()
        try:
            vega = self.pricing_metrics.vega(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            vega = None
        return vega

    # TODO: investigate logic here
    def get_rho(self):
        self.update_pricing_metrics()
        if self.implied_vol == None: self.update_implied_vol()
        try:
            rho = self.pricing_metrics.rho(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            rho = None
        return rho

    # TODO: investigate logic here
    def get_gamma(self):
        self.update_pricing_metrics()
        if self.implied_vol == None: self.update_implied_vol()
        try:
            gamma = self.pricing_metrics.gamma(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            gamma = None
        return gamma

    # TODO: investigate logic here
    def get_vomma(self):
        self.update_pricing_metrics()
        if self.implied_vol == None: self.update_implied_vol()
        try:
            vomma = self.pricing_metrics.vomma(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            vomma = None
        return vomma

    # TODO: investigate logic here
    def get_iv_roll_down(self, black_var_surface, dt):  # iv(tao-1)-iv(tao), tao:maturity
        if self.implied_vol == None: self.update_implied_vol()
        try:
            mdt = self.maturitydt()
            evalDate = self.eval_date
            ttm = (mdt - evalDate).days / 365.0
            black_var_surface.enableExtrapolation()
            implied_vol_t1 = black_var_surface.blackVol(ttm - dt, self.strike())
            iv_roll_down = implied_vol_t1 - self.implied_vol
        except Exception as e:
            print(e)
            iv_roll_down = 0.0
        return iv_roll_down

    # TODO: investigate logic here
    def get_carry(self, bvs, hp):
        ttm = (self.maturitydt() - self.eval_date).days
        # if ttm - hp <= 0: # 期限小于hp
        #     return None, None, None, None
        iv_roll_down = self.get_iv_roll_down(bvs, hp / 365.0)
        if np.isnan(iv_roll_down): iv_roll_down = 0.0
        vega = self.get_vega()
        theta = self.get_theta()
        try:
            option_carry = (vega * iv_roll_down - theta * ttm) / self.option_price() - self.rf
        except:
            option_carry = None
        return option_carry

    def get_init_margin(self):
        # if self.trade_long_short == self.util.long: return 0.0
        # 认购期权义务仓开仓保证金＝[合约前结算价+Max（12%×合约标的前收盘价-认购期权虚值，
        #                           7%×合约标的前收盘价)]×合约单位
        # 认沽期权义务仓开仓保证金＝Min[合约前结算价 + Max（12 %×合约标的前收盘价 - 认沽期权虚值，
        #                               7 %×行权价格），行权价格] ×合约单位
        amt_last_settle = self.mktprice_last_settlement()
        amt_underlying_last_close = self.underlying_last_close()
        if self.option_type() == self.util.type_call:
            otm = max(0, self.strike() - self.underlying_close())
            tmp = amt_last_settle + max(0.12 * amt_underlying_last_close - otm,
                                        0.07 * amt_underlying_last_close)
            init_margin = tmp * self.multiplier()
        else:
            otm = max(0, self.underlying_close() - self.strike())
            tmp = min(amt_last_settle + max(0.12 * amt_underlying_last_close - otm, 0.07 * self.strike()),
                      self.strike())
            init_margin = tmp * self.multiplier()
        return init_margin
    #
    # def get_maintain_margin(self):
    #     if self.trade_long_short == self.util.long: return 0.0
    #     if self.frequency in self.util.cd_frequency_low and self.trade_dt_open == self.eval_date:
    #         return self.get_init_margin()
    #     # 认购期权义务仓维持保证金＝[合约结算价 + Max（12 %×合约标的收盘价 - 认购期权虚值，
    #     #                                           7 %×合约标的收盘价）]×合约单位
    #     # 认沽期权义务仓维持保证金＝Min[合约结算价 + Max（12 %×合标的收盘价 - 认沽期权虚值，7 %×行权价格），
    #     #                               行权价格]×合约单位
    #     amt_settle = self.mktprice_settlement()
    #     if amt_settle == None or amt_settle == np.nan:
    #         amt_settle = self.mktprice_close()
    #     amt_underlying_close = self.underlying_close()
    #     if self.option_type() == self.util.type_call:
    #         otm = max(0, self.strike() - amt_underlying_close)
    #         maintain_margin = (amt_settle + max(0.12 * amt_underlying_close - otm,
    #                                             0.07 * amt_underlying_close)) * self.multiplier()
    #
    #     else:
    #         otm = max(0, amt_underlying_close - self.strike())
    #         maintain_margin = min(amt_settle +
    #                               max(0.12 * amt_underlying_close - otm, 0.07 * self.strike()),
    #                               self.strike()) * self.multiplier()
    #     return maintain_margin
    #
    # def price_limit(self):
    #     # 认购期权最大涨幅＝max｛合约标的前收盘价×0.5 %，min[（2×合约标的前收盘价－行权价格），合约标的前收盘价]×10％｝
    #     # 认购期权最大跌幅＝合约标的前收盘价×10％
    #     # 认沽期权最大涨幅＝max｛行权价格×0.5 %，min[（2×行权价格－合约标的前收盘价），合约标的前收盘价]×10％｝
    #     # 认沽期权最大跌幅＝合约标的前收盘价×10％
    #     return None
    #
    # def get_unit_by_mtmv(self, mtm_value):
    #     unit = np.floor(mtm_value / (self.option_price() * self.multiplier()))
    #     # unit = mtm_value/(self.option_price*self.multiplier)
    #     return unit
    #
    # def senario_calculate_option_price(self, underlying_price, vol):
    #     self.update_pricing_metrics()
    #     try:
    #         p = self.pricing_metrics.option_price(self.evaluation, self.rf, underlying_price,
    #                                               vol, self.engine_type)
    #     except:
    #         p = None
    #     return p
