from OptionStrategyLib.OptionPricing.Options import EuropeanOption
from OptionStrategyLib.Util import PricingUtil
import datetime
from pandas import DataFrame
import numpy as np
from back_test.model.constant import FrequentType, Util, PricingType, EngineType
from back_test.model.base_product import BaseProduct
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator


class BaseOption(BaseProduct):
    """ Contain metrics and trading position info as attributes """

    def __init__(self, df_data: DataFrame, df_daily_data: DataFrame = None,
                 frequency: FrequentType = FrequentType.DAILY, flag_calculate_iv: bool = False,
                 rf: float = 0.03, pricing_type=PricingType.OptionPlainEuropean,
                 engine_type=EngineType.AnalyticEuropeanEngine):
        self.flag_calculate_iv = flag_calculate_iv
        self.pricing_type = pricing_type
        self.engine_type = engine_type
        self.black_calculater: BlackCalculator = None
        self.implied_vol = None
        super().__init__(df_data, df_daily_data, rf, frequency)

    def next(self) -> None:
        self._destroy_black_calculater()
        super().next()
        # self.implied_vol = None
        # self.update_current_state()
        # self.update_current_daily_state()
        # self._destroy_black_calculater()

    def generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.OPTION_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if not columns.contains(column):
                print("{} missing column {}", self.__repr__(), column)
                self.df_data[column] = None

    def __repr__(self) -> str:
        return 'BaseOption(id_instrument: {0},eval_date: {1},current_index: {2},frequency: {3})' \
            .format(self.id_instrument(), self.eval_date, self.current_index, self.frequency)

    """ getters """

    def contract_month(self) -> str:
        return self.current_state[Util.NAME_CONTRACT_MONTH]

    def option_type(self) -> str:
        return self.current_daily_state[Util.CD_OPTION_TYPE]

    def id_underlying(self) -> str:
        return self.current_state[Util.ID_UNDERLYING]

    def maturitydt(self) -> datetime.date:
        return self.current_state[Util.DT_MATURITY]

    def strike(self) -> float:
        return self.current_state[Util.AMT_STRIKE]

    def adj_strike(self) -> float:
        return self.current_state[Util.AMT_ADJ_STRIKE]

    def applicable_strike(self) -> float:
        """ 应对分红调整，为metrics计算实际使用的行权价 """
        if self.current_state[Util.AMT_APPLICABLE_STRIKE] is None:
            print('use origin strike')
            return self.current_state[Util.AMT_STRIKE]
        else:
            return self.current_state[Util.AMT_APPLICABLE_STRIKE]

    def option_price(self) -> float:
        """ 如果close price为空，使用settlement price作为option price """
        return self.current_state[Util.AMT_OPTION_PRICE]

    def adj_option_price(self) -> float:
        ret = self.current_state[Util.AMT_ADJ_OPTION_PRICE]
        if ret is None or ret < 0 or np.isnan(ret):
            return None
        else:
            return ret

    def underlying_close(self) -> float:
        ret = self.current_state[Util.AMT_UNDERLYING_CLOSE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    """ last bar/state, not necessarily daily"""

    def underlying_last_close(self) -> float:
        if self.current_index > 0:
            ret = self.df_data.loc[self.current_index - 1][Util.AMT_UNDERLYING_CLOSE]
        else:
            """ if no previous date, use OPEN price """
            ret = self.current_state[Util.AMT_UNDERLYING_OPEN_PRICE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def underlying_open_price(self):
        ret = self.current_state[Util.AMT_UNDERLYING_OPEN_PRICE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return None
        return ret

    def implied_vol_given(self):
        return self.current_state[Util.PCT_IMPLIED_VOL]

    def multiplier(self):
        return self.current_state[Util.NBR_MULTIPLIER]

    """
    black calculator related calculations.
    """

    # TODO might write this in another class
    def _get_black_calculater(self, spot: float = None) -> BlackCalculator:
        if self.black_calculater is not None:
            return self.black_calculater
        if spot is None:
            spot = self.underlying_close()
        if self.name_code() == "50etf":
            strike = self.applicable_strike()
        else:
            strike = self.strike()
        pricing_util = PricingUtil()
        option = EuropeanOption(strike, self.maturitydt(), Util.TYPE_PUT)
        self.black_calculater = pricing_util.get_blackcalculator(self.eval_date, spot, option, self.rf, 0.0)
        return self.black_calculater

    def _destroy_black_calculater(self) -> None:
        self.implied_vol = None
        self.black_calculater = None

    def update_implied_vol(self, spot: float = None, option_price: float = None) -> None:
        if self.flag_calculate_iv:
            implied_vol = self._get_black_calculater(spot).implied_vol(option_price)
        else:
            implied_vol = self.implied_vol_given() / 100.0
        self.implied_vol = implied_vol

    def get_implied_vol(self):
        if self.implied_vol is None: self.update_implied_vol()
        return self.implied_vol

    def get_delta(self, iv=None):
        if iv is None:
            if self.implied_vol is None:
                self.update_implied_vol()
            return self._get_black_calculater().delta(self.underlying_close(), self.implied_vol)
        else:
            return self._get_black_calculater().delta(self.underlying_close(), iv)

    def get_theta(self):
        if self.implied_vol is None:
            self.update_implied_vol()
        return self._get_black_calculater().theta(self.underlying_close(), self.implied_vol)

    def get_vega(self):
        if self.implied_vol is None:
            self.update_implied_vol()
        return self._get_black_calculater().vega(self.underlying_close(), self.implied_vol)

    def get_rho(self):
        if self.implied_vol is None:
            self.update_implied_vol()
        return self._get_black_calculater().rho(self.underlying_close(), self.implied_vol)

    def get_gamma(self):
        if self.implied_vol is None:
            self.update_implied_vol()
        return self._get_black_calculater().gamma(self.underlying_close(), self.implied_vol)

    def get_vomma(self):
        if self.implied_vol is None:
            self.update_implied_vol()
        return self._get_black_calculater().vomma(self.underlying_close(), self.implied_vol)

    """
    iv(tao-1)-iv(tao), tao:maturity
    """

    # TODO: might need investigation of black_var_surfce
    def get_iv_roll_down(self, black_var_surface, dt):
        if self.implied_vol is None:
            self.update_implied_vol()
        try:
            ttm = (self.maturitydt() - self.eval_date).days / 365.0
            black_var_surface.enableExtrapolation()
            implied_vol_t1 = black_var_surface.blackVol(ttm - dt, self.strike())
            iv_roll_down = implied_vol_t1 - self.implied_vol
        except Exception as e:
            print(e)
            iv_roll_down = 0.0
        return iv_roll_down

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
        if self.option_type() == Util.TYPE_CALL:
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

    # TODO: optimization is needed
    def get_maintain_margin(self):
        if self.trade_long_short == Util.LONG: return 0.0
        if self.frequency in Util.LOW_FREQUENT and self.trade_dt_open == self.eval_date:
            return self.get_init_margin()
        # 认购期权义务仓维持保证金＝[合约结算价 + Max（12 %×合约标的收盘价 - 认购期权虚值，
        #                                           7 %×合约标的收盘价）]×合约单位
        # 认沽期权义务仓维持保证金＝Min[合约结算价 + Max（12 %×合标的收盘价 - 认沽期权虚值，7 %×行权价格），
        #                               行权价格]×合约单位
        amt_settle = self.mktprice_settlement()
        if amt_settle == None or amt_settle == np.nan:
            amt_settle = self.mktprice_close()
        amt_underlying_close = self.underlying_close()
        if self.option_type() == self.util.type_call:
            otm = max(0, self.strike() - amt_underlying_close)
            maintain_margin = (amt_settle + max(0.12 * amt_underlying_close - otm,
                                                0.07 * amt_underlying_close)) * self.multiplier()

        else:
            otm = max(0, amt_underlying_close - self.strike())
            maintain_margin = min(amt_settle +
                                  max(0.12 * amt_underlying_close - otm, 0.07 * self.strike()),
                                  self.strike()) * self.multiplier()
        return maintain_margin

    def price_limit(self):
        # 认购期权最大涨幅＝max｛合约标的前收盘价×0.5 %，min[（2×合约标的前收盘价－行权价格），合约标的前收盘价]×10％｝
        # 认购期权最大跌幅＝合约标的前收盘价×10％
        # 认沽期权最大涨幅＝max｛行权价格×0.5 %，min[（2×行权价格－合约标的前收盘价），合约标的前收盘价]×10％｝
        # 认沽期权最大跌幅＝合约标的前收盘价×10％
        return None

    def get_unit_by_mtmv(self, mtm_value):
        return np.floor(mtm_value / (self.option_price() * self.multiplier()))

    # TODO: need some work for this one
    def senario_calculate_option_price(self, underlying_price, vol):
        return None
        # self.update_pricing_metrics()
        # try:
        #     p = self.pricing_metrics.option_price(self.evaluation, self.rf, underlying_price,
        #                                           vol, self.engine_type)
        # except:
        #     p = None
        # return p
