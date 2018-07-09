import datetime
import pandas as pd
import numpy as np
from typing import Union
from back_test.model.constant import FrequentType, Util, PricingType, EngineType, Option50ETF, OptionFilter, OptionType
from back_test.model.base_product import BaseProduct
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator
from OptionStrategyLib.OptionPricing.BlackFormular import BlackFormula


class BaseOption(BaseProduct):
    """ Contain metrics and trading position info as attributes """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 frequency: FrequentType = FrequentType.DAILY,
                 flag_calculate_iv: bool = False, rf: float = 0.03):
        super().__init__(df_data, df_daily_data, rf, frequency)
        self.flag_calculate_iv = flag_calculate_iv
        self.black_calculater: BlackCalculator = None
        self.implied_vol: float = None

    def next(self) -> None:
        self._destroy_black_calculater()
        super().next()

    def __repr__(self) -> str:
        return 'BaseOption(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    def _validate_data(self):
        # If close price is nan value
        self.df_data[Util.AMT_OPTION_PRICE] = self.df_data.apply(OptionFilter.fun_option_price, axis=1)
        # For Dividend Adjusts
        self.df_data[Util.AMT_NEAREST_STRIKE] = self.df_data.apply(OptionFilter.nearest_strike_level, axis=1)
        self.df_data[Util.AMT_STRIKE_BEFORE_ADJ] = self.df_data.apply(OptionFilter.fun_strike_before_adj, axis=1)
        self.df_data[Util.AMT_APPLICABLE_STRIKE] = self.df_data.apply(OptionFilter.fun_applicable_strike, axis=1)

    def _generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.OPTION_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if column not in columns:
                self.df_data[column] = None

    """ European Option greeks """
    # TODO might write this in another class
    def _get_black_calculater(self) -> Union[BlackCalculator, None]:
        if self.black_calculater is not None:
            return self.black_calculater
        dt_maturity = self.maturitydt()
        strike = self.applicable_strike()
        option_type = self.option_type()
        vol = self.get_implied_vol()
        spot = self.underlying_close()
        self.black_calculater = BlackCalculator(self.eval_date, dt_maturity, strike, option_type, spot, vol, self.rf)
        return self.black_calculater

    def _destroy_black_calculater(self) -> None:
        self.implied_vol: float = None
        self.black_calculater: BlackCalculator = None

    """ getters """

    def contract_month(self) -> Union[str, None]:
        return self.current_state[Util.NAME_CONTRACT_MONTH]

    def option_type(self) -> Union[OptionType, None]:
        type_str = self.current_state[Util.CD_OPTION_TYPE]
        if type_str == Util.STR_CALL:
            option_type = OptionType.CALL
        elif type_str == Util.STR_PUT:
            option_type = OptionType.PUT
        else:
            return
        return option_type

    def id_underlying(self) -> str:
        return self.current_state[Util.ID_UNDERLYING]

    def maturitydt(self) -> datetime.date:
        return self.current_state[Util.DT_MATURITY]

    def strike(self) -> float:
        return self.current_state[Util.AMT_STRIKE]

    def nearest_strike(self) -> float:
        return self.current_state[Util.AMT_NEAREST_STRIKE]

    # """ 计算由于分红调整的合约，调整前的行权价 """
    def strike_before_adj(self) -> float:
        return self.current_state[Util.AMT_STRIKE_BEFORE_ADJ]

    # """ 用于计算的实际行权价序列，分红前为调整前的行权价，分红后即为调整后的行权价 """
    def applicable_strike(self) -> float:
        return self.current_state[Util.AMT_APPLICABLE_STRIKE]

    # """ 如果close price为空，使用settlement price作为option price """
    def option_price(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_OPTION_PRICE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def underlying_close(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_UNDERLYING_CLOSE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    # """ last bar/state, not necessarily daily"""
    def underlying_last_close(self) -> Union[float, None]:
        if self.current_index > 0:
            ret = self.df_data.loc[self.current_index - 1][Util.AMT_UNDERLYING_CLOSE]
        else:
            """ if no previous date, use OPEN price """
            ret = self.current_state[Util.AMT_UNDERLYING_OPEN_PRICE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def underlying_open_price(self) -> Union[float, None]:
        ret = self.current_state[Util.AMT_UNDERLYING_OPEN_PRICE]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def implied_vol_given(self) -> Union[float, None]:
        ret = self.current_state[Util.PCT_IMPLIED_VOL]
        if ret is None or ret == Util.NAN_VALUE or np.isnan(ret):
            return
        return ret

    def multiplier(self) -> Union[int, None]:
        return self.current_state[Util.NBR_MULTIPLIER]

    """
    black calculator related calculations.
    """

    def update_implied_vol(self) -> None:
        if self.flag_calculate_iv:
            dt_maturity = self.maturitydt()
            strike = self.applicable_strike()
            option_type = self.option_type()
            option_price = self.option_price()
            spot = self.underlying_close()
            black_formula = BlackFormula(self.eval_date, dt_maturity, strike, option_type, spot, option_price, self.rf)
            implied_vol = black_formula.ImpliedVolApproximation()
        else:
            implied_vol = self.implied_vol_given() / 100.0
        self.implied_vol = implied_vol

    def get_implied_vol(self) -> Union[float, None]:
        if self.implied_vol is None: self.update_implied_vol()
        return self.implied_vol

    def get_delta(self) -> Union[float, None]:
        return self._get_black_calculater().Delta()

    def get_theta(self) -> Union[float, None]:
        # TODO
        return

    def get_vega(self) -> Union[float, None]:
        # TODO
        return

    def get_rho(self) -> Union[float, None]:
        # TODO
        return

    def get_gamma(self) -> Union[float, None]:
        return self._get_black_calculater().Gamma()

    def get_vomma(self) -> Union[float, None]:
        # TODO
        return

    def get_iv_roll_down(self, black_var_surface, dt) -> Union[float, None]:
        # TODO
        return

    def get_carry(self, bvs, hp) -> Union[float, None]:
        # TODO
        return

    """ init_margin(初始保证金):用于开仓一天，且只有期权卖方收取 """

    # 认购期权义务仓开仓保证金＝[合约前结算价+Max（12%×合约标的前收盘价-认购期权虚值，
    #                           7%×合约标的前收盘价)]×合约单位
    # 认沽期权义务仓开仓保证金＝Min[合约前结算价 + Max（12 %×合约标的前收盘价 - 认沽期权虚值，
    #                               7 %×行权价格），行权价格] ×合约单位
    def get_init_margin(self) -> float:
        amt_last_settle = self.mktprice_last_settlement()
        amt_underlying_last_close = self.underlying_last_close()
        if self.option_type() == OptionType.CALL:
            otm = max(0.0, self.strike() - self.underlying_close())
            tmp = amt_last_settle + max(0.12 * amt_underlying_last_close - otm,
                                        0.07 * amt_underlying_last_close)
            init_margin = tmp * self.multiplier()
        else:
            otm = max(0.0, self.underlying_close() - self.strike())
            tmp = min(amt_last_settle + max(0.12 * amt_underlying_last_close - otm, 0.07 * self.strike()),
                      self.strike())
            init_margin = tmp * self.multiplier()
        return init_margin

    """ maintain_margin(维持保证金):用于非开仓一天，且只有期权卖方收取 """

    # 认购期权义务仓维持保证金＝[合约结算价 + Max（12 %×合约标的收盘价 - 认购期权虚值，
    #                                           7 %×合约标的收盘价）]×合约单位
    # 认沽期权义务仓维持保证金＝Min[合约结算价 + Max（12 %×合标的收盘价 - 认沽期权虚值，7 %×行权价格），
    #                               行权价格]×合约单位
    def get_maintain_margin(self):

        amt_settle = self.mktprice_settlement()
        if amt_settle is None or amt_settle == np.nan:
            amt_settle = self.mktprice_close()
        amt_underlying_close = self.underlying_close()
        if self.option_type() == OptionType.CALL:
            otm = max(0.0, self.strike() - amt_underlying_close)
            maintain_margin = (amt_settle + max(0.12 * amt_underlying_close - otm,
                                                0.07 * amt_underlying_close)) * self.multiplier()

        else:
            otm = max(0, amt_underlying_close - self.strike())
            maintain_margin = min(amt_settle +
                                  max(0.12 * amt_underlying_close - otm, 0.07 * self.strike()),
                                  self.strike()) * self.multiplier()
        return maintain_margin

    def is_valid_option(self) -> bool:
        if self.name_code() in Util.NAME_CODE_159:
            return int(self.id_underlying()[-2, :]) in Util.MAIN_CONTRACT_159
        return True

    """
    update multiplier adjustment.
    """
    #
    # # TODO: ask my queen for detail
    # def update_multiplier_adjustment(self):
    #     if self.name_code() == '50etf':
    #         self.df_data[Util.AMT_ADJ_STRIKE] = \
    #             round(self.df_data[Util.AMT_STRIKE] * self.df_data[Util.NBR_MULTIPLIER] / 10000.0, 2)
    #         self.df_data[Util.AMT_ADJ_OPTION_PRICE] = \
    #             round(self.df_data[Util.AMT_SETTLEMENT] * self.df_data[Util.NBR_MULTIPLIER] / 10000.0, 2)
    #     else:
    #         self.df_data[Util.AMT_ADJ_STRIKE] = self.df_data[Util.AMT_STRIKE]
    #         self.df_data[Util.AMT_ADJ_OPTION_PRICE] = self.df_data[Util.AMT_SETTLEMENT]
    #
    # # TODO: ask my queen for reason of doing nothing for non-50etf
    # def update_applicable_strikes(self):
    #     if self.name_code() == '50etf':
    #         self.df_data[Util.AMT_APPLICABLE_STRIKE] = self.df_data.apply(ETF.fun_applicable_strikes, axis=1)
    #         self.df_data[Util.AMT_APPLICABLE_MULTIPLIER] = self.df_data.apply(ETF.fun_applicable_multiplier, axis=1)

    """
    currently only validate option price
    """
