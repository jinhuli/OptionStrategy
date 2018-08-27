import datetime
import pandas as pd
import numpy as np
import math
from typing import Union
from back_test.model.constant import FrequentType, Util, OptionFilter, OptionType, \
    Option50ETF, ExecuteType, LongShort, OptionExerciseType, PricingUtil
from back_test.model.base_product import BaseProduct
from PricingLibrary.BlackCalculator import BlackCalculator
from PricingLibrary.BlackFormular import BlackFormula
from PricingLibrary.EngineQuantlib import QlBlackFormula, QlBinomial
from back_test.model.trade import Order


class BaseOption(BaseProduct):
    """ Contain metrics and trading position info as attributes """

    def __init__(self, df_data: pd.DataFrame, df_daily_data: pd.DataFrame = None,
                 frequency: FrequentType = FrequentType.DAILY,
                 flag_calculate_iv: bool = False, rf: float = 0.03):
        super().__init__(df_data, df_daily_data, rf, frequency)
        self.flag_calculate_iv = flag_calculate_iv
        # self.black_calculater: BlackCalculator = None
        self.implied_vol: float = None
        self.fee_rate = Util.DICT_OPTION_TRANSACTION_FEE_RATE[self.name_code()]
        self.fee_per_unit = Util.DICT_OPTION_TRANSACTION_FEE[self.name_code()]
        if self.name_code() in ['m', 'sr']:
            self.exercise_type = OptionExerciseType.AMERICAN
        else:
            self.exercise_type = OptionExerciseType.EUROPEAN
        self.pricing_engine = None

    def next(self) -> None:
        self._destroy_pricing_engine()
        super().next()

    def __repr__(self) -> str:
        return 'BaseOption(id_instrument: {0},eval_date: {1},frequency: {2})' \
            .format(self.id_instrument(), self.eval_date, self.frequency)

    def pre_process(self):
        # self.df_data[Util.AMT_OPTION_PRICE] = self.df_data.apply(OptionFilter.fun_option_price, axis=1)
        # For Dividend Adjusts
        self.df_data[Util.AMT_NEAREST_STRIKE] = self.df_data.apply(OptionFilter.nearest_strike_level, axis=1)
        if self.name_code() == Util.STR_50ETF:
            self.df_data[Util.AMT_STRIKE_BEFORE_ADJ] = self.df_data.apply(Option50ETF.fun_strike_before_adj, axis=1)
            self.df_data[Util.AMT_APPLICABLE_STRIKE] = self.df_data.apply(Option50ETF.fun_applicable_strike, axis=1)

    def _generate_required_columns_if_missing(self) -> None:
        required_column_list = Util.OPTION_COLUMN_LIST
        columns = self.df_data.columns
        for column in required_column_list:
            if column not in columns:
                self.df_data[column] = None

    def _set_pricing_engine(self):
        if self.pricing_engine is None:
            dt_maturity = self.maturitydt()
            strike = self.applicable_strike()
            option_type = self.option_type()
            spot = self.underlying_close()
            if self.exercise_type == OptionExerciseType.EUROPEAN:
                black_formula = QlBlackFormula(
                    dt_eval=self.eval_date,
                    dt_maturity=dt_maturity,
                    option_type=option_type,
                    spot=spot,
                    strike=strike,
                    rf=self.rf
                )
                self.pricing_engine = black_formula
            else:
                binomial = QlBinomial(
                    dt_eval=self.eval_date,
                    dt_maturity=dt_maturity,
                    option_type=option_type,
                    option_exercise_type=OptionExerciseType.AMERICAN,
                    spot=spot,
                    strike=strike,
                    rf=self.rf,
                    n=800
                )
                self.pricing_engine = binomial

    def _get_pricing_engine(self, spot):
        dt_maturity = self.maturitydt()
        strike = self.applicable_strike()
        option_type = self.option_type()
        if self.exercise_type == OptionExerciseType.EUROPEAN:
            black_formula = QlBlackFormula(
                dt_eval=self.eval_date,
                dt_maturity=dt_maturity,
                option_type=option_type,
                spot=spot,
                strike=strike,
                rf=self.rf
            )
            return black_formula
        else:
            binomial = QlBinomial(
                dt_eval=self.eval_date,
                dt_maturity=dt_maturity,
                option_type=option_type,
                option_exercise_type=OptionExerciseType.AMERICAN,
                spot=spot,
                strike=strike,
                rf=self.rf,
                n=800
            )
            return binomial

    # def _get_black_calculater(self) -> Union[BlackCalculator, None]:
    #     if self.black_calculater is not None:
    #         return self.black_calculater
    #     dt_maturity = self.maturitydt()
    #     strike = self.applicable_strike()
    #     option_type = self.option_type()
    #     vol = self.get_implied_vol()
    #     spot = self.underlying_close()
    #     self.black_calculater = BlackCalculator(self.eval_date, dt_maturity, strike, option_type, spot, vol, self.rf)
    #     return self.black_calculater

    def _destroy_pricing_engine(self) -> None:
        self.implied_vol: float = None
        # self.black_calculater: BlackCalculator = None
        self.pricing_engine = None

    """ getters """

    def contract_month(self) -> Union[str, None]:
        return self.current_state[Util.NAME_CONTRACT_MONTH]

    def option_type(self) -> Union[OptionType, None]:
        type = self.current_state[Util.CD_OPTION_TYPE]
        if type == Util.STR_CALL:
            option_type = OptionType.CALL
        elif type == Util.STR_PUT:
            option_type = OptionType.PUT
        else:
            return type
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
    def mktprice_close(self) -> Union[float, None]:
        if self.current_state[Util.AMT_CLOSE] != Util.NAN_VALUE:
            option_price = self.current_state[Util.AMT_CLOSE]
        elif self.current_state[Util.AMT_SETTLEMENT] != Util.NAN_VALUE:
            option_price = self.current_state[Util.AMT_SETTLEMENT]
        else:
            print('amt_close and amt_settlement are null!')
            print(self.current_state)
            option_price = None
        return option_price

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
        if self._name_code == Util.STR_50ETF:
            return self.current_state[Util.NBR_MULTIPLIER]
        else:
            return Util.DICT_CONTRACT_MULTIPLIER[self._name_code]

    """
    black calculator related calculations.
    """

    def update_implied_vol(self) -> None:
        if self.flag_calculate_iv:
            option_price = self.mktprice_close()
            self._set_pricing_engine()
            implied_vol = self.pricing_engine.estimate_vol(option_price)
            # dt_maturity = self.maturitydt()
            # strike = self.applicable_strike()
            # option_type = self.option_type()
            # spot = self.underlying_close()
            # # black_formula = BlackFormula(self.eval_date, dt_maturity, option_type, spot, strike,option_price, self.rf)
            # # implied_vol = black_formula.ImpliedVolApproximation()
            # if self.exercise_type == OptionExerciseType.EUROPEAN:
            #     black_formula = QlBlackFormula(
            #         dt_eval=self.eval_date,
            #         dt_maturity=dt_maturity,
            #         option_type=option_type,
            #         spot=spot,
            #         strike=strike,
            #         rf=self.rf
            #     )
            #     implied_vol = black_formula.estimate_vol(option_price)
            # else:
            #     binomial = QlBinomial(
            #         dt_eval=self.eval_date,
            #         dt_maturity=dt_maturity,
            #         option_type=option_type,
            #         option_exercise_type=OptionExerciseType.AMERICAN,
            #         spot=spot,
            #         strike=strike,
            #         rf=self.rf,
            #         n=800
            #     )
            #     implied_vol = binomial.estimate_vol(option_price)
        else:
            implied_vol = self.implied_vol_given() / 100.0
        self.implied_vol = implied_vol

    def get_implied_vol(self) -> Union[float, None]:
        if self.implied_vol is None: self.update_implied_vol()
        return self.implied_vol

    def get_implied_vol_adjusted_by_htbr(self, htb_r) -> float:
        ttm = PricingUtil.get_ttm(self.eval_date, self.maturitydt())
        spot_htb = self.underlying_close() * math.exp(-htb_r * ttm)
        pricing_engine = self._get_pricing_engine(spot_htb)
        implied_vol = pricing_engine.estimate_vol(self.mktprice_close())
        # if self.exercise_type == OptionExerciseType.EUROPEAN:
        #     black_formula = QlBlackFormula(
        #         dt_eval=self.eval_date,
        #         dt_maturity=self.maturitydt(),
        #         option_type=self.option_type(),
        #         spot=spot_htb,
        #         strike=self.applicable_strike(),
        #         rf=self.rf
        #     )
        #     implied_vol = black_formula.estimate_vol(self.mktprice_close())
        # else:
        #     binomial = QlBinomial(
        #         dt_eval=self.eval_date,
        #         dt_maturity=self.maturitydt(),
        #         option_type=self.option_type(),
        #         option_exercise_type=OptionExerciseType.AMERICAN,
        #         spot=spot_htb,
        #         strike=self.strike(),
        #         rf=self.rf
        #     )
        #     implied_vol = binomial.estimate_vol(self.mktprice_close())
        return implied_vol

    def get_delta(self, implied_vol: float) -> Union[float, None]:
        # if htb_r is None:
        #     spot = self.underlying_close()
        # else:
        #     ttm = PricingUtil.get_ttm(self.eval_date, self.maturitydt())
        #     spot = self.underlying_close() * math.exp(-htb_r * ttm)
        self._set_pricing_engine()
        delta = self.pricing_engine.Delta(implied_vol)
        return delta

    def get_theta(self) -> Union[float, None]:
        # TODO
        return

    def get_vega(self) -> Union[float, None]:
        # TODO
        return

    def get_rho(self) -> Union[float, None]:
        # TODO
        return

    def get_gamma(self, implied_vol: float, htb_r=None) -> Union[float, None]:
        # if htb_r is None:
        #     spot = self.underlying_close()
        # else:
        #     ttm = PricingUtil.get_ttm(self.eval_date, self.maturitydt())
        #     spot = self.underlying_close() * math.exp(-htb_r * ttm)
        self._set_pricing_engine()
        gamma = self.pricing_engine.Gamma(implied_vol)
        return gamma

    def get_vomma(self) -> Union[float, None]:
        # TODO
        return

    def get_iv_roll_down(self, black_var_surface, dt) -> Union[float, None]:
        # TODO
        return

    def get_carry(self, bvs, hp) -> Union[float, None]:
        # TODO
        return

    """ 用于计算杠杆率 ：option，买方具有current value为当前的权利金，期权卖方为保证金交易，current value为零 """

    def get_current_value(self, long_short):
        if long_short == LongShort.LONG:
            return self.mktprice_close()
        elif long_short == LongShort.SHORT:
            return 0.0
        else:
            return

    def is_margin_trade(self, long_short):
        if long_short == LongShort.LONG:
            return False
        elif long_short == LongShort.SHORT:
            return True
        else:
            return

    def is_mtm(self):
        return False

    """ init_margin(初始保证金):用于开仓一天，且只有期权卖方收取 """

    # 认购期权义务仓开仓保证金＝[合约前结算价+Max（12%×合约标的前收盘价-认购期权虚值，
    #                           7%×合约标的前收盘价)]×合约单位
    # 认沽期权义务仓开仓保证金＝Min[合约前结算价 + Max（12 %×合约标的前收盘价 - 认沽期权虚值，
    #                               7 %×行权价格），行权价格] ×合约单位
    def get_initial_margin(self, long_short: LongShort) -> float:
        if long_short == LongShort.LONG: return 0.0
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
    def get_maintain_margin(self, long_short: LongShort):
        if long_short == LongShort.LONG:
            return 0.0
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
            return int(self.id_underlying()[-2:]) in Util.MAIN_CONTRACT_159
        return True

    def execute_order(self, order: Order, slippage=0, execute_type: ExecuteType = ExecuteType.EXECUTE_ALL_UNITS):
        if order is None: return
        if execute_type == ExecuteType.EXECUTE_ALL_UNITS:
            order.trade_all_unit(slippage)
        elif execute_type == ExecuteType.EXECUTE_WITH_MAX_VOLUME:
            order.trade_with_current_volume(int(self.trading_volume()), slippage)
        else:
            return
        execution_record: pd.Series = order.execution_res
        if order.long_short == LongShort.LONG:
            # 无保证金交易的情况下，trade_market_value有待从现金账户中全部扣除。
            execution_record[Util.TRADE_MARGIN_CAPITAL] = 0.0
            execution_record[Util.TRADE_MARKET_VALUE] = execution_record[Util.TRADE_UNIT] * \
                                                        execution_record[Util.TRADE_PRICE] * self.multiplier()
        else:
            execution_record[Util.TRADE_MARGIN_CAPITAL] = self.get_initial_margin(order.long_short) * \
                                                          execution_record[Util.TRADE_UNIT]
            execution_record[Util.TRADE_MARKET_VALUE] = 0.0
        if self.fee_per_unit is None:
            # 百分比手续费
            transaction_fee = execution_record[Util.TRADE_PRICE] * self.fee_rate * execution_record[
                Util.TRADE_UNIT] * self.multiplier()
        else:
            # 每手手续费
            transaction_fee = self.fee_per_unit * execution_record[Util.TRADE_UNIT]
        execution_record[Util.TRANSACTION_COST] += transaction_fee
        transaction_fee_add_to_price = transaction_fee / (execution_record[Util.TRADE_UNIT] *
                                                          self.multiplier())
        execution_record[Util.TRADE_PRICE] += execution_record[Util.TRADE_LONG_SHORT].value \
                                              * transaction_fee_add_to_price
        position_size = order.long_short.value * execution_record[Util.TRADE_PRICE] * \
                        execution_record[Util.TRADE_UNIT] * self.multiplier()
        execution_record[
            Util.TRADE_BOOK_VALUE] = position_size  # 头寸规模（含多空符号），例如，空一手豆粕（3000点，乘数10）得到头寸规模为-30000，而建仓时点头寸市值为0。
        return execution_record
