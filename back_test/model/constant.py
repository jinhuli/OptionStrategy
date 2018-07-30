from enum import Enum
import pandas as pd
import numpy as np
from typing import List, Union
import datetime
import math


class FrequentType(Enum):
    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    YEARLY = 4
    MINUTE = 5


class PricingType(Enum):
    OptionPlainEuropean = 1


class EngineType(Enum):
    AnalyticEuropeanEngine = 1


class LongShort(Enum):
    LONG = 1
    SHORT = -1


class MoneynessMethod(Enum):
    METHOD1 = 0
    METHOD2 = 1


class UnderlyingPriceType(Enum):
    CLOSE = 1
    OPEN = 2


class OptionType(Enum):
    CALL = 1
    PUT = -1

class OptionExerciseType(Enum):
    AMERICAN = 0
    EUROPEAN = 1

class DeltaBound(Enum):
    WHALLEY_WILLMOTT = 0
    NONE = -1


class TradeType(Enum):
    OPEN_LONG = 1
    OPEN_SHORT = 2
    CLOSE_LONG = -1
    CLOSE_SHORT = -2
    CLOSE_OUT = -3

class ExecuteType(Enum):
    EXECUTE_ALL_UNITS = 0
    EXECUTE_WITH_MAX_VOLUME = 1

class OptionUtil:
    MONEYNESS_POINT = 3.0

    @staticmethod
    def get_strike_by_monenyes_rank_nearest_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                                   option_type: OptionType) -> float:
        d = OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_nearest_strike(spot: float, strikes: List[float],
                                                     option_type: OptionType) -> dict:
        d = {}
        for strike in strikes:
            if strike <= OptionUtil.MONEYNESS_POINT:
                if spot <= OptionUtil.MONEYNESS_POINT:
                    # strike = 2.9, spot=2.8, moneyness = (2.8-2.9)/0.05
                    rank = int(option_type.value * round((spot - strike) / 0.05))
                else:
                    # strike = 2.9, spot = 3.1, moneyness = (3.0 - 2.9)/0.05 + (3.1 - 3.0)/0.1
                    rank = int(option_type.value * round((OptionUtil.MONEYNESS_POINT - strike) / 0.05
                                                         + (spot - OptionUtil.MONEYNESS_POINT) / 0.1))
            else:
                if spot <= OptionUtil.MONEYNESS_POINT:
                    # strike = 3.1, spot = 2.9, moneyness = (3.0 - 3.1)/0.1+(2.9 - 3.0)/0.05
                    rank = int(option_type.value * round((OptionUtil.MONEYNESS_POINT - strike) / 0.1
                                                         + (spot - OptionUtil.MONEYNESS_POINT) / 0.05))
                else:
                    # strike = 3.1, spot = 3.1, moneyness = (3.1-3.1)/0.1
                    rank = int(option_type.value * round((spot - strike) / 0.1))
            d.update({rank: strike})
        return d

    @staticmethod
    def get_strike_by_monenyes_rank_otm_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                               option_type: OptionType) -> float:
        d = OptionUtil.get_strike_monenyes_rank_dict_otm_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_otm_strike(spot: float, strikes: List[float], option_type: OptionType) -> dict:
        d = {}
        for strike in strikes:
            if strike <= OptionUtil.MONEYNESS_POINT:
                if spot <= OptionUtil.MONEYNESS_POINT:
                    # strike = 2.9, spot=2.8, moneyness = (2.8-2.9)/0.05
                    rank = int(np.floor(option_type.value * (spot - strike) / 0.05) + 1)
                else:
                    # strike = 2.9, spot = 3.1, moneyness = (3.0 - 2.9)/0.05 + (3.1 - 3.0)/0.1
                    rank = int(np.floor(option_type.value * ((OptionUtil.MONEYNESS_POINT - strike) / 0.05
                                                             + (spot - OptionUtil.MONEYNESS_POINT) / 0.1)) + 1)
            else:
                if spot <= OptionUtil.MONEYNESS_POINT:
                    # strike = 3.1, spot = 2.9, moneyness = (3.0 - 3.1)/0.1+(2.9 - 3.0)/0.05
                    rank = int(np.floor(option_type.value * ((OptionUtil.MONEYNESS_POINT - strike) / 0.1
                                                             + (spot - OptionUtil.MONEYNESS_POINT) / 0.05)) + 1)
                else:
                    # strike = 3.1, spot = 3.1, moneyness = (3.1-3.1)/0.1
                    rank = int(np.floor(option_type.value * (spot - strike) / 0.1) + 1)
            d.update({rank: strike})
        return d


class Option50ETF:
    DIVIDEND_DATES = {
        datetime.date(2016, 11, 29): [
            '1612', '1701', '1703', '1706'
        ],
        datetime.date(2017, 11, 28): [
            '1712', '1801', '1803', '1806'
        ]

    }

    def fun_strike_before_adj(df: pd.Series) -> float:
        return round(df[Util.AMT_STRIKE] * df[Util.NBR_MULTIPLIER] / 10000, 2)

    @staticmethod
    def fun_applicable_strike(df: pd.Series) -> float:
        eval_date = df[Util.DT_DATE]
        contract_month = df[Util.NAME_CONTRACT_MONTH]
        dividend_dates = Option50ETF.DIVIDEND_DATES
        dates = sorted(dividend_dates.keys(), reverse=False)
        if eval_date < dates[0]:
            return df[Util.AMT_STRIKE_BEFORE_ADJ]  # 分红除息日前反算调整前的行权价
        elif eval_date < dates[1]:
            if contract_month in dividend_dates[dates[1]]:
                return df[Util.AMT_STRIKE_BEFORE_ADJ]  # 分红除息日前反算调整前的行权价
            else:
                return df[Util.AMT_STRIKE]  # 分红除息日后用实际调整后的行权价
        else:
            return df[Util.AMT_STRIKE]  # 分红除息日后用实际调整后的行权价

            # @staticmethod
            # def fun_applicable_multiplier(df: pd.Series) -> float:
            #     eval_date = df[Util.DT_DATE]
            #     contract_month = df[Util.NAME_CONTRACT_MONTH]
            #     dividend_dates = ETF.DIVIDEND_DATES
            #     dates = sorted(dividend_dates.keys(), reverse=False)
            #     if eval_date < dates[0]:
            #         return 10000  # 分红除息日前
            #     elif eval_date < dates[1]:
            #         if contract_month in dividend_dates[dates[1]]:
            #             return 10000  # 分红除息日前
            #         else:
            #             return df[Util.NBR_MULTIPLIER]  # 分红除息日后用实际multiplier
            #     else:
            #         return df[Util.NBR_MULTIPLIER]  # 分红除息日后用实际multiplier


class OptionFilter:
    @staticmethod
    def fun_option_type_split(self, id_instrument) -> Union[OptionType, None]:
        type_str = id_instrument.split('_')[2]
        if type_str == 'c':
            option_type = OptionType.CALL
        elif type_str == 'p':
            option_type = OptionType.PUT
        else:
            return
        return option_type

    @staticmethod
    def fun_option_price(df: pd.Series) -> float:
        if df[Util.AMT_CLOSE] != Util.NAN_VALUE:
            option_price = df[Util.AMT_CLOSE]
        elif df[Util.AMT_SETTLEMENT] != Util.NAN_VALUE:
            option_price = df[Util.NAN_VALUE]
        else:
            print('amt_close and amt_settlement are null!')
            print(df)
            option_price = None
        return option_price

    @staticmethod
    def nearest_strike_level(s: pd.Series) -> float:
        strike = s[Util.AMT_STRIKE]
        if strike <= 3:
            return round(round(strike / 0.05) * 0.05, 2)
        else:
            return round(round(strike / 0.1) * 0.1, 2)

    # @staticmethod
    # def fun_strike_before_adj(df: pd.Series) -> float:
    #     if df[Util.NAME_CODE] == Util.STR_50ETF:
    #         return Option50ETF.fun_strike_before_adj(df)
    #     else:
    #         return df[Util.AMT_STRIKE]
    #
    # @staticmethod
    # def fun_applicable_strike(df: pd.Series) -> float:
    #     if df[Util.NAME_CODE] == Util.STR_50ETF:
    #         return Option50ETF.fun_applicable_strike(df)
    #     else:
    #         return df[Util.AMT_STRIKE]


class FutureUtil:

    @staticmethod
    def get_contract_shift_cost(c1, c2, long_short: LongShort):
        return


class Hedge:

    @staticmethod
    def whalley_wilmott(ttm, gamma, spot, rho=1, fee=5.0 / 10000.0, rf=0.03):
        # ttm = self.pricing_utl.get_ttm(eval_date, option.dt_maturity)
        H = (1.5 * math.exp(-rf * ttm) * fee * spot * (gamma ** 2) / rho) ** (1 / 3)
        return H


class Util:
    """database column names"""
    # basic
    DT_DATE = 'dt_date'
    DT_DATETIME = 'dt_datetime'
    CODE_INSTRUMENT = 'code_instrument'
    ID_INSTRUMENT = 'id_instrument'

    # option
    DT_MATURITY = 'dt_maturity'
    ID_UNDERLYING = 'id_underlying'
    CD_OPTION_TYPE = 'cd_option_type'
    NAME_CONTRACT_MONTH = 'name_contract_month'
    AMT_STRIKE = 'amt_strike'
    AMT_STRIKE_BEFORE_ADJ = 'amt_strike_before_adj'
    AMT_CLOSE = 'amt_close'
    AMT_OPEN = 'amt_open'
    AMT_HIGH = 'amt_high'
    AMT_LOW = 'amt_low'
    AMT_ADJ_OPTION_PRICE = 'amt_adj_option_price'
    AMT_OPTION_PRICE = 'amt_option_price'
    AMT_UNDERLYING_CLOSE = 'amt_underlying_close'
    AMT_UNDERLYING_OPEN_PRICE = 'amt_underlying_open_price'
    AMT_SETTLEMENT = 'amt_settlement'
    AMT_LAST_SETTLEMENT = 'amt_last_settlement'
    AMT_LAST_CLOSE = 'amt_last_close'
    NBR_MULTIPLIER = 'nbr_multiplier'
    AMT_HOLDING_VOLUME = 'amt_holding_volume'
    AMT_TRADING_VOLUME = 'amt_trading_volume'
    AMT_TRADING_VALUE = 'amt_trading_value'
    AMT_MORNING_OPEN_15MIN = 'amt_morning_open_15min'
    AMT_MORNING_CLOSE_15MIN = 'amt_morning_close_15min'
    AMT_AFTERNOON_OPEN_15MIN = 'amt_afternoon_open_15min'
    AMT_AFTERNOON_CLOSE_15MIN = 'amt_afternoon_close_15min'
    AMT_MORNING_AVG = 'amt_morning_avg'
    AMT_AFTERNOON_AVG = 'amt_afternoon_avg'
    AMT_DAILY_AVG = 'amt_daily_avg'
    AMT_NEAREST_STRIKE = 'amt_nearest_strike'
    PCT_IMPLIED_VOL = 'pct_implied_vol'
    AMT_DELTA = 'amt_delta'
    AMT_THETA = 'amt_theta'
    AMT_VEGA = 'amt_vega'
    AMT_RHO = 'amt_rho'
    AMT_CARRY = 'amt_carry'
    AMT_IV_ROLL_DOWN = 'amt_iv_roll_down'
    NBR_INVEST_DAYS = 'nbr_invest_days'
    RISK_FREE_RATE = 'risk_free_rate'
    AMT_APPLICABLE_STRIKE = 'amt_applicable_strike'
    AMT_APPLICABLE_MULTIPLIER = 'amt_applicable_multiplier'
    AMT_HISTVOL = 'amt_hist_vol'
    AMT_PARKINSON_NUMBER = 'amt_parkinson_number'
    AMT_GARMAN_KLASS = 'amt_garman_klass'
    AMT_HEDHE_UNIT = 'amt_hedge_unit'
    NAME_CODE = 'name_code'
    STR_CALL = 'call'
    STR_PUT = 'put'
    STR_50ETF = '50etf'
    STR_INDEX_50ETF = 'index_50etf'
    STR_ALL = 'all'
    NAN_VALUE = -999.0

    STR_SR = 'sr'
    STR_m = 'm'
    LOW_FREQUENT = [FrequentType.DAILY, FrequentType.WEEKLY, FrequentType.MONTHLY, FrequentType.YEARLY]
    PRODUCT_COLUMN_LIST = [ID_INSTRUMENT, AMT_CLOSE, AMT_OPEN, AMT_SETTLEMENT, AMT_MORNING_OPEN_15MIN,
                           AMT_MORNING_CLOSE_15MIN, AMT_AFTERNOON_CLOSE_15MIN, AMT_MORNING_AVG, AMT_AFTERNOON_AVG,
                           AMT_DAILY_AVG, AMT_HOLDING_VOLUME, AMT_TRADING_VOLUME, AMT_LAST_SETTLEMENT,
                           AMT_LAST_CLOSE]
    INSTRUMENT_COLUMN_LIST = PRODUCT_COLUMN_LIST
    OPTION_COLUMN_LIST = PRODUCT_COLUMN_LIST + \
                         [NAME_CONTRACT_MONTH, AMT_STRIKE, AMT_STRIKE_BEFORE_ADJ, AMT_APPLICABLE_STRIKE, DT_MATURITY,
                          CD_OPTION_TYPE, AMT_OPTION_PRICE, AMT_ADJ_OPTION_PRICE, ID_UNDERLYING, AMT_UNDERLYING_CLOSE,
                          AMT_UNDERLYING_OPEN_PRICE, PCT_IMPLIED_VOL, NBR_MULTIPLIER, AMT_LAST_SETTLEMENT,
                          AMT_SETTLEMENT]
    NAME_CODE_159 = ['sr', 'm', 'ru']
    MAIN_CONTRACT_159 = [1, 5, 9]
    NAME_CODE_1to12 = ['cu']
    # Trade
    LONG = 1
    SHORT = -1
    UUID = 'uuid'
    DT_TRADE = 'dt_trade'
    TRADE_TYPE = 'trade_type'
    TRADE_PRICE = 'trade_price'
    TRANSACTION_COST = 'transaction_cost'
    TRADE_UNIT = 'trade_unit'  # 绝对值
    TIME_SIGNAL = 'time_signal'
    OPTION_PREMIIUM = 'option_premium'
    CASH = 'cash'
    TRADE_MARGIN_CAPITAL = 'trade_margin_capital'
    TRADE_MARKET_VALUE = 'trade_market_value'  # 头寸市值
    TRADE_BOOK_VALUE = 'trade_book_value'  # 头寸规模（含多空符号），例如，空一手豆粕（3000点，乘数10）得到头寸规模为-30000，而建仓时点头寸市值为0。
    TRADE_LONG_SHORT = 'long_short'
    AVERAGE_POSITION_COST = 'average_position_cost'  # 历史多次交易同一品种的平均成本(总头寸规模绝对值/unit)
    TRADE_REALIZED_PNL = 'realized_pnl'
    LAST_PRICE = 'last_price'
    POSITION_CURRENT_VALUE = 'position_current_value'  # 用于计算杠杆率，保证金交易的current value为零
    PORTFOLIO_MARGIN_CAPITAL = 'portfolio_margin_capital'
    PORTFOLIO_MARGIN_TRADE_SCALE = 'portfolio_margin_trade_scale'
    PORTFOLIO_TOTAL_SCALE = 'portfolio_total_scale'
    PORTFOLIO_TRADES_VALUE = 'portfolio_trades_value'
    PORTFOLIO_VALUE = 'portfolio_value'
    PORTFOLIO_NPV = 'npv'
    PORTFOLIO_UNREALIZED_PNL = 'unrealized_pnl'
    PORTFOLIO_LEVERAGE = 'portfolio_leverage'
    PORTFOLIO_SHORT_POSITION_SCALE = 'portfolio_short_position_scale'
    PORTFOLIO_LONG_POSITION_SCALE = 'portfolio_long_position_scale'
    BILLION = 1000000000.0
    TRADE_BOOK_COLUMN_LIST = [DT_DATE,TRADE_LONG_SHORT, TRADE_UNIT,
                              LAST_PRICE, TRADE_MARGIN_CAPITAL,
                              TRADE_BOOK_VALUE, AVERAGE_POSITION_COST,
                              TRADE_REALIZED_PNL, NBR_MULTIPLIER,
                              POSITION_CURRENT_VALUE,PORTFOLIO_UNREALIZED_PNL
                              ]  # ID_INSTRUMENR是df的index
    ACCOUNT_COLUMNS = [DT_DATE, CASH, PORTFOLIO_MARGIN_CAPITAL, PORTFOLIO_TRADES_VALUE,
                        PORTFOLIO_VALUE, PORTFOLIO_NPV, PORTFOLIO_UNREALIZED_PNL,
                       PORTFOLIO_LEVERAGE, TRADE_REALIZED_PNL,
                       PORTFOLIO_SHORT_POSITION_SCALE,PORTFOLIO_LONG_POSITION_SCALE
                       ]
    DICT_FUTURE_MARGIN_RATE = {  # 合约价值的百分比
        'm': 0.05,
        'if': 0.15,
        'ih': 0.15,
        'ic': 0.15,
    }
    DICT_TRANSACTION_FEE = {  # 元/手
        'm': 3.0,
        'if': None,
        'ih': None,
        'ic': None,
    }
    DICT_OPTION_TRANSACTION_FEE_RATE = {  # 百分比
        "50etf": 0.0,
        "m": 0.0,
        "sr": 0.0,
    }
    DICT_OPTION_TRANSACTION_FEE = {  # 元/手
        "50etf": 0.0,
        "m": 0.0,
        "sr": 0.0,
    }
    DICT_TRANSACTION_FEE_RATE = {  # 百分比
        'if': 6.9 / 10000.0,
        'ih': 6.9 / 10000.0,
        'ic': 6.9 / 10000.0,
    }
    DICT_CONTRACT_MULTIPLIER = {  # 合约乘数
        'm': 10,
        'if': 300,
        'ih': 300,
        'ic': 200,
    }
    DICT_OPTION_CONTRACT_MULTIPLIER = {  # 合约乘数
        'm': 10,
        'sr': 10,
        STR_50ETF: 10000
    }
    DICT_FUTURE_CORE_CONTRACT = {
        'm': [1, 5, 9],
        'sr': [1, 5, 6],
        STR_50ETF: STR_ALL}

    DICT_TICK_SIZE = {
        "50etf": 0.0001,
        "m": 1,
        "sr": 0.5,
        'if': 0.2,
        'ih': 0.2,
        'ic': 0.2
    }

    DZQH_CF_DATA_MISSING_DATES = [datetime.date(2017,12,28),datetime.date(2017,12,29),datetime.date(2018,1,26),datetime.date(2018,5,4)]

    @staticmethod
    def filter_invalid_data(x: pd.Series) -> bool:
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
