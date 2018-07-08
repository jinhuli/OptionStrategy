from enum import Enum
import pandas as pd
import numpy as np
from typing import List
import datetime


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


class OptionUtil:
    MONEYNESS_POINT = 3.0

    @staticmethod
    def get_strike_by_monenyes_rank_nearest_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                                   option_type: OptionType) -> float:
        d = OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_nearest_strike(spot: float, strikes: List[float],
                                                     option_type: OptionType) -> float:
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
    def get_strike_monenyes_rank_dict_otm_strike(spot: float, strikes: List[float], option_type: OptionType) -> float:
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


class ETF:
    DIVIDEND_DATES = {
        datetime.date(2016, 11, 29): [
            '1612', '1701', '1703', '1706'
        ],
        datetime.date(2017, 11, 28): [
            '1712', '1801', '1803', '1806'
        ]

    }

    @staticmethod
    def fun_applicable_strikes(df: pd.Series) -> float:
        eval_date = df[Util.DT_DATE]
        contract_month = df[Util.NAME_CONTRACT_MONTH]
        dividend_dates = ETF.DIVIDEND_DATES
        dates = sorted(dividend_dates.keys(), reverse=False)
        if eval_date < dates[0]:
            return df[Util.AMT_ADJ_STRIKE]  # 分红除息日前反算调整前的行权价
        elif eval_date < dates[1]:
            if contract_month in dividend_dates[dates[1]]:
                return df[Util.AMT_ADJ_STRIKE]  # 分红除息日前反算调整前的行权价
            else:
                return df[Util.AMT_STRIKE]  # 分红除息日后用实际调整后的行权价
        else:
            return df[Util.AMT_STRIKE]  # 分红除息日后用实际调整后的行权价

    @staticmethod
    def fun_applicable_multiplier(df: pd.Series) -> float:
        eval_date = df[Util.DT_DATE]
        contract_month = df[Util.NAME_CONTRACT_MONTH]
        dividend_dates = ETF.DIVIDEND_DATES
        dates = sorted(dividend_dates.keys(), reverse=False)
        if eval_date < dates[0]:
            return 10000  # 分红除息日前
        elif eval_date < dates[1]:
            if contract_month in dividend_dates[dates[1]]:
                return 10000  # 分红除息日前
            else:
                return df[Util.NBR_MULTIPLIER]  # 分红除息日后用实际multiplier
        else:
            return df[Util.NBR_MULTIPLIER]  # 分红除息日后用实际multiplier


class OptionFilter:

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
    AMT_ADJ_STRIKE = 'amt_adj_strike'
    AMT_CLOSE = 'amt_close'
    AMT_OPEN = 'amt_open'
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
    TYPE_CALL = 'call'
    TYPE_PUT = 'put'
    NAN_VALUE = -999.0
    LONG = 1
    SHORT = -1
    LOW_FREQUENT = [FrequentType.DAILY, FrequentType.WEEKLY, FrequentType.MONTHLY, FrequentType.YEARLY]
    PRODUCT_COLUMN_LIST = [ID_INSTRUMENT, AMT_CLOSE, AMT_OPEN, AMT_SETTLEMENT, AMT_MORNING_OPEN_15MIN,
                           AMT_MORNING_CLOSE_15MIN, AMT_AFTERNOON_CLOSE_15MIN, AMT_MORNING_AVG, AMT_AFTERNOON_AVG,
                           AMT_DAILY_AVG, AMT_HOLDING_VOLUME, AMT_TRADING_VOLUME, AMT_LAST_SETTLEMENT,
                           AMT_LAST_CLOSE]
    INSTRUMENT_COLUMN_LIST = PRODUCT_COLUMN_LIST
    OPTION_COLUMN_LIST = PRODUCT_COLUMN_LIST + \
                         [NAME_CONTRACT_MONTH, AMT_STRIKE, AMT_ADJ_STRIKE, AMT_APPLICABLE_STRIKE, DT_MATURITY,
                          CD_OPTION_TYPE, AMT_OPTION_PRICE, AMT_ADJ_OPTION_PRICE, ID_UNDERLYING, AMT_UNDERLYING_CLOSE,
                          AMT_UNDERLYING_OPEN_PRICE, PCT_IMPLIED_VOL, NBR_MULTIPLIER, AMT_LAST_SETTLEMENT,
                          AMT_SETTLEMENT]
    FUTURE_BASED_OPTION_NAME_CODE = ['sr', 'm']
    FUTURE_BASED_OPTION_MAIN_CONTRACT = [1, 5, 9]

    # Trade
    UUID = 'uuid'
    DT_TRADE = 'dt_trade'
    TRADING_TYPE = 'trading_type'
    TRADING_PRICE = 'trade_price'
    TRADING_COST = 'trading_cost'
    UNIT = 'unit'
    PREMIIUM_PAID = 'premium paid'
    CASH = 'cash'
    MARGIN_CAPITAL = 'margin capital'

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
