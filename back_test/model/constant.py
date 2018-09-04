from enum import Enum
import pandas as pd
import numpy as np
from typing import List, Union
import math
import datetime
import typing


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


class BuyWrite(Enum):
    BUY = 1
    WRITE = -1


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
    @staticmethod
    def get_option_util_class(name_code):
        if name_code == Util.STR_50ETF:
            return Option50ETF
        elif name_code == Util.STR_M:
            return OptionM
        elif name_code == Util.STR_SR:
            return OptionSR
        else:
            return None

    @staticmethod
    def get_df_by_mdt(df, mdt):
        df = df[df[Util.DT_MATURITY] == mdt].reset_index(drop=True)
        return df


class OptionM:
    MONEYNESS_POINT_LOW = 2000
    MONEYNESS_POINT_HIGH = 5000

    @staticmethod
    def get_moneyness_of_a_strike_by_nearest_strike(spot: float, strike: float, strikes: List[float],
                                                   option_type: OptionType) -> float:
        # TODO
        return None

    @staticmethod
    def get_strike_by_monenyes_rank_nearest_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                                   option_type: OptionType) -> float:
        d = OptionM.get_strike_monenyes_rank_dict_nearest_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_nearest_strike(spot: float, strikes: List[float],
                                                     option_type: OptionType) -> dict:
        d = {}
        min_strike = strikes[0]
        max_strike = strikes[0]
        for strike in strikes:
            if strike < min_strike:
                min_strike = strike
            if strike > max_strike:
                max_strike = strike
        if spot < min_strike:
            spot = min_strike
        elif spot > max_strike:
            spot = max_strike
        for strike in strikes:
            if strike <= OptionM.MONEYNESS_POINT_LOW:
                if spot <= OptionM.MONEYNESS_POINT_LOW:
                    # strike = 1900, spot=1800, moneyness = (1800-1900)/25
                    rank = int(option_type.value * round((spot - strike) / 25))
                elif spot <= OptionM.MONEYNESS_POINT_HIGH:
                    # strike = 1900, spot = 2100, moneyness = (2000 - 1900)/25 + (2100 - 2000)/50
                    rank = int(option_type.value * round((OptionM.MONEYNESS_POINT_LOW - strike) / 25
                                                         + (spot - OptionM.MONEYNESS_POINT_LOW) / 50))
                else:
                    # strike = 1900, spot = 5100, moneyness = (5100 - 5000)/100 + (5000 - 2000)/50+(2000-1900)/25
                    rank = int(option_type.value * round((OptionM.MONEYNESS_POINT_LOW - strike) / 25 + \
                                                         (
                                                                     OptionM.MONEYNESS_POINT_HIGH - OptionM.MONEYNESS_POINT_LOW) / 50 \
                                                         + (spot - OptionM.MONEYNESS_POINT_HIGH) / 100))
            elif strike <= OptionM.MONEYNESS_POINT_HIGH:
                if spot <= OptionM.MONEYNESS_POINT_LOW:
                    # strike = 2100, spot=1800, moneyness = (2000-2100)/50 + (1800-2000)/25
                    rank = int(option_type.value * round((OptionM.MONEYNESS_POINT_LOW - strike) / 50 + \
                                                         (spot - OptionM.MONEYNESS_POINT_LOW) / 25))
                elif spot <= OptionM.MONEYNESS_POINT_HIGH:
                    # strike = 2100, spot = 3000, moneyness = (3000 - 2100)/50
                    rank = int(option_type.value * round((spot - strike) / 50))
                else:
                    # strike = 2100, spot = 5100, moneyness = (5100 - 5000)/100 + (5000 - 2100)/50
                    rank = int(option_type.value * round((OptionM.MONEYNESS_POINT_HIGH - strike) / 50 \
                                                         + (spot - OptionM.MONEYNESS_POINT_HIGH) / 100))
            else:
                if spot <= OptionM.MONEYNESS_POINT_LOW:
                    # strike = 5100, spot=1800, moneyness = (5000-5100)/100 + (2000-5000)/50 + (1800-2000)/25
                    rank = int(option_type.value * round((OptionM.MONEYNESS_POINT_HIGH - strike) / 100 + \
                                                         (
                                                                     OptionM.MONEYNESS_POINT_LOW - OptionM.MONEYNESS_POINT_HIGH) / 50 + \
                                                         (spot - OptionM.MONEYNESS_POINT_LOW) / 25))
                elif spot <= OptionM.MONEYNESS_POINT_HIGH:
                    # strike = 5100, spot = 3000, moneyness = (3000 - 5000)/50 + (5000-5100)/100
                    rank = int(option_type.value * round((spot - OptionM.MONEYNESS_POINT_HIGH) / 50) + \
                               (OptionM.MONEYNESS_POINT_HIGH - strike) / 100)
                else:
                    # strike = 5100, spot = 5300, moneyness = (5300-5100)/100
                    rank = int(option_type.value * round((spot - strike) / 100))
            d.update({rank: strike})
        return d

    @staticmethod
    def get_strike_by_monenyes_rank_otm_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                               option_type: OptionType) -> float:
        d = OptionM.get_strike_monenyes_rank_dict_otm_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_otm_strike(spot: float, strikes: List[float], option_type: OptionType) -> dict:
        d = {}
        min_strike = strikes[0]
        max_strike = strikes[0]
        for strike in strikes:
            if strike < min_strike:
                min_strike = strike
            if strike > max_strike:
                max_strike = strike
        if spot < min_strike:
            spot = min_strike
        elif spot > max_strike:
            spot = max_strike
        for strike in strikes:
            if strike <= OptionM.MONEYNESS_POINT_LOW:
                if spot <= OptionM.MONEYNESS_POINT_LOW:
                    # strike = 1900, spot=1800, moneyness = (1800-1900)/25
                    rank = int(np.floor(option_type.value * (spot - strike) / 25) + 1)
                elif spot <= OptionM.MONEYNESS_POINT_HIGH:
                    # strike = 1900, spot = 2100, moneyness = (2000 - 1900)/25 + (2100 - 2000)/50
                    rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT_LOW - strike) / 25
                                                             + (spot - OptionM.MONEYNESS_POINT_LOW) / 50)) + 1)
                else:
                    # strike = 1900, spot = 5100, moneyness = (5100 - 5000)/100 + (5000 - 2000)/50+(2000-1900)/25
                    rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT_LOW - strike) / 25
                                                             + (
                                                                         OptionM.MONEYNESS_POINT_HIGH - OptionM.MONEYNESS_POINT_LOW) / 50
                                                             + (spot - OptionM.MONEYNESS_POINT_HIGH) / 100)) + 1)
            elif strike <= OptionM.MONEYNESS_POINT_HIGH:
                if spot <= OptionM.MONEYNESS_POINT_LOW:
                    # strike = 2100, spot=1800, moneyness = (2000-2100)/50 + (1800-2000)/25
                    rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT_LOW - strike) / 50 + (
                                spot - OptionM.MONEYNESS_POINT_LOW) / 25)) + 1)
                elif spot <= OptionM.MONEYNESS_POINT_HIGH:
                    # strike = 2100, spot = 3000, moneyness = (3000 - 2100)/50
                    rank = int(np.floor(option_type.value * (spot - strike) / 50) + 1)
                else:
                    # strike = 2100, spot = 5100, moneyness = (5100 - 5000)/100 + (5000 - 2100)/50
                    rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT_HIGH - strike) / 50 + (
                                spot - OptionM.MONEYNESS_POINT_HIGH) / 100)) + 1)
            else:
                if spot <= OptionM.MONEYNESS_POINT_LOW:
                    # strike = 5100, spot=1800, moneyness = (5000-5100)/100 + (2000-5000)/50 + (1800-2000)/25
                    rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT_HIGH - strike) / 100
                                                             + (
                                                                         OptionM.MONEYNESS_POINT_LOW - OptionM.MONEYNESS_POINT_HIGH) / 50
                                                             + (spot - OptionM.MONEYNESS_POINT_LOW) / 25)) + 1)
                elif spot <= OptionM.MONEYNESS_POINT_HIGH:
                    # strike = 5100, spot = 3000, moneyness = (3000 - 5000)/50 + (5000-5100)/100
                    rank = int(np.floor(option_type.value * ((spot - OptionM.MONEYNESS_POINT_HIGH) / 50 + (
                                OptionM.MONEYNESS_POINT_HIGH - strike) / 100)) + 1)
                else:
                    # strike = 5100, spot = 5300, moneyness = (5300-5100)/100
                    rank = int(np.floor(option_type.value * (spot - strike) / 100) + 1)
            d.update({rank: strike})
        # for strike in strikes:
        #     if strike <= OptionM.MONEYNESS_POINT_LOW:
        #         if spot <= OptionM.MONEYNESS_POINT_LOW:
        #             # strike = 2.9, spot=2.8, moneyness = (2.8-2.9)/0.05
        #             rank = int(np.floor(option_type.value * (spot - strike) / 25) + 1)
        #         else:
        #             # strike = 2.9, spot = 3.1, moneyness = (3.0 - 2.9)/0.05 + (3.1 - 3.0)/0.1
        #             rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT - strike) / 0.05
        #                                                      + (spot - OptionM.MONEYNESS_POINT) / 0.1)) + 1)
        #     else:
        #         if spot <= OptionM.MONEYNESS_POINT:
        #             # strike = 3.1, spot = 2.9, moneyness = (3.0 - 3.1)/0.1+(2.9 - 3.0)/0.05
        #             rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT - strike) / 0.1
        #                                                      + (spot - OptionM.MONEYNESS_POINT) / 0.05)) + 1)
        #         else:
        #             # strike = 3.1, spot = 3.1, moneyness = (3.1-3.1)/0.1
        #             rank = int(np.floor(option_type.value * (spot - strike) / 0.1) + 1)
        #     d.update({rank: strike})
        return d

    @staticmethod
    def generate_commodity_option_maturities():
        maturity_date = 0
        dict_option_maturity = {}
        id_list = ['m_1707', 'm_1708', 'm_1709', 'm_1711', 'm_1712']
        calendar = ql.China()
        for contractId in id_list:
            year = '201' + contractId[3]
            month = contractId[-2:]
            date = ql.Date(1, int(month), int(year))
            maturity_date = calendar.advance(calendar.advance(date, ql.Period(-1, ql.Months)), ql.Period(4, ql.Days))
            dt_maturity = QuantlibUtil.to_dt_date(maturity_date)
            dict_option_maturity.update({contractId:dt_maturity})
        id_list_sr = ['sr_1707','sr_1709','sr_1711','sr_1801']
        for contractId in id_list_sr:
            year = '201' + contractId[4]
            month = contractId[-2:]
            date = ql.Date(1, int(month), int(year))
            maturity_date = calendar.advance(calendar.advance(date, ql.Period(-1, ql.Months)), ql.Period(-5, ql.Days))
            dt_maturity = QuantlibUtil.to_dt_date(maturity_date)
            dict_option_maturity.update({contractId:dt_maturity})
        print(dict_option_maturity)
        return maturity_date


class OptionSR:
    MONEYNESS_POINT_LOW = 3000
    MONEYNESS_POINT_HIGH = 10000

    @staticmethod
    def get_moneyness_of_a_strike_by_nearest_strike(spot: float, strike: float, strikes: List[float],
                                                   option_type: OptionType) -> float:
        # TODO
        return None

    @staticmethod
    def get_strike_by_monenyes_rank_nearest_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                                   option_type: OptionType) -> float:
        d = OptionSR.get_strike_monenyes_rank_dict_nearest_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_nearest_strike(spot: float, strikes: List[float],
                                                     option_type: OptionType) -> dict:
        d = {}
        for strike in strikes:
            if strike <= OptionSR.MONEYNESS_POINT_LOW:
                if spot <= OptionSR.MONEYNESS_POINT_LOW:
                    rank = int(option_type.value * round((spot - strike) / 50))
                elif spot <= OptionSR.MONEYNESS_POINT_HIGH:
                    rank = int(option_type.value * round((OptionSR.MONEYNESS_POINT_LOW - strike) / 50
                                                         + (spot - OptionSR.MONEYNESS_POINT_LOW) / 100))
                else:
                    rank = int(option_type.value * round((OptionSR.MONEYNESS_POINT_LOW - strike) / 50 + \
                                                         (OptionSR.MONEYNESS_POINT_HIGH - OptionSR.MONEYNESS_POINT_LOW) / 100 \
                                                         + (spot - OptionSR.MONEYNESS_POINT_HIGH) / 200))
            elif strike <= OptionSR.MONEYNESS_POINT_HIGH:
                if spot <= OptionSR.MONEYNESS_POINT_LOW:
                    rank = int(option_type.value * round((OptionSR.MONEYNESS_POINT_LOW - strike) / 100 + \
                                                            (spot-OptionSR.MONEYNESS_POINT_LOW) / 50))
                elif spot <= OptionSR.MONEYNESS_POINT_HIGH:
                    rank = int(option_type.value * round((spot - strike) / 100))
                else:
                    rank = int(option_type.value * round((OptionSR.MONEYNESS_POINT_HIGH - strike) / 100 \
                                                         + (spot - OptionSR.MONEYNESS_POINT_HIGH) / 200))
            else:
                if spot <= OptionSR.MONEYNESS_POINT_LOW:
                    rank = int(option_type.value * round((OptionSR.MONEYNESS_POINT_HIGH - strike) / 200 + \
                                                            (OptionSR.MONEYNESS_POINT_LOW - OptionSR.MONEYNESS_POINT_HIGH) / 100 + \
                                                            (spot-OptionSR.MONEYNESS_POINT_LOW) / 50))
                elif spot <= OptionSR.MONEYNESS_POINT_HIGH:
                    rank = int(option_type.value * round( (spot - OptionSR.MONEYNESS_POINT_HIGH) / 100) +\
                                                            (OptionSR.MONEYNESS_POINT_HIGH-strike) / 200)
                else:
                    rank = int(option_type.value * round((spot - strike) / 200))
            d.update({rank: strike})
        return d

    @staticmethod
    def get_strike_by_monenyes_rank_otm_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                               option_type: OptionType) -> float:
        d = OptionSR.get_strike_monenyes_rank_dict_otm_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_otm_strike(spot: float, strikes: List[float], option_type: OptionType) -> dict:
        d = {}
        for strike in strikes:
            if strike <= OptionSR.MONEYNESS_POINT_LOW:
                if spot <= OptionSR.MONEYNESS_POINT_LOW:
                    # strike = 1900, spot=1800, moneyness = (1800-1900)/25
                    rank = int(np.floor(option_type.value * (spot - strike) / 25) + 1)
                elif spot <= OptionSR.MONEYNESS_POINT_HIGH:
                    # strike = 1900, spot = 2100, moneyness = (2000 - 1900)/25 + (2100 - 2000)/50
                    rank = int(np.floor(option_type.value * ((OptionSR.MONEYNESS_POINT_LOW - strike) / 25
                                                         + (spot - OptionSR.MONEYNESS_POINT_LOW) / 50)) + 1)
                else:
                    # strike = 1900, spot = 5100, moneyness = (5100 - 5000)/100 + (5000 - 2000)/50+(2000-1900)/25
                    rank = int(np.floor(option_type.value * ((OptionSR.MONEYNESS_POINT_LOW - strike) / 25
                                                             + (OptionSR.MONEYNESS_POINT_HIGH - OptionSR.MONEYNESS_POINT_LOW) / 50
                                                             + (spot - OptionSR.MONEYNESS_POINT_HIGH) / 100)) + 1)
            elif strike <= OptionSR.MONEYNESS_POINT_HIGH:
                if spot <= OptionSR.MONEYNESS_POINT_LOW:
                    # strike = 2100, spot=1800, moneyness = (2000-2100)/50 + (1800-2000)/25
                    rank = int(np.floor(option_type.value * ((OptionSR.MONEYNESS_POINT_LOW - strike) / 50 + (spot-OptionSR.MONEYNESS_POINT_LOW) / 25)) + 1)
                elif spot <= OptionSR.MONEYNESS_POINT_HIGH:
                    # strike = 2100, spot = 3000, moneyness = (3000 - 2100)/50
                    rank = int(np.floor(option_type.value * (spot - strike) / 50) + 1)
                else:
                    # strike = 2100, spot = 5100, moneyness = (5100 - 5000)/100 + (5000 - 2100)/50
                    rank = int(np.floor(option_type.value * ((OptionSR.MONEYNESS_POINT_HIGH - strike) / 50+ (spot - OptionSR.MONEYNESS_POINT_HIGH) / 100))+1)
            else:
                if spot <= OptionSR.MONEYNESS_POINT_LOW:
                    # strike = 5100, spot=1800, moneyness = (5000-5100)/100 + (2000-5000)/50 + (1800-2000)/25
                    rank = int(np.floor(option_type.value * ((OptionSR.MONEYNESS_POINT_HIGH - strike) / 100
                                                             + (OptionSR.MONEYNESS_POINT_LOW - OptionSR.MONEYNESS_POINT_HIGH) / 50
                                                             + (spot-OptionSR.MONEYNESS_POINT_LOW) / 25)) + 1)
                elif spot <= OptionSR.MONEYNESS_POINT_HIGH:
                    # strike = 5100, spot = 3000, moneyness = (3000 - 5000)/50 + (5000-5100)/100
                    rank = int(np.floor(option_type.value * ((spot - OptionSR.MONEYNESS_POINT_HIGH) / 50 + (OptionSR.MONEYNESS_POINT_HIGH-strike) / 100)) + 1)
                else:
                    # strike = 5100, spot = 5300, moneyness = (5300-5100)/100
                    rank = int(np.floor(option_type.value * (spot - strike) / 100) + 1)
            d.update({rank: strike})
        # for strike in strikes:
        #     if strike <= OptionM.MONEYNESS_POINT_LOW:
        #         if spot <= OptionM.MONEYNESS_POINT_LOW:
        #             # strike = 2.9, spot=2.8, moneyness = (2.8-2.9)/0.05
        #             rank = int(np.floor(option_type.value * (spot - strike) / 25) + 1)
        #         else:
        #             # strike = 2.9, spot = 3.1, moneyness = (3.0 - 2.9)/0.05 + (3.1 - 3.0)/0.1
        #             rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT - strike) / 0.05
        #                                                      + (spot - OptionM.MONEYNESS_POINT) / 0.1)) + 1)
        #     else:
        #         if spot <= OptionM.MONEYNESS_POINT:
        #             # strike = 3.1, spot = 2.9, moneyness = (3.0 - 3.1)/0.1+(2.9 - 3.0)/0.05
        #             rank = int(np.floor(option_type.value * ((OptionM.MONEYNESS_POINT - strike) / 0.1
        #                                                      + (spot - OptionM.MONEYNESS_POINT) / 0.05)) + 1)
        #         else:
        #             # strike = 3.1, spot = 3.1, moneyness = (3.1-3.1)/0.1
        #             rank = int(np.floor(option_type.value * (spot - strike) / 0.1) + 1)
        #     d.update({rank: strike})
        return d


class Option50ETF:
    MONEYNESS_POINT = 3.0

    DIVIDEND_DATES = {
        datetime.date(2016, 11, 29): [
            '1612', '1701', '1703', '1706'
        ],
        datetime.date(2017, 11, 28): [
            '1712', '1801', '1803', '1806'
        ]

    }

    @staticmethod
    def fun_strike_before_adj(df: pd.Series) -> float:
        return round(df[Util.AMT_STRIKE] * df[Util.NBR_MULTIPLIER] / 10000, 2)

    @staticmethod
    def fun_applicable_strike(df: pd.Series) -> float:
        eval_date = df[Util.DT_DATE]
        contract_month = df[Util.NAME_CONTRACT_MONTH]
        dividend_dates = Option50ETF.DIVIDEND_DATES
        dates = sorted(dividend_dates.keys(), reverse=False)
        if eval_date < dates[0]:
            return round(df[Util.AMT_STRIKE] * df[Util.NBR_MULTIPLIER] / 10000, 2)  # 分红除息日前反算调整前的行权价
        elif eval_date < dates[1]:
            if contract_month in dividend_dates[dates[1]]:
                return round(df[Util.AMT_STRIKE] * df[Util.NBR_MULTIPLIER] / 10000, 2)  # 分红除息日前反算调整前的行权价
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

    @staticmethod
    def get_moneyness_of_a_strike_by_nearest_strike(spot: float, strike: float, strikes: List[float],
                                                   option_type: OptionType) -> float:
        min_strike = strikes[0]
        max_strike = strikes[0]
        for strike in strikes:
            if strike < min_strike:
                min_strike = strike
            if strike > max_strike:
                max_strike = strike
        if spot < min_strike:
            spot = min_strike
        elif spot > max_strike:
            spot = max_strike
        if strike <= Option50ETF.MONEYNESS_POINT:
            if spot <= Option50ETF.MONEYNESS_POINT:
                # strike = 2.9, spot=2.8, moneyness = (2.8-2.9)/0.05
                rank = int(option_type.value * round((spot - strike) / 0.05))
            else:
                # strike = 2.9, spot = 3.1, moneyness = (3.0 - 2.9)/0.05 + (3.1 - 3.0)/0.1
                rank = int(option_type.value * round((Option50ETF.MONEYNESS_POINT - strike) / 0.05
                                                         + (spot - Option50ETF.MONEYNESS_POINT) / 0.1))
        else:
            if spot <= Option50ETF.MONEYNESS_POINT:
                # strike = 3.1, spot = 2.9, moneyness = (3.0 - 3.1)/0.1+(2.9 - 3.0)/0.05
                rank = int(option_type.value * round((Option50ETF.MONEYNESS_POINT - strike) / 0.1
                                                         + (spot - Option50ETF.MONEYNESS_POINT) / 0.05))
            else:
                # strike = 3.1, spot = 3.1, moneyness = (3.1-3.1)/0.1
                rank = int(option_type.value * round((spot - strike) / 0.1))
        return rank


    @staticmethod
    def get_strike_by_monenyes_rank_nearest_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                                   option_type: OptionType) -> float:
        d = Option50ETF.get_strike_monenyes_rank_dict_nearest_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_nearest_strike(spot: float, strikes: List[float],
                                                     option_type: OptionType) -> dict:
        d = {}
        min_strike = strikes[0]
        max_strike = strikes[0]
        for strike in strikes:
            if strike < min_strike:
                min_strike = strike
            if strike > max_strike:
                max_strike = strike
        if spot < min_strike:
            spot = min_strike
        elif spot > max_strike:
            spot = max_strike
        for strike in strikes:
            if strike <= Option50ETF.MONEYNESS_POINT:
                if spot <= Option50ETF.MONEYNESS_POINT:
                    # strike = 2.9, spot=2.8, moneyness = (2.8-2.9)/0.05
                    rank = int(option_type.value * round((spot - strike) / 0.05))
                else:
                    # strike = 2.9, spot = 3.1, moneyness = (3.0 - 2.9)/0.05 + (3.1 - 3.0)/0.1
                    rank = int(option_type.value * round((Option50ETF.MONEYNESS_POINT - strike) / 0.05
                                                         + (spot - Option50ETF.MONEYNESS_POINT) / 0.1))
            else:
                if spot <= Option50ETF.MONEYNESS_POINT:
                    # strike = 3.1, spot = 2.9, moneyness = (3.0 - 3.1)/0.1+(2.9 - 3.0)/0.05
                    rank = int(option_type.value * round((Option50ETF.MONEYNESS_POINT - strike) / 0.1
                                                         + (spot - Option50ETF.MONEYNESS_POINT) / 0.05))
                else:
                    # strike = 3.1, spot = 3.1, moneyness = (3.1-3.1)/0.1
                    rank = int(option_type.value * round((spot - strike) / 0.1))
            d.update({rank: strike})
        return d

    @staticmethod
    def get_strike_by_monenyes_rank_otm_strike(spot: float, moneyness_rank: int, strikes: List[float],
                                               option_type: OptionType) -> float:
        d = Option50ETF.get_strike_monenyes_rank_dict_otm_strike(spot, strikes, option_type)
        return d.get(moneyness_rank, None)

    @staticmethod
    def get_strike_monenyes_rank_dict_otm_strike(spot: float, strikes: List[float], option_type: OptionType) -> dict:
        d = {}
        min_strike = strikes[0]
        max_strike = strikes[0]
        for strike in strikes:
            if strike < min_strike:
                min_strike = strike
            if strike > max_strike:
                max_strike = strike
        if spot < min_strike:
            spot = min_strike
        elif spot > max_strike:
            spot = max_strike
        for strike in strikes:
            if strike <= Option50ETF.MONEYNESS_POINT:
                if spot <= Option50ETF.MONEYNESS_POINT:
                    # strike = 2.9, spot=2.8, moneyness = (2.8-2.9)/0.05
                    rank = int(np.floor(option_type.value * (spot - strike) / 0.05) + 1)
                else:
                    # strike = 2.9, spot = 3.1, moneyness = (3.0 - 2.9)/0.05 + (3.1 - 3.0)/0.1
                    rank = int(np.floor(option_type.value * ((Option50ETF.MONEYNESS_POINT - strike) / 0.05
                                                             + (spot - Option50ETF.MONEYNESS_POINT) / 0.1)) + 1)
            else:
                if spot <= Option50ETF.MONEYNESS_POINT:
                    # strike = 3.1, spot = 2.9, moneyness = (3.0 - 3.1)/0.1+(2.9 - 3.0)/0.05
                    rank = int(np.floor(option_type.value * ((Option50ETF.MONEYNESS_POINT - strike) / 0.1
                                                             + (spot - Option50ETF.MONEYNESS_POINT) / 0.05)) + 1)
                else:
                    # strike = 3.1, spot = 3.1, moneyness = (3.1-3.1)/0.1
                    rank = int(np.floor(option_type.value * (spot - strike) / 0.1) + 1)
            d.update({rank: strike})
        return d


class OptionFilter:

    dict_maturities = {'m_1707': datetime.date(2017, 6, 7),
                       'm_1708': datetime.date(2017, 7, 7),
                       'm_1709': datetime.date(2017, 8, 7),
                       'm_1711': datetime.date(2017, 10, 13),
                       'm_1712': datetime.date(2017, 11, 7),
                       'sr_1707': datetime.date(2017, 5, 23),
                       'sr_1709': datetime.date(2017, 7, 25),
                       'sr_1711': datetime.date(2017, 9, 25),
                       'sr_1801': datetime.date(2017, 11, 24)
                       }

    @staticmethod
    def fun_option_type_split(df: pd.Series) -> Union[str, None]:
        id_instrument = df[Util.ID_INSTRUMENT]
        type_str = id_instrument.split('_')[2]
        if type_str == 'c':
            option_type = Util.STR_CALL
        elif type_str == 'p':
            option_type = Util.STR_PUT
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


    @staticmethod
    def fun_option_maturity(df):
        if df[Util.DT_MATURITY] is None or pd.isnull(df[Util.DT_MATURITY]):
            return OptionFilter.dict_maturities[df[Util.ID_UNDERLYING]]
        else:
            return df[Util.DT_MATURITY]


class FutureUtil:
    @staticmethod
    def get_contract_shift_cost(c1, c2, long_short: LongShort):
        return

    @staticmethod
    def get_futures_daily_c1(df):
        df = df.sort_values(by=[Util.DT_DATE, Util.AMT_TRADING_VOLUME], ascending=False)
        df_rs = df.drop_duplicates(subset=[Util.DT_DATE]).sort_values(by=Util.DT_DATE, ascending=True).reset_index(drop=True)
        return df_rs

    @staticmethod
    def get_futures_minute_c1(df):
        tmp = df.groupby([Util.DT_DATE,Util.ID_INSTRUMENT]).sum()[Util.AMT_TRADING_VOLUME].to_frame()
        tmp = tmp.reset_index(level=[Util.DT_DATE,Util.ID_INSTRUMENT]).sort_values(by=Util.AMT_TRADING_VOLUME,ascending=False)
        tmp = tmp.drop_duplicates(subset=[Util.DT_DATE]).sort_values(by=Util.DT_DATE, ascending=True)
        df0 = tmp[[Util.DT_DATE,Util.ID_INSTRUMENT]].rename(columns={Util.ID_INSTRUMENT: 'id_core'})
        df2 = pd.merge(df, df0, on=Util.DT_DATE, how='left')
        df2 = df2[df2[Util.ID_INSTRUMENT] == df2['id_core']].reset_index(drop=True)
        return df2

    # TODO
    @staticmethod
    def get_future_c1_by_option_mdt_minute(df, option_maturities):

        return

class Hedge:
    @staticmethod
    def whalley_wilmott(ttm, gamma, spot, rho=1, fee=5.0 / 10000.0, rf=0.03):
        # ttm = self.pricing_utl.get_ttm(eval_date, option.dt_maturity)
        H = (1.5 * math.exp(-rf * ttm) * fee * spot * (gamma ** 2) / rho) ** (1 / 3)
        return H


class Calendar(object):
    def __init__(self, date_list: typing.List[datetime.date]):
        self.date_list = sorted(date_list)
        self.max_year = self.date_list[-1].year
        self.date_map = {}
        self.init()

    """
    Initialize Calender with date_list
    {
        "2017": {
            "1": [datetime(2017,1,1), datetime(2017,1,2)],
            "3": [datetime(2017,3,1), datetime(2017,3,2)]
        }
    }
    """

    def init(self):
        for date in self.date_list:
            year_map = self.date_map.get(date.year, None)
            if year_map is None:
                year_map = {}
                self.date_map[date.year] = year_map
            month_list = year_map.get(date.month, None)
            if month_list is None:
                month_list = []
                year_map[date.month] = month_list
            month_list.append(date)
        for year, year_map in self.date_map.items():
            for month, month_list in year_map.items():
                year_map[month] = sorted(year_map[month])

    def next(self, dt):
        if dt < self.date_list[-1]:
            return self.date_list[self.date_list.index(dt) + 1]
        else:
            return

    def firstBusinessDayNextMonth(self, date: datetime.date) -> datetime.date:
        year = date.year
        month = date.month
        # Date is like 2017-12-3
        if month == 12:
            if year == self.max_year:
                raise ValueError("No available business in next month")
            else:
                return self.date_map.get(year + 1).get(1)[0]
        else:
            return self.date_map.get(year).get(month + 1)[0]

    def lastBusinessDayThisMonth(self, date: datetime.date) -> datetime.date:
        year = date.year
        month = date.month
        last_business_day_this_month = self.date_map.get(year).get(month)[-1]
        if date >= last_business_day_this_month:
            raise ValueError("No available business day after date {} this month".format(date))
        else:
            return last_business_day_this_month


# dl = [datetime.date(2017, 1, 4), datetime.date(2017, 1, 2), datetime.date(2018, 1, 1), datetime.date(2018, 1, 2),datetime.date(2017, 2, 6),
#       datetime.date(2017, 3, 6), datetime.date(2017, 3, 3), datetime.date(2017, 3, 5), datetime.date(2017, 4, 2),
#       datetime.date(2017, 4, 5)]
# c = Calendar(dl, 2018)
# c.init()
# print(c.firstBusinessDayNextMonth(datetime.date(2017,1,1)))


class PricingUtil:
    @staticmethod
    def payoff(spot: float, strike: float, option_type: OptionType):
        return abs(max(option_type.value * (spot - strike), 0.0))

    @staticmethod
    def get_ttm(dt_eval, dt_maturity):
        N = (dt_maturity - dt_eval).total_seconds() / 60.0
        N365 = 365 * 1440.0
        ttm = N / N365
        return ttm

    @staticmethod
    def get_std(dt_eval, dt_maturity, annualized_vol):
        stdDev = annualized_vol * math.sqrt(PricingUtil.get_ttm(dt_eval, dt_maturity))
        return stdDev

    @staticmethod
    def get_discount(dt_eval, dt_maturity, rf):
        discount = math.exp(-rf * PricingUtil.get_ttm(dt_eval, dt_maturity))
        return discount

    @staticmethod
    def get_maturity_metrics(self, dt_date, spot, option):
        strike = option.strike
        if option.option_type == OptionType.PUT:
            if strike > spot:  # ITM
                delta = -1.0
            elif strike < spot:  # OTM
                delta = 0.0
            else:
                delta = 0.5
            option_price = max(strike - spot, 0)
        else:
            if strike < spot:  # ITM
                delta = 1.0
            elif strike > spot:  # OTM
                delta = 0.0
            else:
                delta = 0.5
            option_price = max(spot - strike, 0)
        delta = delta
        option_price = option_price
        return delta, option_price

    @staticmethod
    def get_mdate_by_contractid(commodityType, contractId, calendar):
        maturity_date = 0
        if commodityType == 'm':
            year = '20' + contractId[0: 2]
            month = contractId[-2:]
            date = ql.Date(1, int(month), int(year))
            maturity_date = calendar.advance(calendar.advance(date, ql.Period(-1, ql.Months)), ql.Period(4, ql.Days))
        elif commodityType == 'sr':
            year = '201' + contractId[2]
            month = contractId[-2:]
            date = ql.Date(1, int(month), int(year))
            maturity_date = calendar.advance(calendar.advance(date, ql.Period(-1, ql.Months)), ql.Period(-5, ql.Days))
        return maturity_date


class Util:
    """database column names"""
    # basic
    DT_DATE = 'dt_date'
    DT_DATETIME = 'dt_datetime'
    CODE_INSTRUMENT = 'code_instrument'
    ID_INSTRUMENT = 'id_instrument'
    ID_FUTURE = 'id_future'

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
    AMT_CALL_TRADING_VOLUME = 'amt_call_trading_volume'
    AMT_PUT_TRADING_VOLUME = 'amt_put_trading_volume'
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
    PCT_IV_OTM_BY_HTBR = 'pct_iv_by_htbr'
    PCT_IV_CALL_BY_HTBR = 'pct_iv_call_by_htbr'
    PCT_IV_PUT_BY_HTBR = 'pct_iv_put_by_htbr'
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
    AMT_YIELD = 'amt_yield'
    AMT_HISTVOL = 'amt_hist_vol'
    AMT_PARKINSON_NUMBER = 'amt_parkinson_number'
    AMT_GARMAN_KLASS = 'amt_garman_klass'
    AMT_HEDHE_UNIT = 'amt_hedge_unit'
    AMT_CALL_QUOTE = 'amt_call_quote'
    AMT_PUT_QUOTE = 'amt_put_quote'
    AMT_TTM = 'amt_ttm'
    AMT_HTB_RATE='amt_HTB_rate'
    NAME_CODE = 'name_code'
    STR_CALL = 'call'
    STR_PUT = 'put'
    STR_50ETF = '50etf'
    STR_INDEX_50ETF = 'index_50etf'
    STR_INDEX_50SH = 'index_50sh'
    STR_INDEX_300SH = 'index_300sh'
    STR_M = 'm'
    STR_IH = 'ih'
    STR_IF = 'iF'
    STR_SR = 'sr'
    STR_ALL = 'all'
    NAN_VALUE = -999.0

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
    MAIN_CONTRACT_159 = [1, 5, 9,'01','05','09']
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
    MARGIN_UNREALIZED_PNL = 'margin_unrealized_pnl'
    NONMARGIN_UNREALIZED_PNL = 'nonmargin_unrealized_pnl'
    PORTFOLIO_DELTA = 'portfolio_delta'
    BILLION = 1000000000.0
    TRADE_BOOK_COLUMN_LIST = [DT_DATE, TRADE_LONG_SHORT, TRADE_UNIT,
                              LAST_PRICE, TRADE_MARGIN_CAPITAL,
                              TRADE_BOOK_VALUE, AVERAGE_POSITION_COST,
                              TRADE_REALIZED_PNL, NBR_MULTIPLIER,
                              POSITION_CURRENT_VALUE, PORTFOLIO_UNREALIZED_PNL
                              ]  # ID_INSTRUMENR是df的index
    ACCOUNT_COLUMNS = [DT_DATE, CASH, PORTFOLIO_MARGIN_CAPITAL, PORTFOLIO_TRADES_VALUE,
                       PORTFOLIO_VALUE, PORTFOLIO_NPV, PORTFOLIO_UNREALIZED_PNL,
                       PORTFOLIO_LEVERAGE, TRADE_REALIZED_PNL,
                       PORTFOLIO_SHORT_POSITION_SCALE, PORTFOLIO_LONG_POSITION_SCALE,
                       MARGIN_UNREALIZED_PNL, NONMARGIN_UNREALIZED_PNL,PORTFOLIO_DELTA
                       ]
    DICT_FUTURE_MARGIN_RATE = {  # 合约价值的百分比
        'm': 0.05,
        'sr': 0.05,
        'if': 0.15,
        'ih': 0.15,
        'ic': 0.15,
    }
    DICT_TRANSACTION_FEE = {  # 元/手
        'm': 3.0,
        'sr': 3.0,
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
        'm':None,
        'sr':None,
        'if': 6.9 / 10000.0,
        'ih': 6.9 / 10000.0,
        'ic': 6.9 / 10000.0,
    }
    DICT_CONTRACT_MULTIPLIER = {  # 合约乘数
        'm': 10,
        'sr':10,
        'if': 300,
        'ih': 300,
        'ic': 200
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
        'ic': 0.2,
        'index': 0
    }

    # DZQH_CF_DATA_MISSING_DATES = [datetime.date(2017, 12, 28), datetime.date(2017, 12, 29), datetime.date(2018, 1, 26),
    #                               datetime.date(2018, 5, 4)]
    DZQH_CF_DATA_MISSING_DATES = []

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

    @staticmethod
    def largest_element_less_than(list, val):
        for i in range(len(list), 0, -1):
            if list[i - 1] < val:
                return list[i - 1]
            elif i == 0:
                return None
            else:
                continue


import QuantLib as ql


class QuantlibUtil:
    @staticmethod
    def to_dt_dates(ql_dates):
        datetime_dates = []
        for d in ql_dates:
            dt = datetime.date(d.year(), d.month(), d.dayOfMonth())
            datetime_dates.append(dt)
        return datetime_dates

    @staticmethod
    def to_ql_dates(datetime_dates):
        ql_dates = []
        for d in datetime_dates:
            dt = ql.Date(d.day, d.month, d.year)
            ql_dates.append(dt)
        return ql_dates

    @staticmethod
    def to_ql_date(datetime_date):
        dt = ql.Date(datetime_date.day, datetime_date.month, datetime_date.year)
        return dt

    @staticmethod
    def to_dt_date(ql_date):
        dt = datetime.date(ql_date.year(), ql_date.month(), ql_date.dayOfMonth())
        return dt

    # @staticmethod
    # def get_curve_treasury_bond(evalDate, daycounter):
    #     datestr = str(evalDate.year()) + "-" + str(evalDate.month()) + "-" + str(evalDate.dayOfMonth())
    #     try:
    #         curvedata = pd.read_json(os.path.abspath('..') + '\marketdata\curvedata_tb_' + datestr + '.json')
    #         rates = curvedata.values[0]
    #         calendar = ql.China()
    #         dates = [evalDate,
    #                  calendar.advance(evalDate, ql.Period(1, ql.Months)),
    #                  calendar.advance(evalDate, ql.Period(3, ql.Months)),
    #                  calendar.advance(evalDate, ql.Period(6, ql.Months)),
    #                  calendar.advance(evalDate, ql.Period(9, ql.Months)),
    #                  calendar.advance(evalDate, ql.Period(1, ql.Years))]
    #         krates = np.divide(rates, 100)
    #         curve = ql.ForwardCurve(dates, krates, daycounter)
    #     except Exception as e:
    #         print(e)
    #         print('Error def -- get_curve_treasury_bond in \'svi_read_data\' on date : ', evalDate)
    #         return
    #     return curve

    #
    # @staticmethod
    # def get_rf_tbcurve(evalDate, daycounter, maturitydate):
    #     curve = get_curve_treasury_bond(evalDate, daycounter)
    #     maxdate = curve.maxDate()
    #     # print(maxdate,maturitydate)
    #     if maturitydate > maxdate:
    #         rf = curve.zeroRate(maxdate, daycounter, ql.Continuous).rate()
    #     else:
    #         rf = curve.zeroRate(maturitydate, daycounter, ql.Continuous).rate()
    #     return rf

    @staticmethod
    def get_yield_ts(evalDate, curve, mdate, daycounter):
        maxdate = curve.maxDate()
        if mdate > maxdate:
            rf = curve.zeroRate(maxdate, daycounter, ql.Continuous).rate()
        else:
            rf = curve.zeroRate(mdate, daycounter, ql.Continuous).rate()
        yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(evalDate, rf, daycounter))
        return yield_ts

    @staticmethod
    def get_dividend_ts(evalDate, daycounter):
        dividend_ts = ql.YieldTermStructureHandle(ql.FlatForward(evalDate, 0.0, daycounter))
        return dividend_ts



class Statistics:

    @staticmethod
    def moving_average(df_series, n):
        ma = df_series.rolling(window=n).mean()
        return ma

    @staticmethod
    def standard_deviation(df_series, n):
        std = df_series.rolling(window=n).std()
        return std

    @staticmethod
    def percentile(df_series, n, percent):
        return df_series.rolling(window=n).quantile(percent)

    @staticmethod
    def volatility_by_closes(df_series_closes, n=20):
        series = np.log(df_series_closes).diff()
        vol= series.rolling(window=n).std() * math.sqrt(252)
        return vol