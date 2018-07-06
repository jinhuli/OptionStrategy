from enum import Enum
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


    @staticmethod
    def filter_invalid_data(x) -> bool:
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
