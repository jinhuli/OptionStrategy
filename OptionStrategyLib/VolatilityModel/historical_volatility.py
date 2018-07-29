import math

import pandas as pd

from Utilities import calculate
from back_test.model.constant import Util


class historical_volatility_model:

    @staticmethod
    def hist_vol_1M(df):
        df_vol = df[[Util.DT_DATE]]
        df_vol[Util.AMT_HISTVOL_1M] = calculate.calculate_histvol(df[Util.AMT_CLOSE], 20)
        df_vol = df_vol.dropna().set_index(Util.DT_DATE)
        return df_vol

    @staticmethod
    def parkinson_number_1M(df):
        n = 20
        df_vol = df[[Util.DT_DATE]]
        # squred_log_h_l = (math.log(df[Util.AMT_HIGH] / df[Util.AMT_LOW], math.e)) ** 2
        squred_log_h_l = df.apply(historical_volatility_model.fun_parkinson_number, axis=1)
        sum_squred_log_h_l = squred_log_h_l.rolling(window=n).sum()
        df_vol[Util.AMT_PARKINSON_NUMBER_1M] = sum_squred_log_h_l.apply(
            lambda x: math.sqrt(x / (n * 4 * math.log(2, 2))))
        df_vol = df_vol.dropna().set_index(Util.DT_DATE)
        return df_vol

    @staticmethod
    def fun_parkinson_number(df: pd.Series) -> float:
        return (math.log(df[Util.AMT_HIGH] / df[Util.AMT_LOW], 2)) ** 2
