from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from back_test.model.base_instrument import BaseInstrument
from back_test.model.base_option import BaseOption
from data_access import get_data
import back_test.model.constant as c
import datetime
import numpy as np
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
import math

pu = PlotUtil()

start_date = datetime.date(2016, 2, 1)
end_date = start_date + datetime.timedelta()

min_holding = 15
nbr_maturity = 1
slippage = 0
pct_underlying_invest = 0.7
res = {}
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_underlying = get_data.get_index_mktdata(start_date, end_date, c.Util.STR_INDEX_50ETF)

def get_option_unit(option_put: BaseOption, underlying_value:float):
    unit = np.floor(underlying_value/ option_put.strike() / option_put.multiplier()) # 期权名义本金等于标的市值
    return unit

