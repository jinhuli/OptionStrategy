import pandas as pd
import datetime
import QuantLib as ql
from PricingLibrary.EngineQuantlib import QlBlackFormula
import back_test.model.constant as c
import math

eval_date = datetime.date.today()
spot = 49130

rf = 0.03

calendar = ql.China()
df = pd.read_excel('C:/Users/wangd/Documents/Dongli/00_数据/铜期权日度数据-20180921.xlsx', sheetname='analysis')


def maturity_date(series):
    cm = series['合约月份']
    month = cm - 1900
    d1 = ql.Date(1, month, 2019)
    mdt = calendar.advance(d1, ql.Period(-5, ql.Days))
    mdt = c.QuantlibUtil.to_dt_date(mdt)
    return mdt


def iv(series,S, cd_price,option_type=None):
    if option_type is None:
        if series['期权类型'] == 'C':
            option_type = c.OptionType.CALL
        elif series['期权类型'] == 'P':
            option_type = c.OptionType.PUT
        else:
            option_type = None
    black = QlBlackFormula(eval_date, series['到期日'], option_type, S, series['行权价'], rf=rf)
    iv = black.estimate_vol(series[cd_price])
    return iv

def fun_calculate_iv(series,S,option_type):
    if option_type == c.OptionType.PUT:
        vol = iv(series,S,c.Util.AMT_PUT_QUOTE,c.OptionType.PUT)
    elif option_type == c.OptionType.CALL:
        vol = iv(series, S, c.Util.AMT_CALL_QUOTE, c.OptionType.CALL)
    else:
        return
    return vol

def fun_otm_iv(df_series):
    K = df_series['行权价']
    if K <= spot:
        return df_series['iv_put']
    else:
        return df_series['iv_call']

def fun_otm_iv_htbr(df_series):
    K = df_series['行权价']
    if K <= spot:
        return df_series['iv_put_htbr']
    else:
        return df_series['iv_call_htbr']

def fun_htb_rate(df_series, rf):
    r = -math.log((df_series[c.Util.AMT_CALL_QUOTE] - df_series[c.Util.AMT_PUT_QUOTE]
                   + df_series['行权价'] * math.exp(-rf * df_series[c.Util.AMT_TTM]))
                  / spot) / df_series[c.Util.AMT_TTM]
    return r

def fun_ttm(series):
    return (series['到期日'] - eval_date).total_seconds() / 60.0 / (365 * 1440.0)


df = df[['合约代码', '合约月份', '行权价', '期权类型', '开盘价', '最高价', '最低价', '收盘价', '成交量', '持仓量', '成交额']]
df['到期日'] = df.apply(lambda x: maturity_date(x), axis=1)
df[c.Util.AMT_TTM] = df.apply(lambda x: fun_ttm(x),axis=1)
df['成交量加权均价'] = 10000 * df['成交额'] / df['成交量'] / 5
df['iv_last'] = df.apply(lambda x: iv(x, spot, '收盘价'), axis=1)
df['iv_vw'] = df.apply(lambda x: iv(x, spot, '成交量加权均价'), axis=1)

df1_call = df[(df['合约月份'] == 1902) & (df['期权类型'] == 'C')][['合约月份','到期日', '行权价', '收盘价', 'iv_last',c.Util.AMT_TTM]].rename(
    columns={'iv_last': 'iv_call', '收盘价': c.Util.AMT_CALL_QUOTE})
df1_put = df[(df['合约月份'] == 1902) & (df['期权类型'] == 'P')][['行权价', '收盘价', 'iv_last']].rename(
    columns={'iv_last': 'iv_put', '收盘价': c.Util.AMT_PUT_QUOTE})
df1 = pd.merge(df1_call, df1_put, on='行权价')

diff = abs(df1.loc[:, '行权价'] - spot)
htb_r = fun_htb_rate(df1.loc[diff.idxmin()], rf)
spot_htbr = spot * math.exp(-htb_r * df1[c.Util.AMT_TTM].values[0])

df1['iv_otm_curve'] = df1.apply(lambda x: fun_otm_iv(x), axis=1)
df1['iv_call_htbr'] = df1.apply(lambda x: fun_calculate_iv(x,spot_htbr,c.OptionType.CALL), axis=1)
df1['iv_put_htbr'] = df1.apply(lambda x: fun_calculate_iv(x,spot_htbr,c.OptionType.PUT), axis=1)
df1['iv_otm_curve_htbr'] = df1.apply(lambda x: fun_otm_iv_htbr(x), axis=1)

print(df)
