import datetime
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import numpy as np
import QuantLib as ql

def otc_quote(evalDate,  spot_price,strike, vol, mdtDate=None, T=None, rf=0.03, dividend_rate=0.0, n=3):
    optionType = ql.Option.Call
    calendar = ql.China()
    daycounter = ql.ActualActual()
    underlying = ql.SimpleQuote(spot_price)
    volatility = ql.SimpleQuote(vol)
    eval_date = ql.Date(evalDate.day, evalDate.month, evalDate.year)
    effectivedt = calendar.advance(eval_date, ql.Period(n, ql.Days))  # T+3日可开始行权
    if T != None:
        if T == '1M':
            maturitydt  = calendar.advance(eval_date, ql.Period(1, ql.Months))
        elif T == '2M':
            maturitydt = calendar.advance(eval_date, ql.Period(2, ql.Months))
        elif T == '3M':
            maturitydt = calendar.advance(eval_date, ql.Period(3, ql.Months))
        elif T == '6M':
            maturitydt = calendar.advance(eval_date, ql.Period(6, ql.Months))
        else:
            return '期限不支持！'
    elif mdtDate != None:
        maturitydt = ql.Date(mdtDate.day, mdtDate.month, mdtDate.year)
    else:
        return '缺少到期日！'
    ql.Settings.instance().evaluationDate = eval_date

    exercise = ql.AmericanExercise(effectivedt, maturitydt)
    payoff = ql.PlainVanillaPayoff(optionType, strike)
    ame_option = ql.VanillaOption(payoff, exercise)
    flat_vol_ts = ql.BlackVolTermStructureHandle(
        ql.BlackConstantVol(eval_date, calendar, ql.QuoteHandle(volatility), daycounter))
    dividend_ts = ql.YieldTermStructureHandle(
        ql.FlatForward(eval_date, dividend_rate, daycounter))
    yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(eval_date, rf, daycounter))
    bsmprocess = ql.BlackScholesMertonProcess(ql.QuoteHandle(underlying), dividend_ts, yield_ts, flat_vol_ts)
    ame_option.setPricingEngine(ql.BinomialVanillaEngine(bsmprocess, 'crr', 801))
    ame_price = ame_option.NPV()
    return ame_price

def hisvol(data,n):
    datas=np.log(data)
    df=datas.diff()
    vol=df.rolling(window = n).std()*np.sqrt(252)
    return vol



# TODO: CHANGE URL
url = '../data/'
# TODO : CHANGE TO TEH LAST TEADING DAY
end_date = datetime.date(2018, 4, 19)
evaluation_date = end_date


engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
table_stocks = Table('stocks_mktdata', metadata, autoload=True)


start_date = datetime.date(2017, 4, 7) # 数据起始日期

query_mkt = sess.query(table_stocks.c.dt_date,table_stocks.c.code_instrument,table_stocks.c.amt_close) \
    .filter(table_stocks.c.dt_date >= start_date).filter(table_stocks.c.dt_date <= end_date)

# Read stock data from mysql database
df = pd.read_sql(query_mkt.statement,query_mkt.session.bind).set_index('dt_date').dropna()
df['code'] = df['code_instrument'].apply(lambda x:x.split('.')[0])

# read stock codes to quotes
df_codes = pd.read_excel(url+'20180411_东证润和_期权卖方报价.xls',converters={'股票代码': lambda x: str(x)})
code_stocks = df_codes['股票代码'].unique() # 所有待报价的股票代码
A_shares_codes = df['code'].unique()
# get otc option quotes
hist_vols = []
recheck = []
res_quote = []
print('=' * 100)
print('%20s %20s %20s %20s %20s %20s %20s %20s' %('code','eval date','empty days','vol','spot price','option price 1M(%)','option price 2M(%)','option price 3M(%)'))
print('-'*100)
for code in A_shares_codes:

    dataset = df[df['code'] == code]
    data_checked = dataset[dataset['amt_close']!=-1] # 收盘价不为空
    # data_checked['diff'] = data_checked['amt_close'].pct_change()
    # data_checked = data_checked[data_checked['diff']!=0.0] # 今收盘不等于昨收盘
    closes = data_checked['amt_close']
    cont = len(dataset.index)-len(data_checked.index) # 检查停牌天数
    if cont >= 100:
        recheck.append(code)
    # Check last trading day data
    eval_date = closes.last_valid_index()
    if eval_date != end_date:
        print(code, ' : 最新交易日数据不存在')
        if code not in recheck: recheck.append(code)

    vol_data = hisvol(closes,21).dropna().values
    strike = spot_price = closes.iloc[-1]
    try:
        vol = np.percentile(vol_data, 75)
        hist_vols.append({'code':code,'vol':vol,'spot_price':spot_price})
        if code in code_stocks:
            quote_1M = otc_quote(eval_date, spot_price, strike, vol, T='1M')
            quote_2M = otc_quote(eval_date, spot_price, strike, vol, T='2M')
            quote_3M = otc_quote(eval_date, spot_price, strike, vol, T='3M')
            quote_6M = otc_quote(eval_date, spot_price, strike, vol, T='6M')
            option_price1 = 100*quote_1M/spot_price
            option_price2 = 100*quote_2M/spot_price
            option_price3 = 100*quote_3M/spot_price
            option_price4 = 100*quote_6M/spot_price
            res_quote.append({
                '1 code':code,
                '2 eval date':eval_date,
                '3 empty days':cont,
                '4 vol':vol,
                '5 spot price':spot_price,
                '6 option price 1M (%)':option_price1,
                '7 option price 2M (%)':option_price2,
                '8 option price 3M (%)':option_price3,
                '9 option price 6M (%)': option_price4
            })
            # print('%20s %20s %20s %20s %20s %20s %20s %20s' %(code,eval_date,cont,round(vol,4),spot_price,round(option_price1,4),round(option_price2,4),round(option_price3,4)))
    except Exception as e:
        print(code,' : ',e)

df_histvols = pd.DataFrame(hist_vols).set_index(['code'])
df_histvols.to_excel(url+'hist_vols_'+end_date.strftime('%Y%m%d')+'.xls')
df_quote = pd.DataFrame(res_quote)
df_quote.to_excel(url+'otc_quotes_'+end_date.strftime('%Y%m%d')+'.xls')

df_recheck = pd.DataFrame({'code_errors':recheck})
df_recheck.to_excel(url+'recheck.xls')

res_eval = []
# OTC Option Evaluation
df_eval = pd.read_excel(url+evaluation_date.strftime('%Y%m%d')+'_场外期权估值表.xlsx')
df_eval['code'] = df_eval['标的'].apply(lambda x: x[-6:])
print(df_eval)
for (idx,row) in df_eval.iterrows():
    code = row['标的'].replace(" ", "")[-6:]
    # vol = df_histvols[code]
    strike = row['行权价格']
    mdt = row['终止日期']
    # notional = row['名义本金']
    # df = df_histvols.loc[[code]]
    spot_price = df_histvols.at[code,'spot_price']
    vol = df_histvols.at[code,'vol']
    quote = otc_quote(end_date, spot_price, strike, vol, mdtDate=mdt)
    option_price = quote/spot_price
    res_eval.append({'code':code,'vol':vol,'option price':option_price})
df_eval_res = pd.DataFrame(res_eval)
df_eval_res.to_excel(url+evaluation_date.strftime('%Y%m%d')+'_场外期权估值表_results.xlsx',sheet_name='results')
