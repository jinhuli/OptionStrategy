from back_test.bkt_strategy import BktOptionStrategy
import QuantLib as ql
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
import datetime
from back_test.bkt_util import BktUtil
from back_test.data_option import get_50option_mktdata as get_mktdata


class BktStrategyMoneynessVol(BktOptionStrategy):
    def __init__(self, df_option_metrics, money_utilization=0.2, init_fund=100000000.0, tick_size=0.0001,
                 fee_rate=2.0 / 10000, nbr_slippage=0, max_money_utilization=0.5):

        BktOptionStrategy.__init__(self, df_option_metrics, money_utilization, init_fund, tick_size,
                                   fee_rate, nbr_slippage, max_money_utilization)

    def get_ivs(self,moneyness):
        dates = []
        implied_vols = []
        res = []
        bkt_optionset = self.bkt_optionset
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            evalDate = bkt_optionset.eval_date
            print(evalDate)
            if evalDate == bkt_optionset.end_date:
                break
            df_call = bkt_optionset.get_call(moneyness, self.get_1st_eligible_maturity(evalDate))
            bktoption = df_call.loc[0, self.util.bktoption]
            bktoption.update_implied_vol()
            iv = bktoption.implied_vol
            implied_vols.append(iv)
            dates.append(evalDate)
            res.append({
                'dt_date':evalDate,
                'id_underlying':'index_50etf',
                'cd_mdt':'hp_5_1st',
                'cd_moneyness':0,
                'pct_implies_vol':iv
            })
            # print(evalDate, ' , ', iv)  # npv是组合净值，期初为1
            bkt_optionset.next()
        df = pd.DataFrame(res)
        return df,res


"""Back Test Settings"""
start_date = datetime.date(2015, 6, 19)
end_date = datetime.date(2017, 12, 31)
calendar = ql.China()
daycounter = ql.ActualActual()
util = BktUtil()
open_trades = []
engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
conn = engine.connect()
metadata = MetaData(engine)
table = Table('option_iv_by_moneyness', metadata, autoload=True)

"""Collect Mkt Date"""

df_option_metrics = get_mktdata(start_date, end_date)

"""Run Backtest"""

bkt = BktStrategyMoneynessVol(df_option_metrics)
bkt.set_min_ttm(5) # 期权到期时间至少5个工作日
df,res = bkt.get_ivs(0)
print(df)
for r in res:
    try:
        conn.execute(table.insert(), r)
    except Exception as e:
        print(e)
        print(r)
        continue

# plt.plot(dates,implied_vols)
# plt.show()