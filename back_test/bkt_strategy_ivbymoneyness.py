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
    def __init__(self, df_option_metrics):

        BktOptionStrategy.__init__(self, df_option_metrics)

    def get_ivs_mdt1(self,moneyness):
        res = []
        bkt_optionset = self.bkt_optionset
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            evalDate = bkt_optionset.eval_date
            # print(evalDate)
            if evalDate == bkt_optionset.end_date:
                break
            mdt = self.get_1st_eligible_maturity(evalDate)
            df_call = bkt_optionset.get_call(moneyness, mdt)
            df_put = bkt_optionset.get_put(moneyness, mdt)
            if not df_call.empty:
                bktoption = df_call.loc[0, self.util.bktoption]
                bktoption.update_implied_vol()
                iv_call = round(bktoption.implied_vol,8)
                p_call = round(bktoption.option_price,8)
                res.append({
                    'dt_date':evalDate,
                    'id_underlying':'index_50etf',
                    'cd_option_type':'call',
                    'cd_mdt':'hp_5_1st',
                    'cd_moneyness':moneyness,
                    'pct_implies_vol': iv_call,
                    'amt_option_price': float(p_call)
                })
            if not df_put.empty:
                bktoption1 = df_put.loc[0, self.util.bktoption]
                bktoption1.update_implied_vol()
                iv_put = round(bktoption1.implied_vol,8)
                p_put = round(bktoption1.option_price,8)
                res.append({
                    'dt_date':evalDate,
                    'id_underlying':'index_50etf',
                    'cd_option_type':'put',
                    'cd_mdt':'hp_5_1st',
                    'cd_moneyness':moneyness,
                    'pct_implies_vol':iv_put,
                    'amt_option_price':float(p_put)
                })
            # print(evalDate,mdt,iv_call,iv_put)
            bkt_optionset.next()
        return res

    def get_ivs_keyvols(self):
        res = []
        bkt_optionset = self.bkt_optionset
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            evalDate = bkt_optionset.eval_date

            iv_call = bkt_optionset.get_atmvols_1M('call')
            iv_put = bkt_optionset.get_atmvols_1M('put')
            res.append({
                'dt_date': evalDate,
                'id_underlying': 'index_50etf',
                'cd_option_type': 'call',
                'cd_mdt': '1M',
                'cd_moneyness': 0,
                'pct_implies_vol': iv_call,
            })
            res.append({
                'dt_date': evalDate,
                'id_underlying': 'index_50etf',
                'cd_option_type': 'put',
                'cd_mdt': '1M',
                'cd_moneyness': 0,
                'pct_implies_vol': iv_put,
            })
            print(evalDate,iv_call)
            if evalDate == bkt_optionset.end_date:
                break
            bkt_optionset.next()
        return res

"""Back Test Settings"""
start_date = datetime.date(2018, 1, 8)
end_date = datetime.date(2018, 4, 13)
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
res = bkt.get_ivs_keyvols()
# res = bkt.get_ivs_mdt1(0)
df = pd.DataFrame(res)
print(df)
for r in res:
    query_res = table.select((table.c.id_underlying == r['id_underlying'])
                               & (table.c.dt_date == r['dt_date'])& (table.c.cd_option_type == r['cd_option_type'])
                             & (table.c.cd_mdt == r['cd_mdt'])& (table.c.cd_moneyness == r['cd_moneyness'])).execute()
    if query_res.rowcount > 0:
        table.delete((table.c.id_underlying == r['id_underlying'])
                               & (table.c.dt_date == r['dt_date'])& (table.c.cd_option_type == r['cd_option_type'])
                             & (table.c.cd_mdt == r['cd_mdt'])& (table.c.cd_moneyness == r['cd_moneyness'])).execute()
    try:
        conn.execute(table.insert(), r)
    except Exception as e:
        print(e)
        print(r)
        continue

# plt.plot(dates,implied_vols)
# plt.show()