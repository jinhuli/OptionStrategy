import pandas as pd

from back_test.deprecated.BktOptionStrategy import BktOptionStrategy


class BktStrategyMoneynessVol(BktOptionStrategy):
    def __init__(self, df_option_metrics):
        BktOptionStrategy.__init__(self, df_option_metrics)

    """Get ATM volatility by 1st maturity，(ATM is actually in the OTM part)"""
    def ivs_mdt1_run(self):
        res = []
        bkt_optionset = self.bkt_optionset
        df_ivs = pd.DataFrame()
        cd_underlying_price = 'close'
        cd_mdt = 'hp_8_1st' # TODO : change cd_mdt
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            evalDate = bkt_optionset.eval_date
            call_atm = self.bkt_optionset.get_call(
                0, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0]
            put_atm = self.bkt_optionset.get_put(
                0, self.get_1st_eligible_maturity(evalDate),self.util.long,cd_underlying_price=cd_underlying_price).optionset[0]
            iv_call = call_atm.get_implied_vol()
            iv_put = put_atm.get_implied_vol()
            # res.append({'dt_date': evalDate, 'id_underlying': 'index_50etf', 'cd_option_type': 'call',
            #     'cd_mdt': cd_mdt, 'cd_moneyness': 0, 'pct_implies_vol': iv_call,
            #     'amt_option_price':float(call_atm.option_price()), 'amt_strike':float(call_atm.strike()),
            #     'amt_adj_strike':float(call_atm.adj_strike()),
            #     'amt_underlying_price': float(call_atm.underlying_close())})
            # res.append({'dt_date': evalDate, 'id_underlying': 'index_50etf', 'cd_option_type': 'put',
            #     'cd_mdt': cd_mdt, 'cd_moneyness': 0, 'pct_implies_vol': iv_put,
            #     'amt_option_price':float(put_atm.option_price()), 'amt_strike':float(put_atm.strike()),
            #     'amt_adj_strike':float(put_atm.adj_strike()),
            #     'amt_underlying_price': float(put_atm.underlying_close())})
            res.append({
                'dt_date': evalDate,
                'id_underlying': 'index_50etf',
                'cd_mdt': cd_mdt,
                'cd_moneyness': 0,
                'pct_iv_call': iv_call,
                'pct_iv_put': iv_put,
                'amt_price_call':float(call_atm.option_price()),
                'amt_price_put':float(put_atm.option_price()),
                'amt_strike_call':float(call_atm.strike()),
                'amt_strike_put':float(put_atm.strike()),
                'amt_strike_adj_call':float(call_atm.adj_strike()),
                'amt_strike_adj_put':float(put_atm.adj_strike()),
                'amt_underlying_price': float(call_atm.underlying_close()),
                'dt_maturity':call_atm.maturitydt()
                })
            df_ivs = df_ivs.append(pd.DataFrame(data={
                'dt_date': [evalDate],
                'id_underlying': ['index_50etf'],
                'cd_mdt': [cd_mdt],
                'cd_moneyness': [0],
                'pct_iv_call': [iv_call],
                'pct_iv_put': [iv_put],
                'amt_price_call':[float(call_atm.option_price())],
                'amt_price_put':[float(put_atm.option_price())],
                'amt_strike_call':[float(call_atm.strike())],
                'amt_strike_put':[float(put_atm.strike())],
                'amt_strike_adj_call':[float(call_atm.adj_strike())],
                'amt_strike_adj_put':[float(put_atm.adj_strike())],
                'amt_underlying_price': [float(call_atm.underlying_close())],
                'dt_maturity':[call_atm.maturitydt()]
                }),ignore_index=True)
            if bkt_optionset.index >= len(bkt_optionset.dt_list)-1:break
            bkt_optionset.next()
        df_ivs.to_csv('../save_results/df_ivs_total.csv')
        return res

    """ Get interpolated 1M volatility """
    def get_ivs_keyvols(self):
        res = []
        bkt_optionset = self.bkt_optionset
        while bkt_optionset.index < len(bkt_optionset.dt_list):
            evalDate = bkt_optionset.eval_date
            iv_call = bkt_optionset.get_atmvols_1M('call')
            iv_put = bkt_optionset.get_atmvols_1M('put')
            res.append({
                'dt_date': evalDate, 'id_underlying': 'index_50etf', 'cd_option_type': 'call',
                'cd_mdt': '1M', 'cd_moneyness': 0, 'pct_implies_vol': iv_call
            })
            res.append({
                'dt_date': evalDate, 'id_underlying': 'index_50etf', 'cd_option_type': 'put',
                'cd_mdt': '1M', 'cd_moneyness': 0, 'pct_implies_vol': iv_put
            })
            # print(evalDate,iv_call,iv_put)
            if evalDate == bkt_optionset.end_date:
                break
            bkt_optionset.next()
        return res

# """Back Test Settings"""
# start_date = datetime.date(2015, 1, 1)
# # start_date = datetime.date(2018, 1, 1)
# end_date = datetime.date.today()
# calendar = ql.China()
# daycounter = ql.ActualActual()
# util = BktUtil()
# open_trades = []
# engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
# conn = engine.connect()
# metadata = MetaData(engine)
# table = Table('option_iv_by_moneyness', metadata, autoload=True)
#
# """Collect Mkt Date"""
# df_option_metrics = get_mktdata(start_date, end_date)
#
# """Run Backtest"""
# bkt = BktStrategyMoneynessVol(df_option_metrics)
# bkt.set_min_holding_days(8) # TODO: 选择期权最低到期日
# df_res = bkt.ivs_mdt1_run()
# # df = pd.DataFrame(res)
# print(df_res)
# df_res.to_sql(name='option_atm_iv', con=engine, if_exists = 'append', index=False)

# for r in res:
#     query_res = table.select((table.c.id_underlying == r['id_underlying'])
#                                & (table.c.dt_date == r['dt_date'])& (table.c.cd_option_type == r['cd_option_type'])
#                              & (table.c.cd_mdt == r['cd_mdt'])& (table.c.cd_moneyness == r['cd_moneyness'])).execute()
#     if query_res.rowcount > 0:
#         table.delete((table.c.id_underlying == r['id_underlying'])
#                                & (table.c.dt_date == r['dt_date'])& (table.c.cd_option_type == r['cd_option_type'])
#                              & (table.c.cd_mdt == r['cd_mdt'])& (table.c.cd_moneyness == r['cd_moneyness'])).execute()
#     try:
#         conn.execute(table.insert(), r)
#     except Exception as e:
#         print(e)
#         print(r)
#         continue

