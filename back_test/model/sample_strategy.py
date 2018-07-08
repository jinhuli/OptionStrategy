from back_test.BktOptionStrategy import BktOptionStrategy, BktOptionIndex
from back_test.BktInstrument import BktInstrument
import numpy as np
import datetime
from data_access.get_data import get_50option_mktdata, get_index_ma, get_index_mktdata, get_pciv_ratio


class SampleStrategy(BktOptionStrategy):

    def __init__(self, df_option, money_utilization=0.2, init_fund=100000.0,
                 cash_reserve_pct=0.20):
        BktOptionStrategy.__init__(self, df_option, money_utilization=money_utilization,
                                   init_fund=init_fund)
        self.cash_reserve_pct = cash_reserve_pct

    def run(self):
        bkt_optionset = self.bkt_optionset
        bkt_account = self.bkt_account
        inv_fund = (1 - self.cash_reserve_pct) * bkt_account.cash
        evalDate = bkt_optionset.eval_date

        """Option: Select Strategy and Open Position"""
        # 投资什么条件的期权，这里是第一个到期日，平值
        maturity_date = self.get_1st_eligible_maturity(evalDate)
        moneyness = 0

        # 根基上述条件选出期权投资组合
        portfolio = self.bkt_optionset.get_call(moneyness,maturity_date,self.util.long)
        # 根据现在有多少钱，分配投资组合中的每个期权的持仓unit
        bkt_account.portfolio_rebalancing_eqlfund(portfolio, 0.0, cd_open_by_price='close', fund=inv_fund)
        # 开仓
        bkt_account.open_portfolio(evalDate, portfolio)

        self.flag_trade = True
        bkt_account.mkm_update_portfolio(evalDate, portfolio)
        print(evalDate, bkt_optionset.eval_date, ' , ', bkt_account.npv)

        while bkt_optionset.index < len(bkt_optionset.dt_list):
            bkt_optionset.next()
            evalDate = bkt_optionset.eval_date

            """ STRATEGY : HOLD TO MATURITY """
            if evalDate == maturity_date:
                bkt_account.close_portfolio(evalDate,portfolio,cd_close_by_price='close')
                bkt_account.mkm_update_portfolio(evalDate, portfolio)
                print(evalDate, ' , ', bkt_account.npv)  # npv是组合净值，期初为1
                break
            """按当日价格调整保证金，计算投资组合盯市价值"""
            bkt_account.mkm_update_portfolio(evalDate, portfolio)
            print(evalDate, ' , ', bkt_account.npv)


"""Back Test Settings"""
# start_date = datetime.date(2015, 3, 1)
start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2018, 1, 21)

"""Collect Mkt Date"""

df_option_metrics = get_50option_mktdata(start_date, end_date)
df_index_metrics = get_index_mktdata(start_date, end_date, 'index_50etf')
"""Run Backtest"""

bkt_strategy = SampleStrategy(df_option_metrics, df_index_metrics)
bkt_strategy.set_min_holding_days(8)

bkt_strategy.run()
