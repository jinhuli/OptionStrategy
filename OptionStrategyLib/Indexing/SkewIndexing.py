from data_access.get_data import get_50option_mktdata as option_data
from back_test.BktOptionStrategy import BktOptionStrategy
import datetime
import pandas as pd
class SkewIndexing(BktOptionStrategy):

    def __init__(self,start_date,end_date,min_holding):
        df_metrics = option_data(start_date,end_date)
        BktOptionStrategy.__init__(self, df_metrics)
        self.set_min_holding_days(min_holding)

    def select_eligible_contracts(self,df_data):
        if df_data.empty: return
        df_metrics = self.util.get_duplicate_strikes_dropped(df_data)
        # TODO: add other criterion
        return df_metrics

    def skew(self,eval_date):
        df_daily_state = self.bkt_optionset.df_data[self.bkt_optionset.df_data[self.util.col_date]==eval_date].reset_index(drop=True)
        mdt1 = self.get_1st_eligible_maturity(eval_date)
        mdt2 = self.get_2nd_eligible_maturity(eval_date)
        df_mdt1 = self.select_eligible_contracts(self.util.get_df_by_mdt(df_daily_state,mdt1))
        df_mdt2 = self.select_eligible_contracts(self.util.get_df_by_mdt(df_daily_state,mdt2))
        print(df_mdt1)


start_date = datetime.date(2017,10,1)
end_date = datetime.date(2017,12,12)
skew_indexing = SkewIndexing(start_date,end_date,8)
skew_indexing.skew(datetime.date(2017,10,19))