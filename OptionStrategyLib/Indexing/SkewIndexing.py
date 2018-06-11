from data_access.get_data import get_50option_mktdata as option_data
from back_test.BktUtil import BktUtil

class SkewIndexing(object):


    def __init__(self,start_date,end_date):
        self.util = BktUtil()
        self.start_date = start_date
        self.end_date = end_date
        self.df_metrics = option_data(start_date,end_date)

    def select_eligible_contracts(self,eval_date):
        df_daily_state = self.df_metrics[self.df_metrics[self.util.col_date]==eval_date].reset_index(drop=True)
        df_metrics = self.util.get_duplicate_strikes_dropped(df_daily_state)
