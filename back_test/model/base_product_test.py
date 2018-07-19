from back_test.model.base_product import BaseProduct
from data_access.get_data import get_index_intraday

import datetime

"""Back Test Settings"""
# start_date = datetime.date(2015, 3, 1)
start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2017, 11, 1)

"""Collect Mkt Date"""

df_index_metrics = get_index_intraday(start_date, end_date, 'index_50etf')
product = BaseProduct(df_index_metrics)
product.execute_order(None)