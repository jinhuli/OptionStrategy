from Utilities.calculate import calculate_histvol
from Utilities import admin_util as admin
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
import datetime


pu = PlotUtil()
table_index = admin.table_indexes_mktdata()
name_index = 'index_50sh'
start_date = datetime.date(2004,1,1)


query = admin.session_mktdata().query(table_index.c.dt_date, table_index.c.amt_close, table_index.c.id_instrument) \
        .filter(table_index.c.dt_date >= start_date)\
        .filter(table_index.c.id_instrument == name_index)
df = pd.read_sql(query.statement, query.session.bind)
print(len(df))
df = df.dropna()
print(len(df))

histvol = calculate_histvol(df['amt_close'],20)
print(histvol)
pu.plot_line_chart(df['dt_date'],[histvol],['hist vol'])
plt.show()