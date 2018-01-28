from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import datetime
import pandas as pd
from data_access.db_tables import DataBaseTables as dbt
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil

start_date = datetime.date(2015, 3, 31)
# start_date = datetime.date(2016, 6, 1)
# end_date = datetime.date(2017, 12, 1)
end_date = datetime.date(2017, 12, 31)

pu = PlotUtil()
plt.rcParams['font.sans-serif'] = ['STKaiti']
plt.rcParams.update({'font.size': 13})
engine2 = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
metadata2 = MetaData(engine2)
Session2 = sessionmaker(bind=engine2)
sess2 = Session2()
IndexMkt = dbt.IndexMkt


query_vix = sess2.query(IndexMkt.id_instrument, IndexMkt.dt_date, IndexMkt.amt_close) \
    .filter(IndexMkt.dt_date >= start_date) \
    .filter(IndexMkt.dt_date <= end_date) \
    .filter(IndexMkt.id_instrument == 'index_cvix')

df_vix = pd.read_sql(query_vix.statement,query_vix.session.bind)

fig = pu.plot_line_chart(df_vix['dt_date'].tolist(),[df_vix['amt_close'].tolist()],['vix'])
plt.show()