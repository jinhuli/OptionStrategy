from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from mpl_toolkits.mplot3d import Axes3D
import matplotlib as mpl
from matplotlib import cm as plt_cm
import datetime
import pandas as pd
import numpy as np
from WindPy import w
from data_access.db_tables import DataBaseTables as dbt
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
import QuantLib as ql
from Utilities.calculate import calculate_histvol


###########################################################################################
w.start()
pu = PlotUtil()
plt.rcParams['font.sans-serif'] = ['STKaiti']
plt.rcParams.update({'font.size': 13})
engine1 = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/mktdata_intraday', echo=False)
engine2 = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/mktdata', echo=False)
metadata1 = MetaData(engine1)
Session1 = sessionmaker(bind=engine1)
sess1 = Session1()
metadata2 = MetaData(engine2)
Session2 = sessionmaker(bind=engine2)
sess2 = Session2()
index_intraday = Table('equity_index_mktdata_intraday', metadata1, autoload=True)
EquityIndexIntraday = dbt.EquityIndexIntraday
IndexMkt = dbt.IndexMkt
############################################################################################
# Eval Settings
# evalDate = datetime.date(2018, 1, 30)
# startDate = datetime.date(2016, 1, 1)
indexid = 'index_50sh'
#############################################################################################
# query2_1 = sess2.query(IndexMkt.id_instrument, IndexMkt.dt_date, IndexMkt.amt_close) \
#     .filter(IndexMkt.dt_date >= startDate) \
#     .filter(IndexMkt.dt_date <= evalDate) \
#     .filter(IndexMkt.id_instrument == 'index_cvix')
# cvix_df = pd.read_sql(query2_1.statement, query2_1.session.bind)


# query1 = sess1.query(EquityIndexIntraday.id_instrument,
#                      EquityIndexIntraday.dt_datetime,
#                      EquityIndexIntraday.amt_close) \
#     .filter(EquityIndexIntraday.dt_datetime >= startDate) \
#     .filter(EquityIndexIntraday.dt_datetime <= evalDate) \
#     .filter(EquityIndexIntraday.id_instrument == indexid) \
#     .filter(EquityIndexIntraday.datasource == 'wind')

query2 = sess2.query(IndexMkt.id_instrument, IndexMkt.dt_date, IndexMkt.amt_close) \
    .filter(IndexMkt.id_instrument == indexid)\
    # .filter(IndexMkt.dt_date>=startDate)

# intraday_df = pd.read_sql(query1.statement, query1.session.bind)
index_df = pd.read_sql(query2.statement, query2.session.bind)

# for i in range(len(intraday_df)):
#     intraday_df.loc[i, 'dt_date'] = intraday_df.loc[i, 'dt_datetime'].date()

index_df['histvol_1M'] = list(calculate_histvol(index_df['amt_close'], 21) * 100)
index_df['histvol_3M'] = list(calculate_histvol(index_df['amt_close'], 63) * 100)

index_df.to_csv('../save_results/index50sh_hist_vol.csv')
