from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from mpl_toolkits.mplot3d import Axes3D
import matplotlib as mpl
from matplotlib import cm as plt_cm
import datetime
import pandas as pd
from WindPy import w
from data_access.db_tables import DataBaseTables as dbt
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
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
evalDate = datetime.date(2018, 5, 18)
startDate = datetime.date(2017, 1, 1)
hist_date = w.tdaysoffset(-7, startDate, "Period=M").Data[0][0].date()
index_ids = ['index_300sh','index_50sh','index_500sh']
histvols_3M = []
realizedvols = []
dates = []
mergedvix_df = pd.DataFrame()
query2_1 = sess2.query(IndexMkt.id_instrument, IndexMkt.dt_date, IndexMkt.amt_close) \
        .filter(IndexMkt.dt_date >= startDate) \
        .filter(IndexMkt.dt_date <= evalDate) \
        .filter(IndexMkt.id_instrument == 'index_cvix')

for indexid in index_ids:

    query1 = sess1.query(EquityIndexIntraday.id_instrument,
                         EquityIndexIntraday.dt_datetime,
                         EquityIndexIntraday.amt_close) \
        .filter(EquityIndexIntraday.dt_datetime >= startDate) \
        .filter(EquityIndexIntraday.dt_datetime <= evalDate) \
        .filter(EquityIndexIntraday.id_instrument == indexid) \
        .filter(EquityIndexIntraday.datasource == 'wind')

    query2 = sess2.query(IndexMkt.id_instrument, IndexMkt.dt_date, IndexMkt.amt_close) \
        .filter(IndexMkt.dt_date >= hist_date) \
        .filter(IndexMkt.dt_date <= evalDate) \
        .filter(IndexMkt.id_instrument == indexid)

    index_df = pd.read_sql(query2.statement, query2.session.bind)

    index_df['histvol_120'] = calculate_histvol(index_df['amt_close'],120)
    index_df['histvol_60'] = calculate_histvol(index_df['amt_close'],60)
    index_df['histvol_20'] = calculate_histvol(index_df['amt_close'],20)
    index_df['histvol_10'] = calculate_histvol(index_df['amt_close'],10)
    index_df['histvol_5'] = calculate_histvol(index_df['amt_close'],5)

    merged_df = index_df
    dates = merged_df['dt_date'].tolist()

    vol_set = [ merged_df['histvol_20'].tolist(),
                merged_df['histvol_60'].tolist(),
                merged_df['histvol_120'].tolist()]
    print(dates[-1],indexid,' histvol_120 : ',merged_df['histvol_120'].tolist()[-1])
    print(dates[-1],indexid,' histvol_60 : ',merged_df['histvol_60'].tolist()[-1])
    print(dates[-1],indexid,' histvol_20 : ',merged_df['histvol_20'].tolist()[-1])
    f2, ax2 = plt.subplots()
    ldgs = [ '历史波动率1M', '历史波动率3M','历史波动率6M']
    for cont2, y in enumerate(vol_set):
        pu.plot_line(ax2, cont2, dates, y, ldgs[cont2], '日期', '波动率（%）')
    ax2.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
               ncol=5, mode="expand", borderaxespad=0.)
    for tick in ax2.get_xticklabels():
        tick.set_rotation(90)
    f2.set_size_inches((14, 6))
    f2.savefig('../save_figure/otc_realizedvol_' + indexid + '_' + str(startDate) + ' - ' + str(evalDate) + '.png',
               dpi=300, format='png')
    histvols_3M.append(merged_df['histvol_60'].tolist())
    merged_df.to_csv('../save_figure/index_vols'+indexid+'.csv')



plt.show()





