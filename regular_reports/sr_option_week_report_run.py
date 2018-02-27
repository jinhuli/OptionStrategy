from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from mpl_toolkits.mplot3d import Axes3D
import matplotlib as mpl
from mpl_toolkits.axes_grid1 import host_subplot
from matplotlib.dates import date2num
from matplotlib import cm as plt_cm
import datetime
import pandas as pd
import numpy as np
from WindPy import w
from data_access.db_tables import DataBaseTables as dbt
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
import QuantLib as ql
from regular_reports.sr_option_week_report import sr_hist_atm_ivs,sr_implied_vol_analysis,sr_pcr_analysis


###################################################################################
w.start()
dt_date = datetime.date(2018, 2, 23)  # Set as Friday
dt_last_week = datetime.date(2018, 2, 9)

evalDate = dt_date.strftime("%Y-%m-%d")
sr_implied_vol_analysis(evalDate,w)
sr_hist_atm_ivs(evalDate,w)
sr_pcr_analysis(dt_date,dt_last_week,w)
plt.show()
