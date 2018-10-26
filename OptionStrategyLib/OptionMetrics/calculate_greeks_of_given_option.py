from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_option import BaseOption
from data_access.get_data import get_50option_mktdata,get_comoption_mktdata
import back_test.model.constant as c
import datetime
import pandas as pd
from PricingLibrary.EngineQuantlib import QlBlackFormula, QlBinomial
import Utilities.admin_write_util as admin

start_date = datetime.date(2018, 9, 1)
end_date = datetime.date(2018,10,25)

# df_metrics = get_comoption_mktdata(start_date, end_date,c.Util.STR_M)
df_metrics = get_50option_mktdata(start_date, end_date)
df_metrics1 = df_metrics[(df_metrics[c.Util.ID_INSTRUMENT]=='50etf_1810_c_2.5')&(df_metrics[c.Util.DT_DATE]>=datetime.date(2018,9,27))].reset_index(drop=True)
df_metrics2 = df_metrics[(df_metrics[c.Util.ID_INSTRUMENT]=='50etf_1811_c_2.5')&(df_metrics[c.Util.DT_DATE]>=datetime.date(2018,9,27))].reset_index(drop=True)
baseoption_call = BaseOption(df_metrics1,df_metrics1)
baseoption_put = BaseOption(df_metrics2,df_metrics2)
baseoption_call.init()
baseoption_put.init()
res = []
while not baseoption_put.is_last():
    iv_call = baseoption_call.get_implied_vol()
    delta_call = baseoption_call.get_delta(iv_call)
    iv_put = baseoption_put.get_implied_vol()
    delta_put = baseoption_put.get_delta(iv_put)
    dic = {'dt_date':[baseoption_call.eval_date,baseoption_put.eval_date],'iv_call':iv_call,
                'iv_put':iv_put,'delta_call':delta_call,'delta_put':delta_put,
                'price_call': baseoption_call.mktprice_close(), 'price_put': baseoption_put.mktprice_close(),
                'price':baseoption_call.mktprice_close()-baseoption_put.mktprice_close(),
                'call_underlying':baseoption_call.underlying_close(),'put_underlying':baseoption_put.underlying_close()}
    print(dic)
    res.append({'dt_date':baseoption_call.eval_date,'iv_call':iv_call,
                'iv_put':iv_put,'delta_call':delta_call,'delta_put':delta_put,
                'price_call':baseoption_call.mktprice_close(),'price_put':baseoption_put.mktprice_close(),
                'price_underlying':baseoption_call.underlying_close()})
    baseoption_call.next()
    baseoption_put.next()

df_res = pd.DataFrame(res)
# df_res.to_csv('../../data/delta.csv')