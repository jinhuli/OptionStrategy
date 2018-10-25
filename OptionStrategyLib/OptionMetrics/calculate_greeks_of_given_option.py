from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_option import BaseOption
from data_access.get_data import get_50option_mktdata,get_comoption_mktdata
import back_test.model.constant as c
import datetime
import pandas as pd
from PricingLibrary.EngineQuantlib import QlBlackFormula, QlBinomial
import Utilities.admin_write_util as admin

start_date = datetime.date(2018, 3, 1)
end_date = datetime.date(2018,4,1)

df_metrics = get_comoption_mktdata(start_date, end_date,c.Util.STR_SR)
df_metrics1 = df_metrics[df_metrics[c.Util.ID_INSTRUMENT]=='sr_1805_c_5800'].reset_index(drop=True)
df_metrics2 = df_metrics[df_metrics[c.Util.ID_INSTRUMENT]=='sr_1805_p_5800'].reset_index(drop=True)
baseoption_call = BaseOption(df_metrics1,df_metrics1)
baseoption_put = BaseOption(df_metrics2,df_metrics2)
baseoption_call.init()
baseoption_put.init()
res = []
while not baseoption_call.is_last():
    iv_call = baseoption_call.get_implied_vol()
    delta_call = baseoption_call.get_delta(iv_call)
    iv_put = baseoption_put.get_implied_vol()
    delta_put = baseoption_put.get_delta(iv_put)
    res.append({'dt_date':baseoption_call.eval_date,'iv_call':iv_call,
                'iv_put':iv_put,'delta_call':delta_call,'delta_put':delta_put,
                'price_call':baseoption_call.mktprice_close(),'price_put':baseoption_put.mktprice_close(),
                'price_underlying':baseoption_call.underlying_close()})
    baseoption_call.next()
    baseoption_put.next()

df_res = pd.DataFrame(res)
df_res.to_csv('../../data/delta.csv')