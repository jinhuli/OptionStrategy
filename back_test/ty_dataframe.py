import pandas as pd

import datetime
# df1 = pd.DataFrame({
#     'a': [1,2,3,4,5,],
#     'b': [1,2,3,4,5,],
#     'c': [1,2,3,4,5,],
#      'd': [7,6,4,8,5,]
# })
# df1= df1.sort_values(by=['d'],ascending=True)
# print(df1)
# print(df1.at[1,'a'])
# print(df1.iat[1,1])

d = datetime.date(2018,1,1)-datetime.date(1999,1,1)
print(d.days)
