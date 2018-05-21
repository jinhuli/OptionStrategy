import numpy as np

def calculate_histvol(data,n):
    datas=np.log(data)
    df=datas.diff()
    vol=df.rolling(window = n).std()*np.sqrt(252)
    return vol
