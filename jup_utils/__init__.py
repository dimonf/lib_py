import pandas as pd
from .load_data_1c import get_data_set as get_data_set_for_1c
#from . import _dtct as dtct

def tb_map(s, date_begin, date_end):
    ''' s - series for each row in a dataframe
        date_from - beginning of period of trial balance, datetime
        date_to - end of the period, datetime
        #
        USAGE:
        a = df.apply(tb_map, axis=1, date_begin=20180101, date_end=20181231)
        df.groupby(a).sum().unstack(0)
        #
    '''
    #convert date arguments into timestamp
    #d_bf, d_cf = [pd.to_datetime(x) for x in [date_bf, date_cf]]

    if s['date'] < date_begin:
        return 'bf'
    elif s['date'] > date_end:
        return 'cf'
    elif s['usd'] > 0:
        return 'dt'
    else:
        return 'ct'

def t(time):
    '''
    converts integer/string value into datetime
    '''
    return pd.to_datetime(str(time))
