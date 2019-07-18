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

def get_kernel_id():
   '''get kernel ID for use with "jupyter console --existing <ID>"
      This script shall be run within notebook of interest '''
   import ipykernel

   try:
       kernel_id = ipykernel.connect.get_connection_file().split('/')[-1]
   except:
       return('no MultiInstance support')

   #dump ID to predefined file
   with open('/tmp/jup_id','w') as f:
         f.write(kernel_id)
   return kernel_id

def get_closest_dir_by_function(test_function, start_dir=None):
    '''get closest parent directory, which conforms to the test_function.
       test_function must take one positional argument: testing
       directory, in txt fomrat, and return bool True/False'''
    import os

    if not start_dir:
        start_dir = os.getcwd()

    test_dir = start_dir
    target_dir = None

    while not target_dir:
        #print('testing '+ test_dir)
        if test_function(test_dir):
            return test_dir
        next_up_dir = os.path.dirname(test_dir)
        if next_up_dir == test_dir:
            raise Exception('file not found')
        test_dir = next_up_dir

    return target_dir

def get_root_dir():
    '''Get root dir from any notebook which is situated below it.
       The root dir is designated by file root_indicator_file_name'''
    import os

    root_indicator_file_name = '.jupyter_root_dir'
    def test_function(test_dir):
       return os.path.exists(os.path.join(test_dir, root_indicator_file_name))

    return get_closest_dir_by_function(test_function)

def get_git_dir():
    '''Get (nearest) git root directory'''
    import os

    def test_function(test_dir):
        return(os.path.isdir(os.path.join(test_dir, '.git')))

def get_closest_file(name):
    '''get the closest file (or dir) by the name within full path of cwd'''
    import os

    def test_function(test_dir):
        return os.path.exists(os.path.join(test_dir, name))

    target_dir = get_closest_dir_by_function(test_function)
    return os.path.join(target_dir, name)
