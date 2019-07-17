#this script is designed to be sourced from a jupyter notebook via %run

'''
master file is updated for changes in 1c accounting system only. In 1c there is
a 'constant' which holds the date, at which the previous data dump is still valid.
For new transaction the value of this constant is replaced with the transaction date.
As new data dump precedure is done, the constant value holds the date of the last
transaction which made its way into the dump file.
So, the method is not very efficient as it does not trace individual transactions; rather
it includes one single range of the whole 1c transaction's database spanning from the date
of the earliest of amended/new transaction since last data dump till the older, explicitly
specified date (usually, the current date)
'''

import pandas as pd, numpy as np, os, re, shutil, glob

HOME_DIR = os.path.expanduser("~")
IMPORT_DATA_FILE_GLOB = '1c_trx_dump.txt*'
IMPORT_DATA_FILE_NAME = ""
LOCAL_DATA_PATH = os.path.join(HOME_DIR,'jup','_data',"import_1c.txt.gz")
dtypes={n:'str' for n in 'period ccenter account dtct curr'.split()}

class _gen():
    pass

def read_local_data(file_name=LOCAL_DATA_PATH):
    if os.path.isfile(file_name):
        return pd.read_csv(file_name, header=0, index_col = False,
                 dtype=dtypes, parse_dates=['date'])
    else:
        return False

def get_import_data_file_name(glob_path):
    glob_names = glob.glob(glob_path)
    if len(glob_names) > 1:
        raise Exception('there are more than 1 file which satisfy import filename definition:\n {}'.
            format('\n'.join(glob_names))
            )
    elif len(glob_names) == 0:
        return False
    return glob_names[0]

def update_local_data():
    #resolve glob
    import_data_path = get_import_data_file_name(
            os.path.join(HOME_DIR,"Downloads","00", IMPORT_DATA_FILE_GLOB))
    if not import_data_path:
        return None
    #get imported data
    dt_in = pd.read_csv(import_data_path, header=0, sep="\t",
        encoding="windows-1251", dtype=dtypes, parse_dates=['date'])
    #get rid of intermediary transactions of 1C system
    dt_in = dt_in.query('account !="P1"')
    #
    #update local data
    dt_in_min_date = dt_in['date'].min()
    dt_local = read_local_data()
    if dt_local is False:
        dt_out = dt_in
    else:
        dt_out = pd.concat([dt_local[dt_local['date'] < dt_in_min_date], dt_in]
             ,axis=0, ignore_index=True)

    dt_out.to_csv(LOCAL_DATA_PATH, index=False, encoding='utf-8', compression='gzip')
    #move the imported data file out of sight
    backup_file_path = os.path.join(os.path.dirname(import_data_path),
            re.sub(r'([^.]+)(.*)',r'\1_backup\2',os.path.basename(import_data_path))
            )
    shutil.move(import_data_path, backup_file_path)
    #
    return 'updated from '+dt_in_min_date.strftime('%Y-%m-%d')


def read_data():
    '''first check for available updates, merge it with local data, then read and return
       local data
    '''
    updated = update_local_data()
    if updated:
        print(updated)

    dt_local = read_local_data()
    if dt_local is False:
        raise Exception('no data was found at '+ LOCAL_DATA_PATH)

    return dt_local

def get_data_set():
    rset = _gen()
    rset.dt_source = read_data()

    return rset
#del dtypes
