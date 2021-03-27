import pandas as pd
import beancount as bn
from beancount import loader

import lib_py.bn_count.bean_pandas


def get_transactions(file_name):
    tr = pd.read_excel(file_name)
    return tr

def load_beancount_data_file(file_name):
    bn_entries, bn_errors, bn_options_map
    entries, errors, options_map =  loader.load_file(file_name)
    return entries

def get_new_transactions(securities_df, bean_file):
    """generates new transactions for existing <bean_file> database
    for records in securities_df.

    args:
      - securities_df: standard secuirities transactions report, prepared by
        EDR bank in xlsx format. Must be of certain structure with columns:
        - order               <integer>
        - quantity            <float>
        - ISIN                <string> security ID
        - asset               <string> narration
        - 'avalog asset type' <string> asset type classification
        - description         <str>
        - currency            <str>
        - 'trade date'        <str> ?
        - price               <float>
     - bean_file: path to beancount data file
    """
    bn_entries = load_beancount_data_file(bean_file)

    #get report on securities balances in existing database

