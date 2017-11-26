#!/usr/bin/python3
#import  os, sys, pandas as pd
import os, sys

sys.path.append(os.path.expanduser('~') +'/bin/lib')
import my.acc_rec as acc_rec

pvt = acc_rec.PT()

#class dt(acc_rec.pt):
#    def __init__(self, *args):
#        acc_rec.PT.__init__(self, *args)
#    pass


#bring (filter) functions to global scope
#ff_ = acc_rec.F

#class pf contains 'presentation' functons
#class fp_:
#    def print_all(data, rows=None):
#        with pd.option_context("display.max_rows", rows):
#            print(data)

def _setup_data():
    def_input_data_file = os.path.expanduser("~") + "/Downloads/00/export.txt"
    ###
    def_piv_index = ['source', 'sub2']
    def_piv_columns = 'ccenter'
    def_piv_values = 'usd'
    #
    pvt.import_data(def_input_data_file, 'windows-1251')
    #dt.import_data(def_input_data_file, 'windows-1251')
    #
    settings = dict(
        index   = def_piv_index,
        columns = def_piv_columns,
        values  = def_piv_values)

    pvt.get_pivot('default', settings = settings)

_setup_data()

if __name__ == "main":
    print('i am in')
    pass
