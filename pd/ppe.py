from datetime import datetime
import pandas as pd


######################################## 'one file' design ###########################
### columns 'ccenter', 'cat' and 'details' are filled only once for any item, identified by 'id'
### transaction types:
###   dd: depereciation charge
###   va: value adjustment
###   da: depreciation adjustment
###   vd: disposal of FA item
###   vq: acquisition of FA item
file_ppe = 'data/ppe.csv'


class Ppe:
    cols_ppe =  ['id','date','curr','amount','usd','type','ccenter','cat','details']
    def __init__(self, df):
        if isinstance(df, str):
            #read the file
            self.df_tr = pd.read_csv(df, index_col=0, 
                dtype={'cat':str, 'details':str, 'ccenter':str},
                parse_dates=['date'])
        elif isinstance(df, pd.DataFrame):
            self.df_tr = df.copy()

        self.check_data_structure()

    def check_data_structure(self):
        if set(self.cols_ppe) != set(self.df_tr.columns):
            raise TypeError('Columns do not conform to specification')



    def get_depr(df, data_from, data_to=None, periods=None):
        ''' calculate depreciation for given period.
             df is expected in predetermined format
             periods = [None,'M','Y']'''
       #create 'working' dataframe with
