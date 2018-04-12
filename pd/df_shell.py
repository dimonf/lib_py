''' this module simplifies interactive work with a dataframe.
    it's distinct features are:
        - basic one-line editor (to mangling existing data)
        - parser
        - completion mechanism
    These simplifies data entry/modification. The main intention is
    to make modification of existing dataframe as simple as it is
    with standard spreadsheet gui app.

df = pd.read_csv('export.txt')
sh = my_utils.pd_ext.shell
sh.open(df)
# INSIDE THE SHELL #
10832>columns<ENTER>
    a: date
    b: amount
    c: curr
    d: narr
    e: account
10832>columns comp:a,b,d<ENTER>
10832>exec df[10100:].head(3)[col_ls]<ENTER>
-------------------
             date   amount  curr           details account
10100  2017-12-22  -355.86   EUR      starting bal      24
10101  2017-12-22  2860.07   EUR     Forcon/salary      46
10102  2017-12-27    21.28   EUR  yperagora costas      24
-------------------
#from arg copies modifyable data of existing row (10102) for alteration
10832>append from:10102<ENTER>
       2017-12-22    24.10   EUR   'Kapsalos,VISA'<ENTER>
!POSTED
10833>exec df[10100:].head(4)[col_ls]<ENTER>
-------------------
             date   amount  curr           details
10100  2017-12-22  -355.86   EUR      starting bal
10101  2017-12-22  2860.07   EUR     Forcon/salary
10102  2017-12-27    21.28   EUR  yperagora costas
10100  2017-12-22  -355.86   EUR      starting bal
10833  2017-12-22    24.10   EUR     Kapsalos,VISA
-------------------
'''
import os, cmd, pandas as pd, numpy as np

class DfShell(cmd.Cmd):
    intro = 'Welcome to the dataframe data manipulation shell'
    IFS = '|'

    def __init__(self, df, context=None):
        '''we need context (as locals()) if class is imported. If the code
           run in ipython shell via %run magic, the context arg may be omitted'''
        super().__init__()
        if df.index.dtype != int:
            raise TypeError('only integer indexed df is allowd')
        self.df = df
        self.dtypes = self.df.dtypes
        self.columnes = self.df.columns
        self.context = context
        self.update_prompt()

    def close(self):
        print('bye')

    def update_prompt(self):
        self.prompt = '({0}/{1})'.format(len(self.df),len(self.df.columns))

    def cast_dtypes(self, df):
        for col, t in zip(self.columns, self.dtypes):
            df[col] = df[col].astype(t)

    def format_record(self, str):
        '''parse and format a record for existing dataframe'''
        new_df = None
        values = str.split(self.IFS)
        if len(values) != len(self.df.columns):
            return('number of values provided ' + len(values) +
                  'differs from number of columns '+len(self.df.columns))
        t_df = self.df.iloc[-1,:].copy()
        try:
            #try to append to slice of existing dataframe new row
            #preserving the original data type
            t_df.loc[t_df.index.max()+1,:] = values
            self.cast_dtypes(t_df)
        except Exception as err:
            print(err)
            return(None)
        #preserve data format of master dataframe
        self.df[self.df.index.max()+1,:] = values
        self.cast_dtypes(self.df)
        return new_df
#################################

    def do_append(self, s):
        '''pd.DataFrame.append/concat methods avoided as they produce
           new object and original dataframe, passed as attribute to
           __init___, is left unmodified '''
        df = self.format_record(s)
        print(df)
        if type(df) == pd.DataFrame:
            a = input('append to existing df?(y/n):')
            if a == 'y':
                self.df.loc[self.df.index.max()+1,:] = 
                self.df = self.df.append(df)
                self.update_prompt()

    def complete_append(self, text, line, begidx, endidx):
        print(text)

    def do_exit(self, *args):
        self.close()
        return True

    def do_quit(self, *args):
        self.close()
        return True

    def do_shell(self, s):
        '''provide access to upstream context'''
        try:
            print(eval(s, self.context))
        except NameError as err:
            print(err)
        #os.system(s)

    def do_lshell(self, s):
        '''provide access to local context'''
        try:
            print(eval(s, globals(),locals()))
        except Exception as err:
            print(err)
