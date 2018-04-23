import os, sys
from shutil import copyfile
from dotmap import DotMap
import pandas as pd
#from lib_py.utils import common_utils as cu


class Data():
    '''manages data flow in our notebooks.
       tmp_dir: temporary files. ignored by version control (VC)
       data_dir: csv files optimized for VC (new records get appended)
       files_dir: non-data files, subject to VC
       IMPORTED DATA EVOLUTION
        if external file exists, copy it to 'tmp' and serve cached filename 
        (consult 'read' method)
       '''

    tmp_dir = "tmp"
    data_dir = "data"
    files_dir = "files"
    def __init__(self, root_dir=None):
        if not root_dir:
            root_dir = os.path.curdir
        self.root_dir = root_dir
        #check working dir structure
        for dir in [self.tmp_dir, self.data_dir, self.files_dir]:
            dir_path = os.path.join(root_dir, dir)
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)

    def read(self, file, cached=False, no_cache=False):
        '''
        file: file name for import
        cached: try to read cached version
        no_cache: do not make cached copy on read
        '''
        cached_file = os.path.join(self.root_dir,self.tmp_dir,os.path.basename(file))

        if cached:
            if not os.path.isfile(cached_file):
                raise FileNotFoundError("file " + cached_file + " was not found")
            return cached_file

        if os.path.isfile(file):
            #TODO: check whether file and cached_file are identical
            if not no_cache:
                copyfile(file, cached_file)
            return cached_file

        if os.path.isfile(cached_file):
            return cached_file

        raise FileNotFoundError("file "+ file + " was not found")


class RSet:
    def __init__(self, file, data_source_type='1c-cy'):
        '''import data from dump file generated by 1C accounting system (Windows7)'''
	#TODO: move "filter" and "lowercase" to __init__ arguments
        self.data_source_type = data_source_type
        self.file_path = file
        #discard intermediary records for account P1
        self.dt_source = self.import_data(filter='account != "P1"')
        #lowercase all values in selected columns
        up_cols = 'sub1 sub2'.split()
        self.dt_source[up_cols] = self.dt_source[up_cols].applymap(lambda x:str(x).lower())
        #create instruments to facilitate interactive work. The user is free to populate t with values suitable for task in hand
        _vars = dict(
            cols_ls = [],
            cols_ll =  [],
            filtr = dict(
                period = '',
                sources = [],
                accounts = []
            ),
            period = '2016-01-01:2017-12-31',
            accounts = [],
            m_names = []
        )
        self.vars = DotMap(_vars)

    def import_data(self, filter=None):
        if self.data_source_type == '1c-cy':
            tmp_dt = pd.read_csv(self.file_path, header=0,sep="\t",encoding='windows-1251',dtype={'account':str},
                               compression='infer')
            if filter:
                return tmp_dt.query(filter)
            else:
                return tmp_dt

    def filter_data(self, period, sources=None, accounts=None):
        '''period: "2017-01-01:2018-12-31"
           source: ['co_name_a','co_name_b'],
           accounts: ['24.01','22.04']'''
        (self.vars.period, self.vars.sources, self.vars.accounts) = [period, sources, accounts]
        #TODO: rewrite this function to avoid processing of undefined arguments
        #sources = sources if sources else self.dt_source['source'].unique()
        #accounts = accounts if accounts else self.dt_source['account'].unique()
        #
        sources_b = self.dt_source['source'].isin(sources) if sources else True
        accounts_b = self.dt_source['account'].isin(accounts) if accounts else True
        data_from, data_to = [t.strip() for t in period.split(':')]

        return self.dt_source[(self.dt_source['date'] >= data_from) & (self.dt_source['date'] <= data_to)
                            & sources_b & accounts_b]
#                             & self.dt_source['source'].isin(sources)
#                             & self.dt_source['account'].isin(accounts)]
