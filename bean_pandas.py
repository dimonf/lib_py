'''
   data gateway between pandas and beancount
'''

from beancount import loader
from beancount.query import query

import pandas as pd

class BeanPandas():
    def __init__(self, input_file):
        self._entries, self._errors, self._options_map = loader.load_file(input_file)

    def query(self, query_str, *args):
       '''read doc for run_query in /usr/lib/python3.7/site-packages/beancount/query/query.py
         query('select date,account,number where account~{} and year ={}",'"assets"','2016')
       '''
       return query.run_query(self._entries, self._options_map, query_str, *args)
