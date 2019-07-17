import pandas as pd, numpy as np
import types
import re

from ._pivot import _pivot_table
from . import _rr, _columns_selector
#
##########################################################

def _regex_select_groupby(self, regex_pattern):
    r_search = re.compile(regex_pattern, re.IGNORECASE)
    for t,r in self:
        #if multilevel index - make it flat
        if not isinstance(t, str):
            t = '_'.join(t)
        #
        if r_search.search(t):
            return (r)

@pd.api.extensions.register_dataframe_accessor("ex")
class DfExtensions(object):
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        #if obj['date'].min() <=  'something':
        pass

    def pivotd(self,**kwargs):
        return(_pivot_table(self._obj, **kwargs))

    def rr(self, regex_pattern, col=None, out='d', invert=False):
        return _rr.rr(self, regex_pattern, col, out='d', invert=False)

    def cols(self, pattern):
        '''handy columns selector '''
        return self._obj[_columns_selector.cols_match(self, pattern)]

    def totals(self, axis='row'):
        df_t = self._obj.copy()
        if axis in (0,'row','rows','r','both', 'all'):
            t_total = self._obj.sum(numeric_only=True)
            levels = len(self._obj.index.names)
            #t_index = ['Total:'] * levels
            if levels > 1:
                ind = tuple(['TOTAL:'] + ['---']*(levels-1))
                df_t.loc[ind,:] = t_total
            else:
                df_t.loc['TOTAL:',:] = t_total

        if axis in (1,'column', 'columns','c','col','both', 'all'):
            t_total = self._obj.sum(axis=1, numeric_only=True)
            levels = len(self._obj.columns.names)
            if levels >1:
                ind = tuple(['TOTAL:'] + ['-']*(levels-1))
                df_t.loc[:,ind] = t_total
            else:
                df_t.loc[:,'TOTAL:'] = t_total

        return df_t.fillna('--')

    def rtotal(self, column):
        '''append column with running total for selected columns in a dataframe'''
        pos = self._obj.columns.get_indexer_for([column])[0]
        if pos == -1:
            raise KeyError('no column "'+column + '" found')
        (df_before, df_rtotal, df_after) = self._obj.iloc[:,0:pos+1], \
                self._obj.iloc[:,pos].cumsum(), self._obj.iloc[:,pos+1:]
        df_rtotal.name = column + ":t"
        return pd.concat([df_before, df_rtotal, df_after], axis=1)

    def between (self, column, left, right, inclusive=True):
        ''' syntactic sugar for pd.Series.between function'''
        return(self._obj[self._obj[column].between(left, right, inclusive)])


@pd.api.extensions.register_series_accessor('ex')
class SeriesExtensions(object):
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        #something
        pass

    def ugrep(sr, pattern):
        '''unique grep on a series'''
        re_compiled = re.compile(pattern, re.I)
        return [v for v in sr.unique() if re_compiled.search(v)]

def pd_infect():
    '''extend the rest of pandas objects by composition
    '''
    functions = [
        {'class': pd.core.groupby.DataFrameGroupBy,
         'attribute': 'rr',
         'function': _regex_select_groupby},
    ]

    for f in functions:
        #check if the name is available (avoid overwriting)
        if not hasattr(f['class'],f['attribute']):
                setattr(f['class'],f['attribute'],f['function'])

pd_infect()
