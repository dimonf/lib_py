import pandas as pd, numpy as np
import types
import re
import functools


def rr(self, regex_pattern, col=None, out='d', invert=False):
    ''' provides with convinient method to filter rows (axis=0 only), where
        regex_pattern is applied to either index or a column ('col' argument)

        self._obj: DataFrame
        regex_pattern: either regex pattern or dict {column:regex}
        col: either index or column name of given DataFrame. Ignored if regex_pattern is dict
        out: output as d(ataframe),s(Series:boolean)
    '''
    def get_bool_series(column, regex_p):
        #check whether col value is in columns or index name
        if column in self._obj.columns:
            # deal with NaN
            t_s = self._obj[column].fillna('')
        elif column in self._obj.index.names:
            t_s = self._obj.index.get_level_values(column)
        else:
            raise KeyError('name '+column+' is not found neither in columns: ' +
                           self._obj.columns + ' nor in index names '+self._obj.index.names)

        if t_s.dtype != np.object:
           t_s = t_s.astype('str')

        return t_s.str.match(regex_p, case=False)


    b_series = []

    if type(regex_pattern) == dict:
        for k,v in regex_pattern.items():
            b_series.append(get_bool_series(column = k, regex_p = v))
    else:
        b_series.append(get_bool_series(column = col, regex_p = regex_pattern))

    b_indexer = functools.reduce(lambda x,y: x & y, b_series)

    if invert:
        b_indexer = b_indexer == False
    if out == 'd':
        #filtered [d]ataframe
        return self._obj.loc[b_indexer]
    elif out == 'b':
        #[b]oolean indexer
        return b_indexer
