import pandas as pd, numpy as np
from fnmatch import fnmatch


def cols_match(self, pattern):
    '''
    self: dataframe
    pattern: unix filename pattern matching style columns names, separated by comma
    NOTES:
      - limitation: comma cannot form part of a column pattern
      - patern search is case insensitive
    '''

    columns_p = pattern.split(',')
    columns_out = []

    for col in self._obj.columns:
        if len(columns_p) == 0:
            break
        column_p = columns_p[0]
        if fnmatch(col.lower(),column_p.lower()):
            columns_out.append(col)
            columns_p.pop(0)

    return columns_out
