from dotmap import DotMap
from functools import partialmethod
import types
import numpy as np
import re

#https://github.com/drgrib/dotmap
#we use the object's flexibility for quick prototyping and for storing metadata in custom attributes
#for standard classes (such as DataFrame for pivot table)

DotMap.__dir__ = lambda x: x.keys()
#
import pandas as pd
##########################################################
def _drill(self, search_val):
    import fnmatch
    '''this function is monkey patched to a dataframe instance upon execution of pivot_table method.
       it returns list of records that were aggregated by pivot_table function for a specific value.
       the value of interest is specified by a string comprising concantenated index, value and column,
       which resolves exactly into the value of interest. use of wildcard is allowed (via fnmatch) '''
    def get_filter_spec(t_pivot, search_val):
        '''get specification in form "filter label":"value" for selected value
           in pivot table which for the purpose of search is serialized into the list
           of text strings, where each unique row-value-column combination is represented
           by one line. if more than 1 line matches criteria, the search is failed
        '''

        index_size = len(t_pivot.index.names)
        col_size   = len(t_pivot.columns.names)
        label_ls = []
        rec_filter = []

        catch = dict()
        for r in range(len(t_pivot.index)):
           for c in range(len(t_pivot.columns)):

               label_ls.clear()
               rec_filter.clear()
                ##########
               for n in range(len(t_pivot.index.names)):
                   key = t_pivot.index.names[n]
                   if index_size > 1:
                        val = t_pivot.index[r][n]
                   else:
                       val = t_pivot.index[r]
                   rec_filter.append([key, val])
                   label_ls.append(str(val))

               val = t_pivot.iloc[r,c]
               label_ls.append("{:.2f}".format(val))

               for n in range(len(t_pivot.columns.names)):
                   key = t_pivot.columns.names[n]
                   if not key:
                       continue
                   if col_size > 1:
                        val = t_pivot.columns[c][n]
                   else:
                        val = t_pivot.columns[c]
                   rec_filter.append([key, val])
                   label_ls.append(str(val))
                   #print("col\n",label_ls, '\n', rec_filter)
                ##########

               label_str = '/'.join(label_ls)
               label_str = label_str.lower()
               ###
               if fnmatch.fnmatch(label_str, search_val.lower()):
                   catch[label_str] = rec_filter.copy()

        #print('debug_catch_out', catch)
        return catch

    ############################
    ############################

    search_val = "*{0}*".format(str(search_val).lower())
    catch = get_filter_spec(self, search_val)

    if len(catch) > 1:
        data = '\n'.join(list(catch.keys())[:2])
        message = "more than 1 entries are found, be more specific:\n>>>>>>>\n" + data + "\n>>>>>>>"
        print(message)
        return
        #since the drilldown function is designed for interactive use,
        #raising exception is avoided
#        raise IndexError('more than 1 entries are found. be more specific:\n>>>>>>>\n' + data + '\n>>>>>>>')
#        pass

    if len(catch) == 0:
        return None

    (key, filter_spec) = catch.popitem()

    t_filter_spec = []
    for k,v in filter_spec:
        if type(v) == str:
            v = '"{0}"'.format(v)
        t_filter_spec.append('({0} == {1})'.format(k,v))

    query_spec = ' & '.join(t_filter_spec)
    recs = self._source_df.query(query_spec)
    #print totals if more than 1 record
    if len(recs) > 1:
        return recs.pipe(_totals)
        #return recs.totals()

    return recs

def _pivot_table(self, values=None, index=None, columns=None, aggfunc='sum',
        margins=False, dropna=True, margins_name='All'):

    kargs = locals()
    #remove extra attributes
    del kargs['self']
    #remove empty to avoid pollution and overriding of non-null values previously stored in meta.pivot
    #TODO: some arguments has default values other than None
    kargs = {k:v for k,v in kargs.items() if v is not None}
    '''
    parameters are defined explicitly, rather than collected through **kargs, to facilitate
    ipython's autocompletion functionality. parameters, extra to standard pd.pivot_table function:
        name: set of standard parameters to pivot_table are stored under
              this key in local meta.pivot attribute for future use
    '''

    t_df = self.pivot_table(**kargs)
    #create reference to the original dataframe to enable drill-down functionality
    #Ths source dataframe may have been created 'on the fly' and this would be the only reference to it
    #e.g.: dt[dt['column1] == 'some_value'].pivotd(columns=..., index=..., values=...)
    #with fillna=0  some columns become of integer type which rounds up totals. For this reason
    #we replace NaN with 0 on the next line
    t_df.fillna(0, inplace=True)
    #
    t_df.__dict__.update({'_source_df':self})
    #t_df._source_df = self
    #t_df.drill = types.MethodType(_drill, t_df)
    t_df.__dict__.update({'drill': types.MethodType(_drill, t_df)})
    return t_df


def _regex_select(self, regex_pattern, col, out='d', invert=False):
    ''' provides with convinient method to filter rows (axis=0 only), where
        regex_pattern is applied to either index or a column ('col' argument)

        self: DataFrame
        regex_pattern: text to search for
        col: either index or column name of given DataFrame
        out: output as d(ataframe),b(oolean),u(nique values, Series) or i(ndex)
    '''
    #check whether col value is in columns or index name
    if col in self.columns:
        # deal with NaN
        t_s = self[col].fillna('')
    elif col in self.index.names:
        t_s = self.index.get_level_values(col)
    else:
        raise KeyError('name '+col+' is not found neither in columns ' +
                       self.columns + ' nor in index names '+self.index.names)

    if t_s.dtype != np.object:
       t_s = t_s.astype('str')

    b_indexer = t_s.str.match(regex_pattern, case=False)

    if invert:
        b_indexer = b_indexer == False

    if out == 'd':
        #filtered [d]ataframe
        return self.loc[b_indexer]
    elif out == 'b':
        #[b]oolean indexer
        return b_indexer
    elif out == 'u':
        #[u]nique values
        return self[col].loc[b_indexer].value_counts()
    elif out == 'i':
        #[i]ndex
        return self.loc[b_indexer].index.tolist()

def _regex_select_groupby(self, regex_pattern):
    import re
    r_search = re.compile(regex_pattern, re.IGNORECASE)
    for t,r in self:
        if r_search.search(t):
            return (r)


def _between (self, column, left, right, inclusive=True):
    ''' syntactic sugar for pd.Series.between function'''
    return(self[self[column].between(left, right, inclusive)])


def _rloc(df, search_str):
    '''regex based indexer
    search string format: regex tokens for each index level separated by ','. E.g
      tokens       : index_0 | index_1  | index_3 | col_0 | col_1
      search string: [^D].*ac,bud[gG]et$,         ,       ,exp_a
    the indexer format for each of axis is: tuple(level_0, [level_1_a, level_1_b], ..., level_n)
      each level's indexer can be slice(None) which accepts any value 
      (https://pandas.pydata.org/pandas-docs/stable/advanced.html#cross-section)
    '''
    ind_len = df.index.nlevels
    col_len = df.columns.nlevels

    s_str = search_str.split(',')
    #reshape search args to conform to dataframe overall indexes size:
    s_str = s_str[:ind_len + col_len]
    s_str.extend(['' for i in range(ind_len + col_len - len(s_str))])
    #
    match_hits = 0
    n = 0
    def get_vals(level):
        nonlocal match_hits
        nonlocal n

        l_match = list(level[level.str.contains(s_str[n], case=False)]) if s_str[n] else []
        n+=1

        if len(l_match):
            match_hits +=1
            return l_match
        else:
            return slice(None)

    a = dict(index = dict(ind=[], ref=df.index), columns = dict(ind=[], ref=df.columns))
    for k,v in a.items():
        if v['ref'].nlevels == 1:
            v['ind'].append(get_vals(v['ref']))
        else:
           for level in v['ref'].levels:
              v['ind'].append(get_vals(level))

    #generally we want empty dataframe if our search didn't match any index value
    if match_hits:
        return df.loc[tuple(a['index']['ind']), tuple(a['columns']['ind'])]
    else:
        raise KeyError('not single match was found for search criteria: ' + search_str)

def _totals(df, axis='row'):
    #TODO: allow to specify both axises in one argument
    df_t = df.copy()
    if axis in (0,'row','rows','r','both', 'all'):
        t_total = df.sum(numeric_only=True)
        #t_total.name = "TOTAL:"
        #df = df.append(t_total)
        #df.iloc[-1] = df.iloc[-1].fillna('-')
        levels = len(df.index.names)
        #t_index = ['Total:'] * levels
        if levels > 1:
            ind = tuple(['TOTAL:'] + ['---']*(levels-1))
            df_t.loc[ind,:] = t_total
            #df.loc[['Total:','--'],:] = t_total
        else:
            df_t.loc['TOTAL:',:] = t_total
       # df = df.append(df.sum(numeric_only=True), ignore_index=True)

    if axis in (1,'column', 'columns','c','col','both', 'all'):
        t_total = df.sum(axis=1, numeric_only=True)
        levels = len(df.columns.names)
        if levels >1:
            ind = tuple(['TOTAL:'] + ['-']*(levels-1))
            df_t.loc[:,ind] = t_total
        else:
            df_t.loc[:,'TOTAL:'] = t_total
        #df = df.concat([df, pd.DataFrame(df.sum(axis=1), columns=["Total"])],axis=1)

    return df_t.fillna('--')

def _rtotal(df, column):
    '''append column with running total for selected columns in a dataframe'''
    pos = df.columns.get_indexer_for([column])[0]
    if pos == -1:
        raise KeyError('no column "'+column + '" found')
    (df_before, df_rtotal, df_after) = df.iloc[:,0:pos+1], df.iloc[:,pos].cumsum(), df.iloc[:,pos+1:]
    df_rtotal.name = column + ":t"
    return pd.concat([df_before, df_rtotal, df_after], axis=1)
  
def _drill_group_by(self, df):
    '''
    attached as a method to DF output from groupby.aggregate(), which
    has index structure that reflects the original groupby construction parameters.
    Output is the list of groups of records from the original (df) dataframe
    that were previously aggregated by groupby
    '''
    groups = []
    for val in self.index.values:
        query = []
        for ind, key in enumerate(self.index.names):
            query.append(' {} == "{}" '.format(key, val[ind]))
        query_str = ' and '.join(query)
        groups.append(df.query(query_str))

    return groups

def _ugrep(sr, pattern):
    '''unique grep on a series'''
    re_compiled = re.compile(pattern, re.I)
    return [v for v in sr.unique() if re_compiled.search(v)]
    
def pd_infect():
    '''extend standard pandas.DataFrame object with drill_down functionality.
       restrictions:
           a. multiple values for 'values' parameter of _pivot_table are not supported 
    '''
    import pandas as pd, numpy as np
    functions = [
        (pd.DataFrame, 'pivotd',_pivot_table),
        (pd.DataFrame, 'rr',_regex_select),
        (pd.DataFrame, 'totals',_totals),
        (pd.DataFrame, 'rtotal',_rtotal),
        (pd.DataFrame, 'btw',_between),
        (pd.core.series.Series, 'ugrep',_ugrep),
        (pd.core.groupby.groupby.DataFrameGroupBy, 'rr', _regex_select_groupby)
    ]

    for i in functions:
        #check if the name is available (avoid overwriting)
        if not hasattr(i[0], i[1]):
            setattr(i[0], i[1], i[2])
