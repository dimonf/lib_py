from dotmap import DotMap
from functools import partialmethod
import types

#https://github.com/drgrib/dotmap
#we use the object's flexibility for quick prototyping and for storing metadata in custom attributes
#for standard classes (such as DataFrame for pivot table)

DotMap.__dir__ = lambda x: x.keys()
#
import pandas as pd
##########################################################
def _check_meta_attr_exists(df):
    if not hasattr(df, 'meta'):
        df.meta = DotMap()


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
                   rec_filter.append(tuple([key, val]))
                   label_ls.append(str(val))

               val = t_pivot.iloc[r,c]
               label_ls.append("{:.2f}".format(val))

               for n in range(len(t_pivot.columns.names)):
                   key = t_pivot.columns.names[n]
                   if col_size > 1:
                        val = t_pivot.columns[c][n]
                   else:
                        val = t_pivot.columns[c]
                   rec_filter.append([key, val])
                   label_ls.append(str(val))
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

    search_val = "*{0}*".format(search_val.lower())
    catch = get_filter_spec(self, search_val)

    if len(catch) > 1:
        data = '\n'.join(list(catch.keys())[:2])
        raise IndexError('more than 1 entries are found. be more specific:\n>>>>>>>\n' + data + '\n>>>>>>>')
        pass

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
    #pring totals if more than 1 record
    if len(recs) > 1:
        return recs.pipe(totals)

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
    #Ths soudce dataframe may be created 'on the fly' and this would be the only reference to it
    #e.g.: dt[dt['column1] == 'some_value'].pivotd(columns=..., index=..., values=...)
    #with fillna=0  some columns become of integer type which rounds up totals. For this reason
    #we replace NaN with 0 on the next line
    t_df.fillna(0, inplace=True)
    #
    t_df._source_df = self
    t_df.drill = types.MethodType(_drill, t_df)
    #t_df.drill = _drill
    return t_df

def _regex_select(self, col, regex_pattern, out='d'):
    '''self: DataFrame'''
    # deal with NaN
    b_indexer = self[col].fillna('').str.match(regex_pattern, case=False)
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

def _list_records(self, columns=[], tag='default'):
    _check_meta_attr_exists(self)

    if len(columns) > 0:
        self.meta.list[tag] = columns

    if len(self.meta.list[tag]) > 0:
        return self[self.meta.list[tag]]
    else:
        raise KeyError('no "meta" settings found for tag "'+tag +'"')

def totals(df, axis='row'):
    #TODO: allow to specify both axises in one argument
    t_df = df
    if axis in (0,'row','rows','r','both'):
        t_total = t_df.sum(numeric_only=True)
        t_total.name = "TOTAL:"
        t_df = t_df.append(t_total)
        t_df.iloc[-1] = t_df.iloc[-1].fillna('-')
       # t_df = t_df.append(t_df.sum(numeric_only=True), ignore_index=True)

    if axis in (1,'column', 'columns','c','col','both'):
        t_df = t_df.concat([t_df, pd.DataFrame(t_df.sum(axis=1), columns=["Total"])],axis=1)

    return t_df

def _rloc(df, search_str):
    '''regex based indexer'''
    #search string format: regex tokens for each index level separated by ','. E.g
    #tokens       : index_0 | index_1  | index_3 | col_0 | col_1
    #search string: [^D].*ac,bud[gG]et$,         ,       ,exp_a
    ind_len = len(df.index.levels)
    col_len = len(df.columns.levels)

    s_str = search_str.split(',')
    #reshape search args to conform to dataframe overall indexes size:
    s_str = s_str[:ind_len + col_len]
    s_str.extend(['' for i in range(ind_len + col_len - len(s_str))])
    #
    match_hits = 0
    def get_vals(level, str_v):
        nonlocal match_hits

        l_match = list(level[level.str.contains(str_v, case=False)]) if str_v else []
        if len(l_match):
            match_hits +=1
            return l_match
        else:
            return slice(None)

    (i_s, c_s) = ([],[])
    n=0
    for level in df.index.levels:
        i_s.append(get_vals(level, s_str[n]))
        n+=1
    for level in df.columns.levels:
        c_s.append(get_vals(level, s_str[n]))
        n+=1

    #generally we want empty dataframe if our search didn't match any index value
    if match_hits:
        return df.loc[tuple(i_s), tuple(c_s)]
    else:
        raise KeyError('not single match was found for search criteria: ' + search_str)



class Object:
    '''dumb object serves as building block '''
    pass

def d2obj(d):
    """Convert a dict to an object

    >>> d = {'a': 1, 'b': {'c': 2}, 'd': ["hi", {'foo': "bar"}]}
    >>> obj = dict2obj(d)
    >>> obj.b.c
    2
    >>> obj.d
    ["hi", {'foo': "bar"}]
    """

    try:
        d = dict(d)
    except (TypeError, ValueError):
        return d
    obj = Object()
    for k, v in d.items():
        obj.__dict__[k] = d2obj(v)
    return obj




########################## RUN SECTION ###################
'''extend standard pandas.DataFrame object with drill_down functionality.
   restrictions:
       a. multiple values for 'values' parameter of _pivot_table are not supported
'''

pd.DataFrame.pivotd = _pivot_table
#pd.DataFrame.drill = _drill
pd.DataFrame.rr = _regex_select
pd.DataFrame.rloc = _rloc
#pd.DataFrame.ls = _list_records
pd.DataFrame.ls = partialmethod(_list_records, tag='short')
pd.DataFrame.ll = partialmethod(_list_records, tag='long')

#pd.ll = property(partialmethod(_list_records, tag='long'), partialmethod(_list_records, tag='long'))
#pd.ls = property(partialmethod(_list_records, tag='short'), partialmethod(_list_records, tag='short'))

########################################################
