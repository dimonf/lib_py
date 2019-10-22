import pandas as pd, numpy as np
import types
import re

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

        index_size = t_pivot.index.nlevels
        col_size = t_pivot.columns.nlevels
#        index_size = len(t_pivot.index.names)
#        col_size   = len(t_pivot.columns.names)
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
        #return recs.pipe(_totals)
        return recs.ex.totals()
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
    t_df.__dict__.update({'drill': types.MethodType(_drill, t_df)})
    return t_df
