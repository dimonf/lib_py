import pandas as pd, numpy as np 
import os, sys, fnmatch, json, copy


def_agg_function = np.sum
def_store = os.getenv("HOME") + "/.pandas"

'''
 structure:
 - _global    : {        #holds all settings that can be applied to any pivot table (stored under tags)
        - f_row: {}             #defaults for filter section
        - opts  : {}             #defaults for opts section
        - f_col: {           #keyed array of settings for raw data view
             - short_list: ['source', 'amount', 'usd'],
             - long_list:  ['source', 'id', 'date', 'amount', 'currency', 'usd']
               }
          }
 - _session: {           #contains just usage information
             - f_col_active: 'short_list',    #holds key for latest applied value from _list_opts
             }
 - piv:

     - default(tag):
          - f_row: []                                #list of tuples: (function, columne, value) - check class F
          - opts   : {
               - index  :{['source','sub1']},
               - values : 'usd'
               }

      
'''

class DictS(dict):
    def __init__(self,*args):
        dict.__init__(self, args)
        self["_global"] = dict(
                f_row = {},
                opts = {},
                f_col = {}
                )
        self["_session"] = dict(
                f_col_active = '',
                last_tag = ''
                )
        self["piv"] = dict()

    def get_f_col(self, tag=None):
        #get global list options
        if not tag:
            return self['_global']['f_col']
        elif tag == 'last':
            tag = self['_session']['f_col_active']
        return self['_global']['f_col'].get(tag, None)

    def set_f_col(self, opts, tag=None):
        '''opts - list of options '''
        #
        if not tag:
            #get existing tags and apply not used one-character tag from ASCII range:
            existing_tags = list(map(lambda k: k, self.get_f_col().keys()))
            for t in range(ord('a'), ord('z')+1):
                if chr(t) not in existing_tags:
                    tag = chr(t)
                    break
            if not tag:
                raise IndexError('cannot assign new tag: no more available tags for new col setting')
        self['_global']['f_col'][tag] = opts.copy()

    def set_f_col_last(self, tag):
        if tag in self['_global']['f_col'].keys():
            self['_session']['f_col_active'] =  tag

    ###
    def get_opts(self, tag):
        tag = tag or self.last_tag()
        if not tag in self['piv'].keys():
            self.create_structure(tag)
        return self["piv"][tag]['opts']

    def set_opts(self, settings, tag):
        tag = tag or self.last_tag()
        self.get_opts(tag)
        self['piv'][tag]['opts'] = settings.copy()

    def get_filter(self, tag = None):
        tag = tag or self.last_tag()
        if not tag in self['piv'].keys():
            self.create_structure(tag)
        return self['piv'][tag]['f_row']

    def set_filter(self, filter, tag = None):
        tag = tag or self.last_tag()
        self.get_filter(tag)
        self['piv'][tag]["f_row"] = filter
    def clone(self, new_tag, old_tag = None):
        old_tag = old_tag or self.last_tag()
        p = self['piv']
        p[new_tag] = copy.deepcopy(p[old_tag])

    def last_tag(self):
        return self['_session'].get('last_tag', None)

    def create_structure(self, tag):
        if tag[0] == "_":
            raise NameError('tag name cannot begin with underscore')

        self['piv'][tag] = dict(
                opts = {},
                f_row = []
        )

    def reg_tag_use(self, tag):
        self['_session']['last_tag'] = tag

    def check(self):
        '''check data scheme'''
        for k,v in self['piv'].items():
            #print_debug(['a', k, self['piv'][k]['f_row']])
            #debug: amend the structure:
            f_row = self['piv'][k]['f_row']
            for i in range(len(f_row)):
                if type(f_row[i]) is not tuple:
                    f_row[i] = tuple(f_row[i])


class PT:
    def __init__(self):
        ### allow for full screen tables
        pd.set_option("display.max_columns", 0)
        ###
        ### most recent pivot object
        self.pivot = None
        ### keyed collection of settings for pivot reports
        self.s = DictS()
        ### apply extra functions to DataFrame class
        self.apply_extra_func()
        """ """
    def apply_extra_func(self):
        #extra functionality for a data frame
        #rows
        pd.DataFrame.r_eq = lambda df, key, value : df[df[key] == value]
        pd.DataFrame.r_ge = lambda df, key, value : df[df[key] >= value]
        pd.DataFrame.r_gt = lambda df, key, value : df[df[key] > value]
        pd.DataFrame.r_le = lambda df, key, value : df[df[key] <= value]
        pd.DataFrame.r_lt = lambda df, key, value : df[df[key] < value]
        pd.DataFrame.r_ne = lambda df, key, value : df[df[key] != value]
        pd.DataFrame.r_isin = lambda df, key, value : df[df[key].isin(value)]
        pd.DataFrame.r_nin  = lambda df, key, value : df[~df[key].isin(value)]
        pd.DataFrame.r_rx  = lambda df, key, value : df[df[key].str.contains(value, case=False)]
        pd.DataFrame.r_nrx  = lambda df, key, value : df[~df[key].str.contains(value, case=False)]
        pd.DataFrame.r_gen  = lambda df : df[f(df)]
        ####
        def rx_multiple(df, *args):
            '''*args shall be provided in pair (key0, value0, key1, value1, ...), the way they are consumed
               The only reason for this function is to provide succint way to apply multiple filter
               criteria at one call'''
            df_t = df
            args = [iter(args)] * 2
            for key, value in zip(*args):
                df_t = df_t[df[key].str.contains(value, case=False)]

            return df_t

        pd.DataFrame.rx = rx_multiple
        #columns
        def col_multiple(df, *cols, **kwargs):
            ''' cols can be 
                  'short_list'
                  'source', 'ccenter'
                  'source,ccenter'
                - a key with reference to stored list of columns in self.s._global.f_col[key]
                - a list of columns (either as list like object or comma separated list)
            '''
            if type(cols) is str:
                if cols.find(','):
                    #comma separated list of column names
                    cols_l = cols.split(',')
                else:
                    cols_a = self.s.get_f_col(cols)
                    if cols_a is None:
                        raise KeyError ('no column filter is found for given key ' + cols)
            else:
                cols_a = cols
                print_debug(cols_a, type(cols_a))

            return df.loc[:,cols_a]

        pd.DataFrame.cx  = col_multiple;
        #
        #### presentation ####
        ####
        def pprint(df, all=None, cols=None):
            rows = None if all else 10
            if type(cols) == str:
                #stored setting
                cols = self.s.get_f_col(cols)
            elif type(cols) in (list, tuple):
                #new setting. check first
                dummy = df[cols].count()
                self.s.set_f_col(cols)

            #with pd.option_context('display.max_rows', rows, 'display.max_columns', None):
            with pd.option_context('display.max_rows', rows):
                if cols:
                    print(df[cols])
                else:
                    print(df)

        pd.DataFrame.pp = pprint


    def import_data(self, input_file, input_cp = 'utf-8'):
        self.data_in = pd.read_csv(input_file, encoding = input_cp, sep="\t")
        return self.data_in

    def settings_save(self, file_name=None):
        file_name = file_name or def_store + "/pd_cli"

        #complex objects are stored as str representation:
        def def_json(o):
            return str(o)


        os.makedirs(def_store, exist_ok=True)
        with open(file_name, "w") as conf_file:
            json.dump(self.s, conf_file, sort_keys = True, skipkeys=True,
                    indent = 4, ensure_ascii=False, default=def_json)


    def settings_restore(self, file_name=None):
        file_name = file_name or def_store + "/pd_cli"

        with open(file_name) as conf_file:
            settings_in = json.load(conf_file)
        self.s.clear()

        self.s.update(settings_in)
        #json does not recognizes differences between tuple and list. We do.
        self.s.check()


    def get_pivot(self, tag=None, settings={}, filters=[], df=None):
        '''filters: tuple(function, columne, value) '''
        tag = tag or self.s.last_tag()
        df = df or self.data_in


        #lowest priority: default settings applied to every single pivot table
        sett = dict(
                fill_value = 0,
                dropna     = True)

        sett.update(self.s.get_opts(tag))
        #higher priority defaults (overriding):
        sett.update({'aggfunc':def_agg_function})
        #
        sett.update(settings)
        self.s.set_opts(sett, tag)
        #
        #note: only exact duplication are removed by set()
        pre_flt = self.s.get_filter(tag)
        pre_flt = list(set(pre_flt + filters))
        self.s.set_filter(pre_flt, tag)
        #

        #data = self.get_records(pre_flt, df)
        data = self.get_rec(pre_flt, df)

        self.pivot = data.pivot_table(**sett)
        self.s.reg_tag_use(tag)
        return self.pivot


    def get_filter_spec(self, search_val):
        '''get specification in form "filter label":"value" for selected value
           in pivot table which for the purpose of search is serialized into the list
           of text strings, where each unique row-value-column combination is represented
           by one line. if more than 1 line matches criteria, the search is failed
        '''
        index_size = len(self.pivot.index.names)
        col_size   = len(self.pivot.columns.names)
        #rec_filter = dict()
        label_ls = []
        catch = dict()

        for r in range(len(self.pivot.index)):
           for c in range(len(self.pivot.columns)):
               #rec_filter.clear()
               rec_filter = []
               label_ls.clear()
                ##########
               for n in range(len(self.pivot.index.names)):
                   key = self.pivot.index.names[n]
                   if index_size > 1:
                        val = self.pivot.index[r][n]
                   else:
                       val = self.pivot.index[r]
                   rec_filter.append(tuple(['eq', key, val]))
                   #rec_filter[key] = val
                   label_ls.append(val)

               val = self.pivot.iloc[r,c]
               label_ls.append("{:.2f}".format(val))

               for n in range(len(self.pivot.columns.names)):
                   key = self.pivot.columns.names[n]
                   if col_size > 1:
                        val = self.pivot.columns[c][n]
                   else:
                        val = self.pivot.columns[c]
                   rec_filter.append(tuple(['eq', key, val]))
                   #rec_filter[key] = val
                   label_ls.append(val)
                ##########

               label_str = '/'.join(label_ls)
               label_str = label_str.lower()
               ###
               if fnmatch.fnmatch(label_str, search_val.lower()):
                   catch[label_str] = rec_filter.copy()

        # catch = {'vistrou inc/yacht expenses/10918.67/external': {'sub2': 'yacht expenses', 'source': 'Vistrou Inc', 'ccenter': 'External'}}
        # should be {'..........................................': [('eq','sub2','yacht expenses'), .. ]
        return catch

    def drill(self, search_val, col_tag=None):
        pre_flt = self.s.get_filter()

        search_val = "*{0}*".format(search_val.lower())
        catch = self.get_filter_spec(search_val)

        if len(catch) > 1:
            raise IndexError('more than 1 entries are found. be more specific')
            pass

        if len(catch) == 0:
            return None

        (key, filter_spec) = catch.popitem()
        #include 'global' filter, applied before pivot table was formed
        if pre_flt:
            filter_spec = list(set(filter_spec + pre_flt))

        #recs = self.get_records(filter_spec)
        recs = self.get_rec(filter_spec)

        if len(recs) > 1:
            recs = recs.append(recs.sum(numeric_only=True), ignore_index=True)

        #select columns to print
        if col_tag:
            filter_col = self.s.get_f_col(col_tag)
            if not filter_col:
                raise KeyError('no column filter defined for ' + col_tag)

            self.s.set_f_col_last(col_tag)
            return recs[filter_col]

        return recs

    def get_rec(self, filters=[], df=None):
        if df is None:
            df = self.data_in

        '''filter can be in form of 'query' or list of tuples, each tuple containing:
        [0] f_name       - function name as defined in class F
        [1] c_name       - column name
        [2] value        - value (can be string, array of strings or regular expression)
        '''
        if not filters:
            return df

        if type(filters) == 'str':
            return df.query(filters)

        t_df = df
        for f in filters:
            (f_name, c_name, val) = f
            t_df = getattr(t_df, "r_" + f_name)(c_name, val)

        return t_df


def print_debug(*obj):
    print("     ***   DEBUG  ***")
    for o in obj:
        print(o)
#    if type(obj) in (list, tuple):
#        for i in obj:
#            print(i)
#    else:
#        print(obj)

if __name__ == "__main__":
    p = pt()
    p.import_data()
    p.get_pivot('default')
    p.drill(sys.argv[1])
