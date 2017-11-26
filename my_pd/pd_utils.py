import os
import pandas as pd
import re


class Mapper(dict):
    '''maps values from imported data to "our" values. underlying dataframe structure:
               val | source | our | sub | timestamp
       val      : "Dorston Fifth","Blue Survey Group " #imported value
       our      : "dorston inc, blue survey ltd"       #our value
       source   : "1c-cy local_co_a", "1c-Voronez"     #source of imported data
       sub      : "51.01","ccenter"                    #subdomain (arbitrary use)
       modified : "2016-11-30 15:33:01"                #record added/amended

       to meet git functionality:
        - new entry shall be appended to the end of the file,
        - not changed entries shall maintain their shape and order

       '''
    cols = 'val our source sub modified'.split()

    def __init__(self, file, source=None, sub=None, *arg, **kw):
        super().__init__(*arg, **kw)
        #
        self.file   = file
        self.source = source
        self.sub    = sub

        #leave non-null entries only
        t_filter_args = {k:[v] for k,v in [('source', self.source),('sub',self.sub)] if v}
        t_cols = list(t_filter_args.keys())

        return
        t_df = self.read_from_data_file()
        #apply filter
        if len(t_cols) > 0:
            t_df = t_df[t_df[t_cols].isin(t_filter_args).all(axis=1)]

        self.update(dict(zip(t_df['val'], t_df['our'])))

    def read_from_data_file(self):
        ''' read data from text file and return pd.DataFrame '''
        if not self.file:
            return
        #test if we can write to the file
        if not os.path.isfile(self.file):
            raise FileNotFoundError("file \"" + self.file + "\" is not reachable")
        if not os.access(self.file, os.W_OK):
            raise PermissionError("file \"" + self.file + "\" is not readable")

        t_dtype = {k:str for k in self.cols}
        t_dtype['modified'] = pd.Timestamp

        return(pd.read_csv(self.file, header=0, dtype=t_dtype, comment='#'))

    def save(self, file=None):
        if not file:
            file = self.file

        #discard data with the same context as our object
        t_filter_args = {k:[v] for k,v in [('source', self.source),('sub',self.sub)] if v}
        t_cols = list(t_filter_args.keys())

        if len(t_cols) > 0:
            t_df_o = self.read_from_data_file()
            t_df_o = t_df_o[~t_df_o[t_cols].isin(t_filter_args).all(axis=1)]
        else:
            t_df_o = pd.DataFrame(columns=self.cols)


        #re-create dataframe from dictionary
        t_df = pd.DataFrame([(k,v) for k,v in self.items()],columns = ['val','our'])
        t_cols = 'source sub modified'.split()
        t_df[t_cols] = t_df.apply(lambda x: pd.Series(
            [self.source, self.sub, pd.Timestamp.now()],index=t_cols),axis=1)

        #combine two dataframes, sort by 'modified' column and dump it to 



        t_df[self.cols].to_csv(file, index=False)


def group_by_get_group_names(group_by_obj, pat, show_records = True):
    '''the function is designed for interactive use. Most of the time the user
       will expec to have a single value out ot if which is achied by
       conditional conversion o output value'''
    re_s = re.compile(str(pat), re.IGNORECASE)
    out_l = [k for k in group_by_obj.groups.keys() if re.search(re_s, str(k))]
    if len(out_l) == 1:
        return group_by_obj.get_group(out_l[0])

    return out_l


