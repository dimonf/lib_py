class Mapper():
    '''
    fake use case for class Mapper
      scenario: transactions are imported from CY 1C accounting system, where names are taken from bank statements,
      while data with total is provided by other office, where names reflect contract and transportation records
      The report will be in this form:
      name: Dumster Store Lines
      date               USD         narr
      1-Jan-2014         xxxxx.xx    balance b/f
      15-Feb-2014        xxxx.xx     HB:214-242332-22/ nv 24423212-12, 349271-12, 322349-12
      ...
      31-Dec-2014        xxxxx.xx    balance c/f

       maps values from imported data to "our" values. underlying dataframe structure:
               val | source | our | sub | timestamp
       val      : "Dorston Fifth","Blue Survey Group " #imported value
       our      : "dorston inc, blue survey ltd"       #our value
       source   : "1c-cy local_co_a", "1c-Voronez"     #source of imported data
       sub      : "51.01","ccenter"                    #subdomain (arbitrary use)
       modified : "2016-11-30 15:33:01"                #record added/amended

       to vc-friendly:
        - new entry shall be appended to the end of the file,
        - unmodified entries shall maintain their shape and order

        Attributes
        ---------
       '''
    cols = 'val our source sub modified'.split()

    def __init__(self, file, source=None, sub=None, *arg, **kw):
        '''
        Parameters
        ----------
        file : string
            filename where underlying data is stored
        source : string
            name of external source from which the data is imported
        sub : string
            subdomain. Extra field to partition namespace of underlying data

        '''
        self.file   = file
        self.source = source
        self.sub    = sub

        #leave non-null entries only
        t_filter_args = {k:[v] for k,v in [('source', self.source),('sub',self.sub)] if v}
        t_cols = list(t_filter_args.keys())

        self.df = self.read_from_data_file()
        #apply filter
        if len(t_cols) > 0:
            self.df = self.df[self.df[t_cols].isin(t_filter_args).all(axis=1)]

    def __getitem__(self, key):
        pass

    def __setitem__(self, key, value):
        pass

    def read_from_data_file(self):
        ''' read data from text file and return pd.DataFrame '''
        if not self.file:
            return
        #test if we can write to the file
        if not os.path.isfile(file):
            raise FileNotFoundError("file \"" + file + "\" is not reachable")
        if not os.access(file, os.W_OK):
            raise PermissionError("file \"" + file + "\" is not readable")

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
