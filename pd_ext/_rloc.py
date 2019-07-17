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
