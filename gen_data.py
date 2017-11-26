class Names(object):
    ''' contains static methods for standard operation with names.json file
        names.json holds local database with mapping which helps to exchange information
        between different systems. file format:
            - index:        string representation of an entity;
            - "canonical":  canonical name of an entity
            - "origin":     where the index entriy was originally taken from'''

    @staticmethod
    def rev_map(t_names):
        import pandas as pd
        '''reversive mapping: determine for which caninonical names there is no entry in index
           and add simple entry where index and "canonical" value are equal. Origin is
           designated as "canonical_reverse". This ensures that canonical name is always resolved
           into itself'''
        t_mapped_names = t_names[~t_names['canonical'].isnull()]
        t_mapped_names_not_mapped = t_mapped_names[~t_mapped_names['canonical'].isin(t_names.index)]['canonical'].unique()
        #t_names.append(pd.DataFrame(index=t_mapped_names_not_mapped))
        t_addition = pd.DataFrame(t_mapped_names_not_mapped, index=t_mapped_names_not_mapped, columns=['canonical'])
        t_addition['origin'] = 'canonical_reverse'
        t_combined = t_names.append(t_addition, verify_integrity=True)
        return(t_combined)
