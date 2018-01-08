def d2obj(d):
    """Convert a dict to an object

    >>> d = {'a': 1, 'b': {'c': 2}, 'd': ["hi", {'foo': "bar"}]}
    >>> obj = dict2obj(d)
    >>> obj.b.c
    2
    >>> obj.d
    ["hi", {'foo': "bar"}]
    """
    class Object:
        '''dumb object serves as building block '''
        pass

    try:
        d = dict(d)
    except (TypeError, ValueError):
        return d
    obj = Object()
    for k, v in d.items():
        obj.__dict__[k] = d2obj(v)
    return obj

def grep_list(l, s):
    import re

    re_compiled = re.compile(s)
    return [v for v in l if re_compiled.search(v)]

def get_name_dict(names):
    '''returns dict mapping of nicknamed versions of given array (names)'''
    def rectify(n):
        return n.replace(' ','_').replace('(','').replace(')','').replace('-','_').replace('__','_').lower()

    len_default = 5
    names_d = dict()
    for n in names:
        t_n = rectify(n[:len_default])
        if not t_n in names_d:
            names_d[t_n] = n
        else:
            #TODO: call itself recursively
            t_n = t_n + "_"+rectify(n[-len_default:])
            t_n = rectify(t_n)
            if t_n in names_d:
                raise KeyError("can'f generate proper nickname for "+n+
                    "\n the name "+t_n +" seems to be aloread in the lsit")
            names_d[t_n] = n
    return names_d