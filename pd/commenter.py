import csv, re, datetime, pandas as pd

class Commenter():
    fieldnames = ['id','comment', 'time']
    sep = '|'

    def __init__(self, file, filter=''):
        self.file = file
        self.prefix = filter

    def __call__(self, obj, comment=False):

        if type(obj) == pd.core.frame.DataFrame:
            return self.__run_me(index=obj.index, comment=comment)

        try:
            iter(obj)
            return self.__run_me(index=obj, comment=comment)
        except TypeError:
            return self.__run_me(index=[obj], comment=comment)

    def __run_me(self, index, comment=False):
        '''
        create or return a comment for an object.
        Suitable objects:
            - pandas.DataFrame of 1 or more rows
        '''
        if '' in index:
            raise KeyError('index value cannot be empty')
        ids = ['{}/{}'.format(self.prefix, id) for id in index]
        if comment == False:
            '''find and return comment '''
            records = self.__get_records(ids=ids)
            return [(row['comment'], row['id'], row['time']) for row in records]

        elif comment == '!rm':
            ''' delete comment from the file '''
            if len(ids) == 0:
                raise ValueError('record amendment cannot be done without specifying id')
            records = self.__get_records(ids=ids, invert=True)
            self.__dump_records(records)

        else:
            ''' write comment to the file '''
            #print(ids)
            if len(ids) == 0:
                raise ValueError('record amendment cannot be done without specifying id')
            records = self.__get_records(ids=ids, invert=True)
            for i in ids:
                records.append(dict(
                    id      = i,
                    comment = comment,
                    time    = datetime.datetime.now().isoformat()[:-7]
                ))
            self.__dump_records(records)

    def __dump_records(self, records):
        with open(self.file, 'w') as fh:
            writer = csv.DictWriter(fh, fieldnames=self.fieldnames, delimiter=self.sep)
            writer.writerows(records)

    def __get_records(self, ids=[], invert=False):
        records = []
        try:
            fh = open(self.file)
        except FileNotFoundError:
            return []

        reader = csv.DictReader(fh, delimiter=self.sep, fieldnames=self.fieldnames)
        records = []
        for row in reader:
            if len(ids) == 0:
                records.append(row)
            elif invert:
                if not row['id'] in ids:
                    records.append(row)
            elif row['id'] in ids:
                records.append(row)

        return records

    def info(self):
        #get some statistics
        def print_date(val):
            #print(val)
            if type(val) == datetime.datetime:
                #print ('val :'+str(val) + 'is datetime')
                return val.isoformat()
            else:
                return ''

        def rectify_dict(d):
            return{k:print_date(v) for k,v in d.items()}


        context_count    = {}
        context_time_min = {}
        context_time_max = {}
        time_min         = datetime.datetime.now()
        time_max         = datetime.datetime(1999,1,1)
        for row in self.__get_records():
            context,ind   = row['id'].split('/')
            dtime = datetime.datetime.strptime(row['time'],'%Y-%m-%dT%H:%M:%S')
            if context_time_max.get(context, datetime.datetime(1999,1,1)) < dtime:
                context_time_max[context] = dtime
            if context_time_min.get(context, datetime.datetime.now()) > dtime:
                context_time_min[context] = dtime
            context_count[context] = context_count.get(context,0) + 1
            if time_max < dtime:
                time_max = dtime
            if time_min > dtime:
                time_min = dtime


        if len(context_count) == 0:
            return {'warning': 'file is empty'}

        return dict(
            file            = self.file,
            delimiter       = self.sep,
            context_current = self.prefix,
            context_num        = context_count,
            time_min = print_date(time_min),
            time_max = print_date(time_max),
            context_time_min = rectify_dict(context_time_min),
            contex_time_max = rectify_dict(context_time_max)
        )
