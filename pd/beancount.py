import csv
import pandas as pd
import re
import datetime


re_split_space = re.compile(r'\s+')

def csv2df(csvdata, columns, delimeter=',', dtypes={}):
    '''
    csvdata shall not include column labels and index
    columns: list of str values
    dtypes: {index:keyword}
            keywords: position, date
    '''
    records = []
    flag_columns = True

    for row in csv.reader(csvdata, skipinitialspace=True):
        #remove spaces
        for i in range(len(row)):
            row[i] = row[i].strip()
        #split "position" data type into two columns: amount and curr
        for k,v in dtypes.items():
            if v.count('position'):
                try:
                    amount,commodity =  re_split_space.split(row[k])
                except IndexError:
                    print('wrong position_types value')

                row[k] = float(amount)
                row.insert(k+1, commodity)
                if flag_columns:
                    columns.insert(k+1, columns[k] + "_curr")
                    columns[k] = columns[k] + "_amount"
                    flag_columns = False
            elif v.count('date'):
                date = datetime.date.fromisoformat(row[k])
        records.append(row)

    return pd.DataFrame(records, columns=columns)

