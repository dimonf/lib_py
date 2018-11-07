import csv
import pandas as pd
import re
import datetime
import beancount


re_split_space = re.compile(r'\s+')

def csv2dataframe(csvdata, columns, delimeter=',', dtypes={}):
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

def normalise(data):
    '''
    data comes in structure:
      - beancount...ResultRow:
         - str
         - Inventory
              .currencies -> set {'USD','EUR'}, 
              .get_currency_units('USD') -> beancount...Amount,
                  .currency -> str 'USD'
                  .number -> Decimal('-55424.69')
              .get_positions() -> 
                 [Position, Position ...]
                 Position.units ->  beancount...Amount,

    '''
    def check_number_of_Inventory(row):
        inv_items = [1 for item in row if isinstance(item, beancount.core.inventory.Inventory)]
        return sum(inv_items)

    records = []
    for row in data:
        #only single occurence of Inventory data type is allowed in a row. check only once
        if len(records) == 0 and check_number_of_Inventory(row) > 1:
            raise ValueError('''beancount.core.inventory.Inventory (e.g. position) appears
            more than once in source data set! Aborting''')

        records.append([])
        #row is of ResultRow data type
        col_index = 0
        col_index_inventory = 0
        inventory_cache = []
        for item in row:
            if isinstance(item, beancount.core.inventory.Inventory):
                i = 0
                for p in item.get_positions():
                    if i==0:
                        #remember column index for inventory item
                        col_index_inventory = col_index
                        #
                        records[-1].append(p.units.number)
                        records[-1].append(p.units.currency)
                        col_index += 2
                    else:
                        #store values in 'cache' to populate output dataset later
                        inventory_cache.append({'number':p.units.number,
                                                'currency':p.units.currency})
                    i += 1
            else:
                records[-1].append(item)
                col_index += 1
        #upon completion of processing of a row of beancount data, broadcast
        first_record_index = len(records) - 1
        for inventory_r in inventory_cache:
           records.append(records[first_record_index])
           records[-1][col_index_inventory] = inventory_r['number']
           records[-1][col_index_inventory+1] = inventory_r['currency']

    return(records)

def raw2dataframe(data, columns):
    data = normalise(data)
    return pd.DataFrame(data, columns=columns)

def query2dataframe(datafile, query, columns):
    '''please note that beancount source shall be modified in order to
       handle "raw" output format '''
    import beancount.query.shell as bean_shell
    data = bean_shell.main([ '-f', 'raw', datafile, query])

    return raw2dataframe(data, columns)
