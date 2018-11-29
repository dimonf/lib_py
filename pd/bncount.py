import csv
import pandas as pd
import re
import datetime
import beancount
import os
import beancount.query.shell as bean_shell

class Bncount():
    def __init__(self, datafile):
        self.datafile = datafile
        self.columns = []
        if not os.path.isfile(self.datafile):
            raise Exception('file '+ self.datafile + ' error') 

    def query2df(self, query, columns=None):
        data = self.query(columns=None, query=query)
        data = self.normalize(data)
        if not columns and self.columns:
            columns = self.columns
        return pd.DataFrame(data, columns=columns)

    def query(self, query, columns=None):
        '''please note that beancount source shall be modified in order to
           handle "raw" output format '''
        self.query_str = query
        return bean_shell.main([ '-f', 'raw', self.datafile, self.query_str])

    def normalize(self, data):
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
            inv_items = [1 for item in row if isinstance(item, 
                beancount.core.inventory.Inventory)]
            return sum(inv_items)

        def add_label(records, col_template, items_template):
            '''column labels migrate from col_template to self.columns
            col_template: ['account','position','sum_convert_cost_position_c__date']
                         derived from beancount.query.query_execue.ResultRow._fields
            items_template: format string for str.format method. for one input item
                            in col_template may produce multiple items
            '''
            if len(records) != 1:
                return
            col_labels = items_template.format(col_template.pop(0))
            self.columns.extend(col_labels.split(','))


        records = []
        self.columns = []
        for row in data:
            #only single occurence of Inventory data type is allowed in a row. check only once
            if len(records) == 0 and check_number_of_Inventory(row) > 1:
                raise ValueError('''beancount.core.inventory.Inventory (e.g. position) appears
                more than once in source data set! Aborting''')

            records.append([])
            if len(records) == 1:
                col_template = list(row._fields) #beancount.query.query_execute.ResultRow._fields
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
                            records[-1].append(float(p.units.number))
                            records[-1].append(p.units.currency)
                            col_index += 2
                            #
                            add_label(records, col_template, '{0},{0}_curr')
                        else:
                            #store values in 'cache' to populate output dataset later
                            inventory_cache.append({'number':p.units.number,
                                                    'currency':p.units.currency})
                        i += 1
                elif isinstance(item, beancount.core.position.Position):
                    records[-1].append(float(item.units.number))
                    records[-1].append(item.units.currency)
                    col_index += 2
                    #
                    add_label(records, col_template, '{0},{0}_curr')
                elif isinstance(item,beancount.core.amount.Amount):
                    records[-1].append(float(item.number))
                    records[-1].append(item.currency)
                    col_index += 2
                    #
                    add_label(records, col_template, '{0},{0}_curr')
                else:
                    records[-1].append(item)
                    col_index += 1
                    #
                    add_label(records, col_template, '{0}')
            #upon completion of processing of a row of beancount data, broadcast
            first_record_index = len(records) - 1
            for inventory_r in inventory_cache:
               records.append(records[first_record_index])
               records[-1][col_index_inventory] = inventory_r['number']
               records[-1][col_index_inventory+1] = inventory_r['currency']

        return(records)
