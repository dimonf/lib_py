'''
report format:
date | balance b/f | change | balance c/f | interest % | days | interest (USD)

data file format:
loan_id | date | transaction_type | transaction_value | narrative

transaction_type: pmn-loan|pmn-interest|rate-change
transaction_value: paymet_amount | new rate
'''
import pandas as pd

class Loan():
    def __init__(self, input):
        if type(input) is str:
            self.read_from_file(input)
        else:
            self.data = self.parse_n_validate(data)

    def report_on(self, date):
        '''returns standard report on a given date'''

    def set_options(self, options):
        '''set filter (regex): id, lender, borrower '''

    def parse_n_validate(self, records):
        ''' extract and broadcast information from narrative '''
        t_df = pd.DataFrame.from_records(records,
                columns = 'loan_id date tr_id tr_val narr'.split())
        self.data = t_df

    def read_from_file(self, filename):
        ''' read text file '''
        records = []
        with open(filename) as fl:
            for line in  fl.readlines():
                if line == '' or line[0] == '#':
                    continue
                records.append(map(str.strip, line.split('|')))

        self.parse_n_validate(records)









