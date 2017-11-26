import unittest, os, sys
import pandas as pd
from my_pd.ppe import Ppe


class PPE_TestCase(unittest.TestCase):
    def setUp(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        data_file = os.path.join(current_dir,'testdata_ppe.csv')
        #import pdb; pdb.set_trace()
        self.ppe = Ppe(data_file)

    def test_temp(self):
        self.assertTrue(len(self.ppe.df_tr)>0,
            'Size of dataframe shall not be zero')


