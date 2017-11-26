import unittest
import acc_rec 


test_data_file = 'test_acc_rec_data.txt'
test_data_file_encod = 'windows-1251'

settings = dict(
       index   = ['source','sub2'],
       columns = 'ccenter',
       values  = 'usd')

class AccRecTestCase(unittest.TestCase):
    '''test for method 'import_data' is omitted '''
    def setUp(self):
        self.pt = acc_rec.pt()
        self.pt.import_data(test_data_file, test_data_file_encod)

    def test_get_pivot(self):
        self.pt.get_pivot('common', settings)
        self.assertEqual(self.pt.pivot.index.names,  settings['index'])

    def test_get_pivot_with_filters(self):
        self.pt.pivots.add( settings, 'test')  
        self.pt.pivots.set_filter(dict(source = 'Dorson Ltd.'), 'test')
        self.pt.get_pivot('test')
        self.assertEqual(self.pt.pivot.index.levels[0][0], 'Dorson Ltd.')

    def test_copy_settings(self):
        self.pt.get_pivot('common', settings)
        self.pt.pivots.clone('test')
        self.assertTrue(self.pt.pivots['test'] == self.pt.pivots['common'])

    def test_settings_save(self):
        file_name = '/tmp/acc_rec_test_sett_store'
        #copy from test_get_pivot_with_filters - to test composition/decomposition
        #of stored object (__filters__)
        self.pt.pivots.add( settings, 'test')  
        self.pt.pivots.set_filter(dict(source = 'Dorson Ltd.'), 'test')
        self.pt.get_pivot('test')

        pivot_a = self.pt.get_pivot('test')
        self.pt.settings_save(file_name)
        #drop the app altogether and create from scratch new instance
        del self.pt
        self.setUp()
        #
        self.pt.settings_restore(file_name)
        pivot_b = self.pt.get_pivot('test')
        self.assertTrue(pivot_a.equals(pivot_b))




if __name__ == '__main__':
    unittest.main()
