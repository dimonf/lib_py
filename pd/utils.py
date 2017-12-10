import os, sys
from shutil import copyfile


class Data():
    '''manages data flow in our notebooks.
       tmp_dir: temporary files. ignored by VC
       data_dir: csv files optimized for VC (new records appended)
       files_dir: non-data files, subject to VC
       IMPORTED DATA EVOLUTION
        if external file exists, copy it to 'tmp' and serve cached filename 
        (consult 'read' method)
       '''

    tmp_dir = "tmp"
    data_dir = "data"
    files_dir = "files"
    def __init__(self, root_dir=None):
        if not root_dir:
            root_dir = os.path.curdir
        self.root_dir = root_dir
        #check working dir structure
        for dir in [self.tmp_dir, self.data_dir, self.files_dir]:
            dir_path = os.path.join(root_dir, dir)
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)

    def read(self, file, cached=False, no_cache=False):
        '''
        file: file name for import
        cached: try get 'local' cached version instead
        no_cache: do not make cached copy on read
        '''
        cached_file = os.path.join(self.root_dir,self.tmp_dir,os.path.basename(file))

        if cached:
            if not os.path.isfile(cached_file):
                raise FileNotFoundError("file " + cached_file + " was not found")
            return cached_file

        if os.path.isfile(file):
            #TODO: check whether file and cached_file are identical
            if not no_cache:
                copyfile(file, cached_file)
            return cached_file

        if os.path.isfile(cached_file):
            return cached_file

        raise FileNotFoundError("file "+ file + " was not found")



