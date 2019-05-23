def get_kernel_id():
   '''get kernel ID for use with "jupyter console --existing <ID>"
      This script shall be run within notebook of interest '''
   import ipykernel

   try:
       kernel_id = ipykernel.connect.get_connection_file().split('/')[-1]
   except:
       return('no MultiInstance support')

   #dump ID to predefined file
   with open('/tmp/jup_id','w') as f:
         f.write(kernel_id)
   return kernel_id

def get_closest_dir_by_function(test_function, start_dir=None):
    '''get closest parent directory, which conforms to the test_function.
       test_function must take one positional argument: testing 
       directory, in txt fomrat, and return bool True/False'''
    import os
    
    if not start_dir:
        start_dir = os.getcwd()
    
    test_dir = start_dir
    target_dir = None
    
    while not target_dir:
        #print('testing '+ test_dir)
        if test_function(test_dir):
            return test_dir
        next_up_dir = os.path.dirname(test_dir)
        if next_up_dir == test_dir:
            raise Exception('file not found')
        test_dir = next_up_dir
            
    return target_dir 

def get_root_dir():
    '''Get root dir from any notebook which is situated below it.
       The root dir is designated by file root_indicator_file_name'''
    import os
    
    root_indicator_file_name = '.jupyter_root_dir'
    def test_function(test_dir):
       return os.path.exists(os.path.join(test_dir, root_indicator_file_name))

    return get_closest_dir_by_function(test_function)

def get_git_dir():
    '''Get (nearest) git root directory'''
    import os

    def test_function(test_dir):
        return(os.path.isdir(os.path.join(test_dir, '.git')))

def get_closest_file(name):
    '''get the closest file (or dir) by the name within full path of cwd'''
    import os
    
    def test_function(test_dir):
        return os.path.exists(os.path.join(test_dir, name))
    
    target_dir = get_closest_dir_by_function(test_function)
    return os.path.join(target_dir, name)
