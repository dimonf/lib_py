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

def get_root_dir():
    '''returns root (top) directory relative path from any location within it.
       Some shared resources, such as bulky data files with imported records,
       are stored it predetermined location, e.g. [root]/data. It is convenient 
       to have unified access to such resources from code of any notebook, 
       located at any level within hirearchy. The root dir is indicated by 
       placement of a file .jupyter_root_dir into it'''
    import os
    
    root_indicator = '.jupyter_root_dir'
    next_up_dir = os.path.dirname(os.getcwd())
    root_dir = None
    while not root_dir:
        next_up_dir = os.path.dirname(next_up_dir)
        if os.path.exists(os.path.join(next_up_dir, root_indicator)):
            root_dir = next_up_dir
    return root_dir