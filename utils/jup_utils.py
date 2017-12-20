def get_kernel_id():
   '''get kernel ID for use with "jupyter console --existing <ID>"
      This script shall be run within notebook of interest '''
   import ipykernel
   kernel_id = ipykernel.connect.get_connection_file().split('/')[-1]
   #dump ID to predefined file
   with open('/tmp/jup_id','w') as f:
         f.write(kernel_id)
   return kernel_id
