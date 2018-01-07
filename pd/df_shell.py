import cmd

''' this module simplifies interactive work with a dataframe.
    it's distinct features are:
        - basic one-line editor (to mangling existing data)
        - parser
        - completion mechanism
    These simplifies data entry/modification. The main intention is
    to make modification of existing dataframe as simple as it is
    with standard spreadsheet gui app.

df = pd.read_csv('export.txt')
sh = my_utils.pd_ext.shell
sh.open(df)
# INSIDE THE SHELL #
10832>columns<ENTER>
    a: date
    b: amount
    c: curr
    d: narr
    e: account
10832>columns comp:a,b,d<ENTER>
10832>exec df[10100:].head(3)[col_ls]<ENTER>
-------------------
             date   amount  curr           details account
10100  2017-12-22  -355.86   EUR      starting bal      24
10101  2017-12-22  2860.07   EUR     Forcon/salary      46
10102  2017-12-27    21.28   EUR  yperagora costas      24
-------------------
#from arg copies modifyable data of existing row (10102) for alteration
10832>append from:10102<ENTER>
       2017-12-22    24.10   EUR   'Kapsalos,VISA'<ENTER>
!POSTED
10833>exec df[10100:].head(4)[col_ls]<ENTER>
-------------------
             date   amount  curr           details
10100  2017-12-22  -355.86   EUR      starting bal
10101  2017-12-22  2860.07   EUR     Forcon/salary
10102  2017-12-27    21.28   EUR  yperagora costas
10100  2017-12-22  -355.86   EUR      starting bal
10833  2017-12-22    24.10   EUR     Kapsalos,VISA
-------------------
'''

class DfShell(cmd.Cmd):
    intro = 'Welcome to the dataframe data manipulation shell'
    prompt = '(df)'

    def __init__(self, df):
        super().__init__()
        self.df = df

    def do_append(self, arg):
        print(arg)
        print(type(arg))

    def complete_append(self, text, line, begidx, endidx):
        print(text)

    def do_shell(self, arg):
        print(arg)

    def do_exit(self, *args):
        self.close()
        return True

    def do_quit(self, *args):
        self.close()
        return True

    def close(self):
        print('bye')

