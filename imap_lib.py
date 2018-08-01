import datetime
import email, email.parser
import imaplib
import os
import re
import sys

''' use example:
    acc = IMAP_account(host='some.com',user='toster@some.com',password='1234')
    acc.list_mboxes()
    acc.search(
 
'''
class IMAP_account():
    #source code from https://pymotw.com/3/imaplib/
    att_dir = 'download_att'
    re_num_uid = re.compile(r'^([0-9]+)\s+.*\bUID\s+([0-9]+).*')
    re_att_filename = re.compile(r'filename["\s]+(=[^"]+)')
    re_att_filesize = re.compile(r'base64["\s]+([0-9]+)')
    re_box = re.compile(r'\((?P<flags>.*?)\) "(?P<delimiter>.*)" (?P<name>.*)')

    def __init__(self, host, user, password):
        self._host, self._user, self._password, self._box = ['']*4
        self.par = {}
        self.connect_ssl(host, user, password)

    def connect_ssl(self, host=None, user=None, password=None, mbox=None):
        self.set_l_vals(locals())
        try:
            self.con.close()
            self.con.logout()
        except:
            pass

        self.con = imaplib.IMAP4_SSL(self.par['host'])
        self.con.login(self.par['user'], self.par['password'])
        if mbox:
            self.conn.select(mbox)
        #get list of mailboxes
        return self.list_mboxes()

    def list_mboxes(self):
        self.check_conn()
        resp,data = self.con.list('""','*')
        if resp != "OK":
            raise Exception('unable to obtain list of mailboxes from the server')
        return [self.parse_list_response(l) for l in data]

    def status_mbox(self, mbox, format_str='(MESSAGES RECENT UIDNEXT UIDVALIDITY UNSEEN)'):
        return self.con.status('"{}"'.format(mbox), format_str)

    def search(self, search_str):
        '''don't forget to select mailbox first: self.con.select('INBOX') '''
        self.check_conn()
        self.set_l_vals(locals())
        resp, [response] = self.con.uid('search', search_str)
        return [u for u in response.decode('utf-8').split(' ')]

    def search_all_mboxes(self, search_str):
        ''' returns dict {mbox_name : [uid,uid,uid]'''
        typ, mbox_data = self.con.list()
        out = {}
        for line in mbox_data:
            flags, delimeter, mbox_name = self.parse_list_response(line)
            self.con.select('"{}"'.format(mbox_name), readonly=False)
            out[mbox_name] = self.search(search_str)

    def b2dict_headers(self, msg_data, headers=['subject','to','from'], attachments=False):
        '''converts binary imap return to structured dataset. uid and date is always extracted'''
        out = []
        for i in ['uid','date']:
            if not i in headers:
                headers.insert(0, i)

        for response_part in msg_data:
            if isinstance(response_part, tuple):
                #get message uid

                #
                msg_str= {}
                email_parser = email.parser.BytesFeedParser()
                email_parser.feed(response_part[1])
                msg = email_parser.close()
                for header in headers:
                    try:
                        v = msg[header]
                        msg_str[header] = self.extra_formatting(header, v)
                    except:
                        pass
                out.append(msg_str)
        return out



    #### P A R S E R S ##############
    def extra_formatting(self, header, val):
        if header == ['date']:
            return self.get_date(val)
        return val

    def get_date(self, data):
        local_date_str = ''
        date_tuple = email.utils.parsedate_tz(data)
        if date_tuple:
           local_date = datetime.datetime.fromtimestamp(
                 email.utils.mktime_tz(date_tuple))
           local_date_str = local_date.strftime("%Y-%b-%d %H:%M")
        return local_date_str

    def parse_list_response(self, line):
        match = self.re_box.match(line.decode('utf-8'))
        flags, delimiter, mbox_name = match.groups()
        mbox_name = mbox_name.strip('"')
        return (flags, delimiter, mbox_name)

    def parse_attachments_fname(self, data):
        out = []
        fnames_r = re.findall(self.re_att_filename, data)
        for n in fnames_r:
            out.append(str(email.header.make_header(
                email.header.decode_header(n))))
        return out

    def parse_attachments_size(self, data):
        out = []
        sizes = re.findall(self.re_att_filesize, data)
        for s in sizes:
            out.append('{}Kb'.format(int(int(s)/1364)))
        return out

    #### I N T E R N A L S #############
    def check_conn(self):
        ''' check if there is active connection'''
        if not self.con:
            raise Exception('No established connection detected')

    def set_l_vals(self,vars):
        '''s '''
        for k,v in vars.items():
            if k == 'self':
                continue
            elif v:
                self.par[k] = v


