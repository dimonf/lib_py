import datetime
import email
import imaplib
import os
import re
import sys

#requires python3. for python2 replace  message_from_bytes with message_from_string

class IMAP():
    att_dir = 'download_att'
    re_num_uid = re.compile(r'^([0-9]+)\s+.*\bUID\s+([0-9]+).*')
    re_att_filename = re.compile(r'filename["\s]+(=[^"]+)')
    re_att_filesize = re.compile(r'base64["\s]+([0-9]+)')


    def __init__(self, *args, **kwargs):
        self._host, self._user, self._password, self._box = ['']*4
        self.connect_ssl(*args, **kwargs)

    def bnumbers2str(self, numbers):
        ''' get messages numbers, returned by IMAP server to SEARCH command and
            convert byte message to comma-separated string'''
        try:
            numbers = ','.join([s.decode() for s in numbers[0].split()])
        except:
            return numbers
        return numbers

    def decode(self, b):
        '''attempt to decode a value'''
        try:
            return b.decode()
        except:
            return b

    def get_num_UID(self, part):
        '''parse both, number and uid'''
        num_uid = self.re_num_uid.match(part)
        if num_uid:
            return num_uid.groups()
        else:
            return ()

    def parse_bodystructure(self, data, part):
        if part == "attachments":
            out = []
            fnames_r = re.findall(self.re_att_filename, data)
            for n in fnames_r:
                out.append(str(email.header.make_header(
                    email.header.decode_header(n))))
        if part == 'attachments_size':
            out = []
            sizes = re.findall(self.re_att_filesize, data)
            for s in sizes:
                out.append('{}Kb'.format(int(int(s)/1364)))
        return out

    def get_date(self, data):
        local_date_str = ''
        date_tuple = email.utils.parsedate_tz(data)
        if date_tuple:
           local_date = datetime.datetime.fromtimestamp(
                 email.utils.mktime_tz(date_tuple))
           local_date_str = local_date.strftime("%Y-%b-%d %H:%M")
        return local_date_str


    def connect_ssl(self, host=None, user=None, password=None, box=None, readonly=False):
        host = host if host else self._host
        user = user if user else self._user
        password = password if password else self._password
        box = box if box else self._box

        box = box if box else 'INBOX'

        self.con = imaplib.IMAP4_SSL(host)
        self.con.login(user, password)
        self.con.select(box, readonly)
        #
        (self._host, self._user, self._password, self._box) = (
            host, user, password, box)

        return self.con

    def search(self, imap_filter='UNSEEN'):
        '''
        imap_filter:
            (SUBJECT "test message 2")
            'ALL'
            (FROM "doug" SUBJECT "test message")
            '[UN]SEEN'
        output:
            mail|str|text
        query_str:
            (BODY.PEEK[HEADER] FLAGS)   #just peek header without changing enything
            (BODY.PEEK[HEADER] UID) #get UID of the message (rfc3501 2.3.1)
            (RFC822)    #the whole message
        '''

        resp, numbers = self.con.search(None, imap_filter)
        if resp != 'OK':
            return 'No message found'

        self.numbers = self.bnumbers2str(numbers)
        return self.numbers

    def list(self, numbers=None, columns='num,uid,message-id,date,subject,from,att'):
        query_str = '(BODY.PEEK[HEADER] BODYSTRUCTURE UID)'

        if not numbers:
            numbers = self.numbers
        else:
            self.numbers = self.bnumbers2str(numbers)

        resp,data = self.con.fetch(self.numbers, query_str)
        records = []
        #select only even items in array
        for i in range(0, len(data), 2):
            d = {}
            message = email.message_from_bytes(data[i][1])
            message_h = data[i][0].decode()
            bodystructure = data[i+1].decode()
            d['num'],d['uid'] = self.get_num_UID(message_h)
            d['message_id'] = message['Message-ID']
            #d['date'] = message['Date']
            d['date'] = self.get_date(message['Date'])
            d['subject'] = str(email.header.make_header(
                email.header.decode_header(message['Subject'])))
            d['from'] = message['From']
            d['attachments'] = list(zip(
                self.parse_bodystructure(bodystructure,'attachments'),
                self.parse_bodystructure(bodystructure,'attachments_size')
            ))
            records.append(d)
        return records
    def get_attachments(self, numbers, dir=None, dry_run=None):
        for n in numbers:
            rep, data = self.con.fetch(n, '(RFC822)')
            if not rep:
                continue
            



    def get_attachments_old(self, imap_filter='UNSEEN', dir=None, dry_run=None, exclude_subj=None):
        '''just to be a bit more memory-vise efficient, pre-fetch data in bytes
           and convert it to an object upon processing, so at any time only one "mail"
           object eats memory
        '''
        mm = self.search(imap_filter = imap_filter, output='txt', query_str='(RFC822)')
        if not dir:
            dir_path = os.path.join(os.path.expanduser('~'), 'Downloads', self.att_dir)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

        for m in mm:
            m = email.message_from_bytes(m)
            fl_exclude=''
            if m.get_content_maintype() != 'multipart':
                continue

            if exclude_subj:
                if re.search(exclude_subj, m['Subject'], re.IGNORECASE):
                    fl_exclude = ' EXCLUDED '

            print(fl_exclude + m["Date"] + " [" + m["From"] + "] :" + m["Subject"])
            if fl_exclude:
                continue

            for part in m.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue

                file_name = part.get_filename()
                file_path = os.path.join(dir_path, file_name)

                file_error=''
                if not os.path.isfile(file_path):
#                    fp = open(file_path, 'wb')
#                    if dry_run:
#                        fp.close()
#                        os.remove(file_path)
                    if not dry_run:
                        fp = open(file_path, 'wb')
                        try:
                            fp.write(part.get_payload(decode=True))
                        except TypeError:
                            file_error = 'filetype ERROR: '
                        fp.close()
                    print(file_error+'   file: ' + file_path)
                else:
                    print("write error, file exists: " + file_path)



