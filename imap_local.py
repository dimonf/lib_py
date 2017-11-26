import imaplib
import email
import os
import sys
import re

#requires python3. for python2 replace  message_from_bytes with message_from_string

class IMAP():
    att_dir = 'download_att'

    def __init__(self, *args, **kwargs):
        self.connect(*args, **kwargs)

    def connect(self, host, user, password, box='INBOX'):
        self.con = imaplib.IMAP4_SSL(host)
        self.con.login(user, password)
        self.con.select(box)

    def search(self, imap_filter='UNSEEN', output='mail', query_str='(BODY.PEEK[HEADER])'):
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
            (RFC822)    #the whole message
        '''

        msgs = []
        resp, items = self.con.search(None, imap_filter)
        if resp != 'OK':
            return 'Error'
        items = items[0].split()

        for id in items:
            resp, data = self.con.fetch(id, query_str)
            email_body = data[0][1]
            if output == 'mail':
                mail = email.message_from_bytes(email_body)
                msgs.append(mail)
            elif output in ['str','text', 'txt']:
                msgs.append(email_body)

        return msgs

    def get_attachments(self, imap_filter='UNSEEN', dir=None, dry_run=None, exclude_subj=None):
        '''just to be a bit more memory-vise efficient, pre-fetch data in bytes
           and convert it to an object upon processing, so at any time only one "mail"
           object eats memory
        '''
        mm = self.search(imap_filter = imap_filter, output='txt', query_str='(RFC822)')
        if not dir:
            dir_path = os.path.join(os.path.expanduser('~'), 'Downloads', self.att_dir)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

        for m  in mm:
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
                    fp = open(file_path, 'wb')
                    if dry_run:
                        fp.close()
                        os.remove(file_path)
                    else:
                        try:
                            fp.write(part.get_payload(decode=True))
                        except TypeError:
                            file_error = 'filetype ERROR: '
                        fp.close()
                    print(file_error+'   file: ' + file_path)
                else:
                    print("write error, file exists: " + file_path)


