import email
import imaplib
import os

class FetchEmail():

    #con = None
    #error = None

    def __init__(self, mail_server, username, password):
        self.con = imaplib.IMAP4_SSL(mail_server)
        self.con.login(username, password)
        self.con.select(readonly=True) # do not change read status

    def close_connection(self):
        """
        Close the connection to the IMAP server
        """
        self.con.close()

    def save_attachment(self, msg, download_folder="/tmp"):
        """

        Given a message, save its attachments to the specified
        download folder (default is /tmp)

        return: file path to attachment
        """
        att_path = "No attachment found."
        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue

            filename = part.get_filename()
            att_path = os.path.join(download_folder, filename)

            if not os.path.isfile(att_path):
                fp = open(att_path, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()
        return att_path

    def get_messages(self, filter="ALL"):
        result, messages = self.con.search(None, filter)


    def fetch_messages(self, imap_filter='ALL',
         imap_format='BODY.PEEK[HEADER])',
         save_attachments = False):
        """
        Retrieve all messages
        imap_filter: (UNSEEN)
          (FROM "Doug" SUBJECT "test message 2")
        imap_format: (BODY.PEEK[HEADER])
           (RFC822)
        """
        emails = []
        result, messages = self.con.search(None, imap_filter)
        if result == "OK":
            for message in messages[0].split():
                try:
    #                ret, data = self.con.fetch(message,'(RFC822)')
                    ret, data = self.con.fetch(message, imap_format)
                    print(data)
                except:
                    print( "No emails found.")
                    self.close_connection()
                    exit()

                continue

                msg = email.message_from_string(data[0][1])
                if isinstance(msg, str) == False:
                    if save_attachments == True:
                        self.save_attachment(msg)
                    else:
                        print(msg)
                        #emails.append(msg)
                #response, data = self.con.store(message, '+FLAGS','\\Seen')

            return emails

        self.error = "Failed to retreive emails."
        return emails

    def parse_email_address(self, email_address):
        """
        Helper function to parse out the email address from the message

        return: tuple (name, address). Eg. ('John Doe', 'jdoe@example.com')
        """
        return email.utils.parseaddr(email_address)
