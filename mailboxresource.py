#!/usr/bin/env python
#-*- coding:utf-8 -*-

from __future__ import print_function

import imaplib, email
import re
import json, os
import hashlib
from message import Message
import datetime
from dateutil.parser import parse
from elasticsearch import Elasticsearch

class MailboxClient:
    """Operations on a mailbox"""

    def __init__(self, host, port, username, password, remote_folder):
        self.mailbox = imaplib.IMAP4_SSL(host, port)
        self.mailbox.login(username, password)
        # self.mailbox.select(remote_folder, readonly=True)
        self.mailbox.select(remote_folder, readonly=False)

    def copy_emails(self, days, local_folder, wkhtmltopdf, es_host, kibana_host):

        n_saved = 0
        n_exists = 0

        self.local_folder = local_folder
        self.wkhtmltopdf = wkhtmltopdf
        self.es_host = es_host
        self.kibana_host = kibana_host

        criterion = 'ALL'

        if days:
            date = (datetime.date.today() - datetime.timedelta(days)).strftime("%d-%b-%Y")
            criterion = '(SENTSINCE {date})'.format(date=date)

        # typ, data = self.mailbox.search(None, criterion)
        typ, data = self.mailbox.search(None, 'UnSeen')
        for num in data[0].split():
            typ, data = self.mailbox.fetch(num, '(RFC822)')
            if self.saveEmail(data):
                typ, data = self.mailbox.store(num, '+FLAGS','\\Seen')
                n_saved += 1
            else:
                n_exists += 1

        return (n_saved, n_exists)


    def cleanup(self):
        self.mailbox.close()
        self.mailbox.logout()


    def getEmailFolder(self, msg, data):
        if msg['Message-Id']:
            foldername = re.sub('[^a-zA-Z0-9_\-\.()\s]+', '', msg['Message-Id'])
        else:
            foldername = hashlib.sha224(data).hexdigest()

        # year = 'None'
        date = 'None'
        if msg['Date']:
            # match = re.search('\d{1,2}\s\w{3}\s(\d{4})', msg['Date'])
            # if match:
            #     year = match.group(1)
            date=parse(msg['Date']).strftime("%Y-%m-%d")
            month=parse(msg['Date']).strftime("%Y-%m")

        # return os.path.join(self.local_folder, year, foldername)
        return os.path.join(self.local_folder, date, foldername), month



    def saveEmail(self, data):
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = ""
                try:
                    msg = email.message_from_string(response_part[1].decode("utf-8"))
                except:
                    print("couldn't decode message with utf-8 - trying 'ISO-8859-1'")
                    msg = email.message_from_string(response_part[1].decode("ISO-8859-1"))

                directory, month = self.getEmailFolder(msg, data[0][1])

                if os.path.exists(directory):
                    return False

                os.makedirs(directory)

                try:
                    message = Message(directory, msg, self.kibana_host)
                    message.createRawFile(data[0][1])
                    message.createMetaFile()
                    message.extractAttachments()

                    if self.wkhtmltopdf:
                        message.createPdfFile(self.wkhtmltopdf)
                    
                    es = Elasticsearch([{'host': self.es_host, 'port': '9200'}])
                    file = directory + '/metadata.json'
                    f = open(file,'r')
                    index='azfolder-' + month
                    es.index(index=index, ignore=400, doc_type='message', body=json.load(f))
                    err = os.system('cd %s && tar -czf bundle.tar.gz ./* && rm -rf attachment* me* raw*'%directory)


                except Exception as e:
                    # ex: Unsupported charset on decode
                    print(directory)
                    if hasattr(e, 'strerror'):
                        print("MailboxClient.saveEmail() failed:", e.strerror)
                    else:
                        print("MailboxClient.saveEmail() failed")
                        print(e)

        return True


def save_emails(account, options):
    mailbox = MailboxClient(account['host'], account['port'], account['username'], account['password'], account['remote_folder'])
    stats = mailbox.copy_emails(options['days'], options['local_folder'], options['wkhtmltopdf'], options['es_host'], options['kibana_host'])
    mailbox.cleanup()
    print('{} emails created, {} emails already exists'.format(stats[0], stats[1]))


def get_folder_fist(account):
    mailbox = imaplib.IMAP4_SSL(account['host'], account['port'])
    mailbox.login(account['username'], account['password'])
    folder_list = mailbox.list()[1]
    mailbox.logout()
    return folder_list
