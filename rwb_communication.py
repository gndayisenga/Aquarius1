'''
Created on Nov 30, 2022

@author: USER
'''

import smtplib

class rwb_communication():
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self._from_email = 'alerts@rwb.rw'
        self._from_pwd = 'RWB@2023!!'
        self._server = 'mail.rwb.rw'
        self._port = 587
    def send_email(self, recipients, message, subject):
        from_email = self._from_email
        password = self._from_pwd
        email_server = self._server
        port = self._port
        if type(recipients) != list:
            r = recipients.split(",")
        else:
            r = recipients
        to_email = ", ".join(r)
        print(to_email)
        smtpserver = smtplib.SMTP(email_server, port)
        smtpserver.ehlo()
        smtpserver.starttls()
        smtpserver.ehlo()
        smtpserver.login(from_email, password)
        header = 'To: ' + to_email + '\n' + 'From: ' + from_email + '\n' + 'Subject: ' + subject + '\n'
        mmsg = header + '\n' + message + '\n'
        smtpserver.sendmail(from_email, recipients, mmsg)
        smtpserver.close()
        print("Email successfully sent.")