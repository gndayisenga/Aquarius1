'''
Created on Nov 30, 2022

@author: USER
'''
import pandas as pd
from rwb_communication import rwb_communication
from rwb_aquarius_data_analysis import rwb_aquarius_data_analysis
import datetime

def read_locationID_file(file):
    locs = []
    if 'xls' in file:
        data = pd.read_excel(file)
        locs = data['location_id'].tolist()
    else:
        data = [line.rstrip('\n') for line in open(file)]
        for line in data:
            tp = line.split('\t')
            locs.append(tp[0])
    return locs

def read_recipients_file(file):
    emails = []
    if 'xls' in file:
        data = pd.read_excel(file)
        emails = data['recipients'].tolist()
    else:
        data = [line.rstrip('\n') for line in open(file)]
        for line in data:
            tp = line.split('\t')
            emails.append(tp[0])
    return emails

if __name__ == '__main__':
    url = 'http://10.10.82.215/AQUARIUS/'
    f = r'C:\Users\USER\Documents\RWB\aquarius_thresholds.txt'
    r = r'C:\Users\USER\Documents\RWB\aquarius_thresholds_emails.txt'
    locs = read_locationID_file(f)
    recipients = read_recipients_file(r)
    startDate = (datetime.datetime.now() - datetime.timedelta(days = 20)).strftime('%Y-%m-%d %H:%M')
    endDate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    db = rwb_aquarius_data_analysis(url, 'User3', 'iwrm@2021!')
    sendEmail = False
    emailText = ''
    for loc in locs:
        print(loc)
        sendMail, eText = db.check_exceedance_for_email(loc, startDate, endDate)
        if sendMail == True:
            sendEmail = True
        emailText = emailText + eText + '\n'
    if sendEmail:
        com = rwb_communication()
        subject = 'AQUARIUS Exceedance Alert'
        com.send_email(recipients, emailText, subject)
        