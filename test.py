'''
Created on Nov 29, 2022

@author: USER
'''
import requests
import datetime
import pandas as pd
import smtplib




def start_connection(url, username, password):
    headers = {"Username": username, "EncryptedPassword": password, "Locale":""}
    if "/aquarius/publish/v2/session" in url.lower():
        response = requests.post(url, headers)
        return response.text
    else:
        return "BAD URL"

def get_uniqueID_with_threshold(url, locationID, token):
    header = {'X-Authentication-Token': str(token), 'Accept': 'application/json'}
    param = {'LocationIdentifier': str(locationID)}
    uniqueIDs = {}
    uniqueIDsNames ={}
    if 'gettimeseriesdescriptionlist' in url.lower():
        response = requests.get(url, headers = header, params = param)
        x = response.json()
        for p in x['TimeSeriesDescriptions']:
            thresholds = p['Thresholds']
            if thresholds != []:
                uid = p['UniqueId']
                for threshold in thresholds:
                    trs = threshold['Periods']
                    referenceValueToUse = 0
                    maxDate = datetime.datetime(1, 1, 1)
                    for tr in trs:
                        ''' this is done to get the most recent threshold'''
                        startT = datetime.datetime.strptime(tr['StartTime'][:-14], "%Y-%m-%dT%H:%M:%S")
                        if startT >= maxDate:
                            referenceValueToUse = tr['ReferenceValue']
                            maxDate = startT
                    uniqueIDs[uid] = referenceValueToUse
                    uniqueIDsNames[uid] = p['Parameter'] + '-' + p['Label']
        return uniqueIDs, uniqueIDsNames
    else:
        return 'BAD URL', 'BAD URL'

def get_location_name(url, locationID, token):
    header = {'X-Authentication-Token': str(token), 'Accept': 'application/json'}
    param = {'LocationIdentifier': str(locationID)}
    if 'getlocationdescriptionlist' in url.lower():
        response = requests.get(url, headers = header, params = param)
        return response.json()['LocationDescriptions'][0]['Name']
    else:
        return "BAD URL"

def get_timeseries_data(url, uniqueID, startDate, endDate, token):
    header = {'X-Authentication-Token': str(token), 'Accept': 'application/json'}
    if type(uniqueID) != list:
        UIDs = [uniqueID]
    else:
        UIDs = uniqueID
    df = pd.DataFrame(columns = ['parameter', 'datetime', 'value', 'unit'])
    for uid in UIDs:
        param = {'TimeSeriesUniqueIds': str(uid), 'QueryFrom': startDate, 'QueryTo': endDate}
        response = requests.get(url, headers = header, params = param)
        unit = response.json()['TimeSeries'][0]['Unit']
        valueName = response.json()['TimeSeries'][0]['Parameter'] + ' ' + response.json()['TimeSeries'][0]['Label']
        data = response.json()['Points']
        timeKey = 'Timestamp'
        valueKey = 'NumericValue1'
        for i in data:
            dateFormat = "%Y-%m-%dT%H:%M:%S"
            time = i[timeKey]
            dt_object = datetime.datetime.strptime(time[:-14], dateFormat)
            if time[-6:-5] == "+":
                mult = +1
            else:
                mult = -1
            offset_minutes = mult * (int(time[-5:-3]) * 60) + int(time[-2:])
            utc_offset = datetime.timedelta(minutes = offset_minutes)
            local_time = dt_object + utc_offset
            s = pd.Series({'parameter': valueName, 'datetime': local_time, 'value': i[valueKey], 'unit': unit})
            df = pd.concat([df, s.to_frame().T], ignore_index = True)
    return df

def read_threshold_file(file):
    locs = []
    params = []
    limits = []
    
    if 'xls' in file:
        data = pd.read_excel(file)
        locs = data['location_id'].tolist()
        params = data['parameter'].tolist()
        limits = data['threshold'].tolist()
        
    else:
        data = [line.rstrip('\n') for line in open(file)]
        for line in data:
            tp = line.split('\t')
            locs.append(tp[0])
            params.append(tp[1])
            limits.append(tp[2])
    return locs, params, limits
        
def send_email(recipients, from_email, password, email_server, message, subject):
    if type(recipients) != list:
        r = recipients.split(",")
    else:
        r = recipients
    to_email = ", ".join(r)
    smtpserver = smtplib.SMTP(email_server, 587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo()
    smtpserver.login(from_email, password)
    header = 'To: ' + ", " + to_email[0] + '\n' + 'From: ' + from_email + '\n' + 'Subject: ' + subject + '\n'
    mmsg = header + '\n' + message + '\n'
    smtpserver.sendmail(from_email, to_email, mmsg)
    smtpserver.close()
    print("Email successfully sent.")
    
def check_exceedance(data, limit, loc, param):
    exceed = data.loc[data['value'] >= limit,:]
    sendEmail = False
    emailText = ''
    if exceed.shape[0] > 0:
        unit = data.loc[0, 'unit']
        sendEmail = True
        emailText = 'There has been at least one exceedance of ' + param + ' at location ' + loc + ' since ' + startDate + \
                    '. The threshold value is ' + str(limit) + unit + '\n'
    else:
        unit = data.loc[0, 'unit']
        print('No exceedance of ' + uid_names[uid] + ' at location ' + loc  + ' since ' + startDate + \
              '. The threshold value is ' + str(limit) + unit)
    return sendEmail, emailText
        
if __name__ == "__main__":
    f = r'C:\Users\USER\Documents\RWB\aquarius_thresholds.xlsx'
    session_url = 'http://10.10.82.215/AQUARIUS/Publish/v2/session'
    params_url = 'http://10.10.82.215/AQUARIUS/Publish/v2/GetTimeSeriesDescriptionList'
    data_url = 'http://10.10.82.215/AQUARIUS/Publish/v2/GetTimeSeriesData'
    locname_url = 'http://10.10.82.215/AQUARIUS/Publish/v2/GetLocationDescriptionList'
    locs, params, limits = read_threshold_file(f)
    print('Initiating connection to Aquarius...')
    token = start_connection(session_url, 'User3', 'iwrm@2021!')
    if token != 'BAD URL':
        print('Successful connection to Aquarius')
        startDate = (datetime.datetime.now() - datetime.timedelta(days = 20)).strftime('%Y-%m-%d %H:%M')
        runTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        endDate = ''
        sendEmail = False
        emailText = ''
        i = 0
        while i < len(locs):
            print('Getting unique IDs for location ' + str(locs[i]))
            uids, uid_names = get_uniqueID_with_threshold(params_url, str(locs[i]), token)
            if uids != 'BAD URL':
                for uid in uids.keys():
                    print('Unique IDs successfully retrieved')
                    data = get_timeseries_data(data_url, uid, startDate, endDate, token)
                    param = uid_names[uid]
                    loc = (get_location_name(locname_url, locs[i], token)).replace("BAD URL", str(locs[i]))
                    limit = uids[uid]
                    if data.shape[0] > 0:
                        print('Data for Unique ID ' + uid + ' have been successfully retrieved.')
                        sendEmail, Text = check_exceedance(data, limit, loc, param)
                        emailText = emailText + Text + '\n'
                    else:
                        print('No data found for Unique ID ' + uid + ', start date ' + startDate)
            else:
                print('Unique IDs not retrieved')
            i += 1
    if sendEmail:
        send_email('bingwa01@hotmail.com', 'fidele.bingwa@rwb.rw', 'GodisGood130497!!', 'mail.rwb.rw', emailText, 'Aquarius Exceedance Alert')
    else:
        print('No exceedances at all locations between ' + startDate + ' and ' + runTime)

