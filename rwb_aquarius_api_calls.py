'''
Created on Nov 30, 2022

@author: USER
'''
import requests
import datetime
import pandas as pd
import sys
import numpy as np


class rwb_aquarius_api_calls(object):
    '''
    classdocs
    '''
    def __init__(self, url, username, password):
        '''
        the URL provided should be the aquarius server url e.g.: http://10.10.82.215/AQUARIUS/
        '''
        
        if '/aquarius/' in url.lower():
            self._url = url
            self._user = username
            self._pwd = password
            url = self._url + '/Publish/v2/session'
            headers = {"Username": self._user, "EncryptedPassword": self._pwd, "Locale":""}
            response = requests.post(url, headers)
            self._token = response.text
        else:
            sys.exit("Bad aquarius API url")
    
    
    def start_connection(self):
        url = self._url + '/Publish/v2/session'
        headers = {"Username": self._user, "EncryptedPassword": self._pwd, "Locale":""}
        response = requests.post(url, headers)
        return response.text
    
    def convert_utc_time(self, date, utc_offset_hrs):
        utc_offset = datetime.timedelta(hours = utc_offset_hrs)
        utc_time = date - utc_offset
        datetime.datetime.strftime(utc_time, "%Y-%m-%d %H:%M")
        return utc_time
    
    def get_all_TimeSeriesDescription_byList(self, uniqueIDs):
        url = self._url + '/Publish/v2/GetTimeSeriesDescriptionListByUniqueId'
        header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
        uniqueIDsList = ''
        d = []
        i = 0
        for i in range(len(uniqueIDs)):
            if type(uniqueIDs[i]) == dict:
                uniqueIDsList = uniqueIDsList + str(uniqueIDs[i]['UniqueId']) + ','
                if i % 40 == 0 and i > 0 or i == len(uniqueIDs)-1:
                    param = {'TimeSeriesUniqueIds': uniqueIDsList[:-1]}
                    response = requests.get(url, headers = header, params = param)
                    u = response.json()['TimeSeriesDescriptions']
                    d += u
                    uniqueIDsList = ''
            else:
                uniqueIDsList = uniqueIDsList + str(uniqueIDs[i]) + ','
                if (i % 40 == 0 and i > 0) or i == len(uniqueIDs)-1:
                    param = {'TimeSeriesUniqueIds': uniqueIDsList[:-1]}
                    response = requests.get(url, headers = header, params = param)
                    u = response.json()['TimeSeriesDescriptions']
                    d += u
                    uniqueIDsList = ''
            i += 1
        return d
    
    def get_uniqueID(self, locationID, **kwargs):
        uniqueIDs = {}
        uniqueIDsNames ={}
        uniqueIDsLocationIDs = {}
        parameter = kwargs.get('parameter', 'yxyx')
        threshold = kwargs.get('threshold', False)
        if locationID != '':
            url = self._url + '/Publish/v2/GetTimeSeriesDescriptionList'
            header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
            param = {'LocationIdentifier': str(locationID)}
            response = requests.get(url, headers = header, params = param)
            x = response.json()
            descriptions = x['TimeSeriesDescriptions']
        else:
            url = self._url + '/Publish/v2/GetTimeSeriesUniqueIdList'
            header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
            response = requests.get(url, headers = header)
            x = response.json()
            uniqueIDsList = x['TimeSeriesUniqueIds']
            descriptions = self.get_all_TimeSeriesDescription_byList(uniqueIDsList)
        for p in descriptions:
            if p['Parameter'].lower() == parameter.lower().replace('yxyx', p['Parameter'].lower()) and p['TimeSeriesType'].lower() == 'processorbasic':
                thresholds = p['Thresholds']
                if threshold and thresholds != []:
                    uid = p['UniqueId']
                    for t in thresholds:
                        trs = t['Periods']
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
                        uniqueIDsLocationIDs[uid] = p['LocationIdentifier']
                elif threshold == False:
                    uid = p['UniqueId']
                    uniqueIDs[uid] = np.NaN
                    uniqueIDsNames[uid] = p['Parameter'] + '-' + p['Label']
                    uniqueIDsLocationIDs[uid] = p['LocationIdentifier']
        return uniqueIDs, uniqueIDsNames, uniqueIDsLocationIDs
    
    def get_location_name(self, locationID):
        url = self._url + '/Publish/v2/GetLocationDescriptionList'
        header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
        param = {'LocationIdentifier': str(locationID)}
        response = requests.get(url, headers = header, params = param)
        return response.json()['LocationDescriptions'][0]['Name']
    
    def get_unit(self, locationID, parameter):
        url = self._url + '/Publish/v2/GetTimeSeriesDescriptionList'
        header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
        param = {'LocationIdentifier': str(locationID)}
        response = requests.get(url, headers = header, params = param)
        x = response.json()
        descriptions = x['TimeSeriesDescriptions']
        unit = ''
        for p in descriptions:
            if p['Parameter'].lower() == parameter.lower():
                unit = p['Unit']
        return unit
    
    def get_timeseries_data(self, locationID, startDate, endDate, **kwargs):
        url = self._url + '/Publish/v2/GetTimeSeriesData'
        header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
        parameter = kwargs.get('parameter', '')
        threshold = kwargs.get('threshold', False)
        uniqueIDs, uniqueIDsNames, uniqueIDsLocationIDs = self.get_uniqueID(locationID, parameter = parameter, threshold = threshold)
        df = pd.DataFrame(columns = ['locid', 'location', 'parameter', 'datetime', 'value', 'unit', 'threshold'])
        if startDate == '':
            startDate = '1900-01-01 00:00'
        if endDate == '':
            endDate = '2079-01-01 00:00'
        st = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M')
        utc_st = self.convert_utc_time(st, 2)
        et = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M')
        utc_et = self.convert_utc_time(et, 2)
        for uid in uniqueIDs.keys():
            param = {'TimeSeriesUniqueIds': str(uid), 'QueryFrom': utc_st, 'QueryTo': utc_et}
            response = requests.get(url, headers = header, params = param)
            unit = response.json()['TimeSeries'][0]['Unit']
            valueName = uniqueIDsNames[uid]
            param, label = valueName.split('-')
            limit = uniqueIDs[uid]
            data = response.json()['Points']
            timeKey = 'Timestamp'
            valueKey = 'NumericValue1'
            if locationID != '':
                locName = self.get_location_name(locationID)
                locID = locationID
            else:
                locName = self.get_location_name(uniqueIDsLocationIDs[uid])
                locID = uniqueIDsLocationIDs[uid]
            for i in data:
                dateFormat = "%Y-%m-%dT%H:%M:%S"
                time = i[timeKey]
                dt_object = datetime.datetime.strptime(time[:-14], dateFormat)
                if dt_object >= st and dt_object <= et:
                    s = pd.Series({'locid': locID, 'location': locName, 'parameter': param, \
                                   'label': label, 'datetime': dt_object, 'value': i[valueKey],\
                                   'unit': unit, 'threshold': limit})
                    df = pd.concat([df, s.to_frame().T], ignore_index = True)
        return df
    
    def get_location_coordinates(self, locationID):
        url = self._url + '/Publish/v2/GetLocationData'
        header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
        uniqueIDs, uniqueIDsNames, uniqueIDsLocationIDs = self.get_uniqueID(locationID)
        df = pd.DataFrame(columns = ['locid', 'location', 'description', 'longitude', 'latitude', 'srid'])
        for uid in uniqueIDsLocationIDs.keys():
            locName = self.get_location_name(uniqueIDsLocationIDs[uid])
            locID = uniqueIDsLocationIDs[uid]
            param = {'LocationIdentifier' : str(locID)}
            response = requests.get(url, headers = header, params = param)
            data = response.json()
            x = data['Longitude']
            y = data['Latitude']
            coord_type = data['Srid']
            if 'Description' in data.keys():
                desc = data['Description']
            else:
                desc = ''
            s = pd.Series({'locid': locID, 'location': locName, 'description': desc, \
                           'longitude': x, 'latitude': y, 'srid': coord_type})
            df = pd.concat([df, s.to_frame().T], ignore_index = True)
        return df
    
    def get_parameter_list(self, locationID, **kwargs):
        url = self._url + '/Publish/v2/GetTimeSeriesDescriptionList'
        header = {'X-Authentication-Token': str(self._token), 'Accept': 'application/json'}
        param = {'LocationIdentifier': str(locationID)}
        response = requests.get(url, headers = header, params = param)
        label = kwargs.get('label', False)
        x = response.json()
        descriptions = x['TimeSeriesDescriptions']
        parameters = []
        for p in descriptions:
            if label:
                parameters.append(p['Parameter'].lower() + '-' + p['Label'])
            else:
                parameters.append(p['Parameter'].lower())
        return parameters
            
            