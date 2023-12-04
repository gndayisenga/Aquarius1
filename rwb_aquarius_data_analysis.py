'''
Created on Nov 30, 2022

@author: USER
'''
import pandas as pd
from rwb_aquarius_api_calls import rwb_aquarius_api_calls as api
import numpy as np
import datetime
import os
from dask.dataframe.io.tests.test_json import df

class rwb_aquarius_data_analysis(object):
    '''
    classdocs
    '''


    def __init__(self, url, username, password):
        '''
        Constructor
        '''
        self._url = url
        self._user = username
        self._pwd = password
        self._aq = api(self._url, self._user, self._pwd)
    
    def get_aquarius_data(self, location, startDate, endDate, **kwargs):
        parameter = kwargs.get('parameter', '')
        threshold = kwargs.get('threshold', False)
        data = self._aq.get_timeseries_data(location, startDate, endDate, parameter = parameter, threshold = threshold)
        return data
    
    def check_exceedance_for_email(self, locationID, startDate, endDate):
        data = self._aq.get_timeseries_data(locationID, startDate, endDate, threshold = True)
        exceed = data.loc[data['value'] >= data['threshold'],:]
        sendEmail = False
        emailText = ''
        number_of_locations = len(data['location'].unique())
        if exceed.shape[0] > 0:
            for loc in exceed['location'].unique():
                exceed2 = exceed.loc[exceed['location'] ==  loc, ['locid','parameter', 'unit', 'threshold']].reset_index()
                for param in exceed2['parameter'].unique():
                    u = exceed2.loc[exceed2['parameter'] == param, ['locid', 'unit', 'threshold']].reset_index()
                    unit = u.loc[0, 'unit']
                    limit = u.loc[0, 'threshold']
                    locID = u.loc[0, 'locid']
                    sendEmail = True
                    emailText = 'There has been at least one exceedance of ' + param + ' at location ' + loc + ' (location ID ' + \
                                str(locID) + ') since ' + startDate + '. The threshold value is ' + str(limit) + ' ' + unit + '\n'
        else:
            if locationID == '':
                print('No exceedances found since ' + startDate + '. ' + str(number_of_locations) + ' locations with thresholds were checked.')
            else:
                loc = self._aq.get_location_name(locationID)
                print('No exceedances at location ' + loc  + ' (location ID ' + str(locationID) + ') since ' + startDate + '.')
        return sendEmail, emailText
    
    def export_data_to_file(self, locationID, startDate, endDate, file, **kwargs):
        exceeding = kwargs.get('exceeding', True)
        if exceeding:
            exceed = self._aq.get_timeseries_data(locationID, startDate, endDate, threshold = True)
            data = exceed.loc[exceed['value'] >= exceed['threshold'],:]
        else:
            data = self._aq.get_timeseries_data(locationID, startDate, endDate)
        if 'xls' in file:
            data.to_excel(file, index = False)
        else:
            data.to_csv(file, index = False, sep = '\t')
    
    def get_data_stats(self, locationID, startDate, endDate):
        data = self._aq.get_timeseries_data(locationID, startDate, endDate)
        grouped = data.groupby(['locid', 'location', 'parameter', 'unit'])['value', 'datetime'].agg([np.min, np.max, np.average, np.size, 'std']).reset_index()
        return grouped
        
    def get_historical_daily_stats(self, locationID, parameter):
        path = r'C:\Users\Public\Documents\Aquarius' + '\\' + locationID + '_' + parameter + '.h5'
        
        st, df = self.check_historical_data_download(path)
        startDate = st.strftime("%Y-%m-%d %H:%M")
        
        if st.date() == datetime.date.today() and not df.empty:
            data = df
        elif st == datetime.datetime(1900, 1, 1, 0, 0):
            endDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            data = self._aq.get_timeseries_data(locationID, startDate, endDate, parameter = parameter)
            data.to_hdf(path, parameter, index = False)
        else:
            endDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            d = self._aq.get_timeseries_data(locationID, startDate, endDate, parameter = parameter)
            data = pd.concat([df, d])
            data.to_hdf(path, parameter, index = False, mode = 'a')
        
        if data.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        else:
            data['day_of_year'] = data['datetime'].dt.day_of_year
            data =  data.drop(['datetime'], axis = 1)
            
            d_h_gp = data.groupby(by = 'day_of_year')['value'].agg(['mean', 'min', 'max'])
            start_date = datetime.date(datetime.datetime.now().year, 1, 1)
            d_h_gp['datetime'] = d_h_gp.index
            for i, row in d_h_gp.iterrows():
                d_h_gp.loc[i, 'datetime'] =  start_date + datetime.timedelta(days = i - 1)
            d_h_gp.index = d_h_gp['datetime']
            
            daily_historical_mean = d_h_gp['mean']
            daily_historical_min = d_h_gp['min']
            daily_historical_max = d_h_gp['max']
            
            return daily_historical_mean, daily_historical_min, daily_historical_max
    
    def get_daily_stats(self, locationID, parameter, startDate, endDate):
        data = self._aq.get_timeseries_data(locationID, startDate, endDate, parameter = parameter)
        
        if data.empty:
            return []
        else:
            data.index = data['datetime']
            
            rolling_24h = data.loc[:,['datetime','value']].rolling('24h', on = 'datetime').mean()
            rolling_30d = data.loc[:,['datetime','value']].rolling('30D', on = 'datetime').mean()
            
            data['day_of_year'] = data['datetime'].dt.day_of_year
            data =  data.drop(['datetime'], axis = 1)
            
            d_h_gp = data.groupby(by = 'day_of_year')['value'].agg(['mean', 'min', 'max'])
            start_date = datetime.date(datetime.datetime.now().year, 1, 1)
            d_h_gp['datetime'] = d_h_gp.index
            for i, row in d_h_gp.iterrows():
                d_h_gp.loc[i, 'datetime'] =  start_date + datetime.timedelta(days = i - 1)
            d_h_gp.index = d_h_gp['datetime']
            
            daily_mean = d_h_gp['mean']
            daily_min = d_h_gp['min']
            daily_max = d_h_gp['max']
            return [daily_mean, daily_min, daily_max, rolling_24h, rolling_30d]
        
    def get_location_name(self, locationID):
        return self._aq.get_location_name(locationID)
    
    def get_unit(self, locationID, parameter):
        return self._aq.get_unit(locationID, parameter)
    
    def check_historical_data_download(self, path):
        if os.path.exists(path):
            data = pd.read_hdf(path, mode = 'a')
            if not data.empty:
                last_date = data.iloc[-1,:]['datetime']
            else:
                last_date = datetime.datetime(1900, 1, 1, 0, 0)
                data = pd.DataFrame()
        else:
            last_date = datetime.datetime(1900, 1, 1, 0, 0)
            data = pd.DataFrame()
        return last_date, data
    
    def get_location_coordinates(self, locationID):
        return self._aq.get_location_coordinates(locationID)
    
    def get_parameter_list(self, locationID, **kwargs):
        label = kwargs.get('label', False)
        return self._aq.get_parameter_list(locationID, label = label)
    