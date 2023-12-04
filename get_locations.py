'''
Created on May 25, 2023

@author: USER
'''

import pandas as pd
from rwb_aquarius_data_analysis import rwb_aquarius_data_analysis


url = 'http://10.10.82.215/AQUARIUS/'
file = r'C:\Users\USER\Documents\RWB\aquarius_location_active_20230612.txt'
db = rwb_aquarius_data_analysis(url, 'User3', 'iwrm@2021!')

data = db.get_aquarius_data_by_parameter('', 'stage', '2023-06-11 00:00', '2023-06-13 00:00')
data2 = db.get_aquarius_data_by_parameter('', 'water level', '2023-06-11 00:00', '2023-06-13 00:00')

d = pd.concat([data, data2])

 
locs = db.get_location_coordinates('')

locs.drop_duplicates(inplace = True, ignore_index = True)

#locs['parameters'] = ''
locs['active'] = ''

active_locs = d['locid'].unique()

for i, loc in locs.iterrows():
    if loc['locid'] in active_locs:
        locs.iloc[i]['active'] = 'Yes'
    else:
        locs.iloc[i]['active'] = 'No'
    # params = db.get_parameter_list(loc['locid'])
    # locs.iloc[i]['parameters'] = params

locs.to_csv(file, index =  False, sep = '\t')

