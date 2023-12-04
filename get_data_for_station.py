'''
Created on Jun 11, 2023

@author: USER
'''

import pandas as pd
from rwb_aquarius_data_analysis import rwb_aquarius_data_analysis

url = 'http://IP/AQUARIUS/'
file = r'C:\Users\USER\Documents\RWB\aquarius_location_data_20230703.txt'
file2 = r'C:\Users\USER\Documents\RWB\aquarius_location_data_20230704.txt'
db = rwb_aquarius_data_analysis(url, 'User3', 'iwrpass!')

data = db.get_aquarius_data('270001', '2023-06-10 00:00', '2023-06-12 00:00', parameter = 'stage')

data2 = db.get_aquarius_data('270001', '2023-06-10 00:00', '2023-06-12 00:00', parameter = 'stage')

data.to_csv(file, index =  False, sep = '\t')
data2.to_csv(file2, index =  False, sep = '\t')
