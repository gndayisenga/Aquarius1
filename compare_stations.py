'''
Created on May 3, 2023

@author: USER
'''
import pandas as pd
from rwb_communication import rwb_communication
from rwb_aquarius_data_analysis import rwb_aquarius_data_analysis
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np
from trend_classifier import Segmenter

url = 'http://10.10.82.215/AQUARIUS/'
db = rwb_aquarius_data_analysis(url, 'User3', 'iwrm@2021!')
file1 = r'C:\Users\USER\OneDrive\Documents\Work\Data\Aquarius\mukungwa_nyakinama.txt'
file2 = r'C:\Users\USER\OneDrive\Documents\Work\Data\Aquarius\nyabarongo_ruliba.txt'
file4 = r'C:\Users\USER\OneDrive\Documents\Work\Data\Aquarius\kanzenze.txt'
loc1 = '294701'
loc2 = '270001'
loc3 = '259501'
st = '1900-01-01 00:00'
et = '2023-01-01 00:00'
#data = db.get_aquarius_data_by_parameter(loc3, 'stage', st, et)
#data.to_csv(file4, index =  False, sep = '\t')

data = pd.read_csv(file2, sep = '\t')
data['datetime'] = pd.to_datetime(data['datetime'])
df = data.loc[:, ['datetime', 'value']]
df.index = df.datetime
d_mean = df.rolling('24h', on = 'datetime').mean()
m_mean = df.rolling('30D', on = 'datetime').mean()

#fig, ax = plt.subplots(figsize=(12, 6))
fig, ax = plt.subplots(2, 1, sharex = True, sharey=True, figsize = (12, 12))

d_gp = df.groupby(pd.Grouper(key = 'datetime', freq = '1D')).mean()
m_gp = df.groupby(pd.Grouper(key = 'datetime', freq = '1M')).mean()

dff = df
dff['day_of_year'] = df['datetime'].dt.day_of_year
dff =  dff.drop(['datetime'], axis = 1)

d_h_gp = dff.groupby(by = 'day_of_year')['value'].agg(['mean', 'min', 'max'])

start_date = datetime.date(datetime.datetime.now().year, 1, 1)
d_h_gp['datetime'] = d_h_gp.index
for i, row in d_h_gp.iterrows():
    d_h_gp.loc[i, 'datetime'] =  start_date + datetime.timedelta(days = i - 1)

d_h_gp.index = d_h_gp['datetime']

dh_mean = d_h_gp['mean']
dh_min = d_h_gp['min']
dh_max = d_h_gp['max']

ax[0].plot(d_mean, color = "blue", label = 'Daily Rolling Mean')
ax[0].plot(m_mean, color = "red", label = 'Monthly Rolling Mean')
ax[0].set_title("Current Data")
ax[0].legend(loc = "best")
#ax[0].tick_params(labelcolor='black', labelsize='medium', width=3)
fmt = mdates.DateFormatter('%m-%d')
ax[0].xaxis.set_major_locator(mdates.DayLocator())
ax[0].xaxis.set_major_formatter(fmt)
ax[0].xaxis.set_minor_locator(mdates.DayLocator())
# Rotates and right-aligns the x labels so they don't crowd each other.
for label in ax[0].get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')



# ax.plot(m_gp, color = "black", label = 'Monthly Rolling Mean')
# ax.plot(d_gp, color = "green", label = 'Daily Rolling Mean')
ax[1].plot(dh_mean, color = 'green', label = 'Historical Mean of Day')
ax[1].plot(dh_min, color = 'blue', label = 'Historical Min of Day')
ax[1].plot(dh_max, color = 'red', label = 'Historical Max of Day')
ax[1].plot(d_gp, color = "black", label = 'Daily Rolling Mean')

ax[1].set_xlim([datetime.date(2023,4,15), datetime.date(2023,5,1)])
ax[1].set_title("Historical Comparison")
ax[1].legend(loc = "upper right")
for label in ax[1].get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')
plt.show()

df3 = df.loc[df.index.date >= datetime.date(2023,4,15),:]
seg = Segmenter(df = df3, column = 'value', n=20)
seg.calculate_segments()


tr = seg.segments.to_dataframe()
file3 = r'C:\Users\USER\OneDrive\Documents\Work\Data\segments.txt'
tr.to_csv(file3, index = False, sep = '\t')
last_trend = tr["slope"].iloc[-1]
if last_trend > 0:
    print('Increasing')
else:
    print('Descreasing')

seg.plot_segments()
# def time_series_analysis(data):
#


