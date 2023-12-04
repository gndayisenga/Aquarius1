'''
Created on May 15, 2023

@author: USER
'''
import pandas as pd
from rwb_aquarius_data_analysis import rwb_aquarius_data_analysis
from matplotlib.backends.backend_pdf import PdfPages
from rwb_aquarius_plots import rwb_aquarius_plots

url = 'http://10.10.82.215/AQUARIUS/'
file = r'C:\Users\USER\Documents\RWB\aquarius_location_details.txt'
db = rwb_aquarius_data_analysis(url, 'User3', 'iwrm@2021!')
st = '2023-04-15 00:00'
et = '2023-05-15 00:00'

# data = db.get_aquarius_data('', st, et)
# loc = data['locid'].unique()

loc = ['294701','SW5','SW19','192001','SW2','SW20','70017','SW7','SW23','SW18','292001',\
       'SW9','WQ27','298001','281001','70027','70014','WQ16','SW10','294701','196501',\
       'SW13','WQ22','WQ21','262001','WQ50','282001','WQ54','WQ15']


url = 'http://10.10.82.215/AQUARIUS/'
user = 'User3'
pwd = 'iwrm@2021!'

path = r'C:\Users\USER\OneDrive\Documents\Work\Data\Aquarius\bulletin'
path = path + '_' + st[:-6] + '_' + et[:-6] + '.pdf' 

pdf = PdfPages(path)

logo = r'C:\Users\Public\Documents\Aquarius\RWB-logo.png'
footer = r'C:\Users\Public\Documents\Aquarius\RWB-footer.png'


for l in loc:
    print('starting location ' + l)
    for parameter in ['stage', 'water level']:
        params = db.get_parameter_list(l)
        if parameter in params:
            p = rwb_aquarius_plots(url, user, pwd, logo, footer, [2,1], (10, 12), l, parameter, st, et)
            if p._data != []:
                p.plot_daily_mean(0)
                p.plot_rolling_means(1)
            p.plot_historical(0)
            p.plot_info()
            p.add_rwb_logo()
            p.add_rwb_footer()
            p.save_to_pdf(pdf)
    print('finished location ' + l)


pdf.close()