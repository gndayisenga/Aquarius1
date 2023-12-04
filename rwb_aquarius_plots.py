'''
Created on May 15, 2023

@author: USER
'''
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import matplotlib.image as image
from rwb_aquarius_data_analysis import rwb_aquarius_data_analysis
import datetime

class rwb_aquarius_plots(object):
    '''
    classdocs
    '''
    def __init__(self, url, user, password, logo, footer, subplt, size, locationID, parameter, startDate, endDate):
        '''
        Constructor
        '''
        self._loc = locationID
        self._param = parameter
        self._st = startDate
        self._et = endDate
        self._db = rwb_aquarius_data_analysis(url, user, password)
        self._fig, self._axs = plt.subplots(subplt[0], subplt[1], sharey = True, figsize = size)
        fmt = mdates.DateFormatter('%m-%d')
        for ax in self._axs:
            ax.xaxis.set_major_locator(mdates.DayLocator())
            ax.xaxis.set_major_formatter(fmt)
            ax.xaxis.set_minor_locator(mdates.DayLocator())
            for label in ax.get_xticklabels(which='major'):
                label.set(rotation=30, horizontalalignment='right')
        self._data = self._db.get_daily_stats(locationID, parameter, startDate, endDate)
        self._logo = logo
        self._size = size
        self._footer = footer
        
    
    def plot_info(self):
        loc_name = self._db.get_location_name(self._loc)
        self._fig.suptitle(loc_name + ": " + self._param.title(), \
                           x = 0.52, y = 0.93, fontsize = 14, fontname = 'Arial', fontweight = 'bold')
        st = datetime.datetime.strptime(self._st, '%Y-%m-%d %H:%M')
        et = datetime.datetime.strptime(self._et, '%Y-%m-%d %H:%M')
        for ax in self._axs:
            ax.set_ylabel(self._param + ' (' + self._db.get_unit(self._loc, self._param) + ')', fontname = 'Arial')
            ax.set_xlim([st, et])
            #ax.set_ylim(bottom = 0)
        
    def plot_historical(self, plot_index):
        hist_mean, hist_min, hist_max = self._db.get_historical_daily_stats(self._loc, self._param)
        i = 0
        if not hist_mean.empty:
            self._axs[plot_index].plot(hist_mean, color = 'green', label = 'Historical Mean of Day')
            i = 1
        if not hist_min.empty:
            self._axs[plot_index].plot(hist_min, color = 'blue', label = 'Historical Minimum of Day')
            i = 1
        if not hist_max.empty:
            self._axs[plot_index].plot(hist_max, color = 'red', label = 'Historical Maximum of Day')
            i = 1
        if i == 1:
            self._axs[plot_index].set_title("Historical Data Comparison")
            self._axs[plot_index].legend(loc = "upper right")
        
    def plot_rolling_means(self, plot_index):
        roll_24h = self._data[3]
        roll_30d = self._data[4]
        self._axs[plot_index].plot(roll_24h, color = "blue", label = '24 hours Rolling Mean')
        self._axs[plot_index].plot(roll_30d, color = "red", label = '30 days Rolling Mean')
        self._axs[plot_index].set_title("Current Data")
        self._axs[plot_index].legend(loc = "upper right")
    
    def plot_daily_mean(self, plot_index):
        self._axs[plot_index].plot(self._data[0], color = 'black', label = 'Daily Mean')
        self._axs[plot_index].set_title("Current Data")
        self._axs[plot_index].legend(loc = "upper right")
        
    def plot_show(self):
        plt.show()
    
    # def open_pdf(self, path):
    #     pdf = PdfPages(path)
    #     return pdf
    
    def save_to_pdf(self, pdf):
        pdf.savefig(self._fig)
    
    # def close_pdf(self, pdf):
    #     pdf.close()
    
    def get_fig(self):
        return self._fig

    def add_rwb_logo(self):
        with cbook.get_sample_data(self._logo) as file:
            im = image.imread(file)
        ymax = int(self._size[1] * 87)
        self._fig.figimage(im, 0, ymax, alpha = 0.6)
    def add_rwb_footer(self):
        with cbook.get_sample_data(self._footer) as file:
            im = image.imread(file)
        x = int(self._size[0] * 19.5)
        self._fig.figimage(im, x, 5, alpha = 0.7)
        