#!/usr/bin/env python
# coding: utf-8

# In[ ]:


''' Niagara Falls Events list - 10Times.com

* List of all events in Niagara Falls for the next 11 months (from search month)

1) Scrape all event names and event URLs

2) Create and clean data frame with event names and URLs - into CSV file

3) Loop and open each URL from CSV file and scrape event name, date, GIS locations- addresses
'''




#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "EventScraperNiagaraFalls"
__package__ = "events"
__module__ = "scraper"
__app__ = "wrangler"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"
__events_data__ = "events/"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages '''
    import os
    import sys
    import logging
    import traceback
    import functools
    import configparser
    ''' function specific python packages '''
    
    from bs4 import BeautifulSoup
    import requests
    import lxml
    import datetime
    import csv
    from csv import writer
    import pandas as pd
    import numpy as np
    import re
    import json
    from datetime import datetime, date, timedelta
    from requests import Request, Session
    from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
    import time   # to convert datetime to unix timestamp int



    print("All %s-module %s-packages in function-%s imported successfully!"
          % (__module__,__package__,__name__))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"          .format(__module__,__package__,__name__,e))


'''
    CLASS spefic to scraping Events data
'''

class EventScraper():

    ''' Function
            name: __init__
            parameters:
            
            procedure: Initialize the class
            return None
            
            author: <nileka.desilva@colombo.rezgateway.com>
            
            resources: to implement s3 logger - 
            https://medium.com/nerd-for-tech/simple-aws-s3-logging-in-python3-using-boto3-cfbd345ef65b
    '''
    def __init__(self, desc : str="EventScraper Class", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
         if desc is None:
            self.__desc__ = " ".join([self.__app__,
                                      self.__module__,
                                      self.__package__,
                                      self.__name__])
        else:
            self.__desc__ = desc
        
        self._data = None
        self._connection = None
        self._prices = None

        __s_fn_id__ = "__init__"

        global pkgConf
#         global appConf
        global logger
        global clsRW
        global clsNoSQL
        global clsSpark
        global config
        global clsUtil
        global clsNLP
        global clsSparkWL

         self.cwd=os.path.dirname(__file__)
        sys.path.insert(1,self.cwd)
        import scraperUtils as eventsu

        config = configparser.ConfigParser()
        config.read(os.path.join(self.cwd,__ini_fname__))

        self.rezHome = config.get("CWDS","REZAWARE")
        sys.path.insert(1,self.rezHome)
        
        from utils.modules.etl.load import sparkwls as spark
        from utils.modules.ml.natlang import nlp
        ''' initialize util class to use common functions '''
        clsUtil = eventsu.Utils(desc='Utilities class for event data scraping')
        clsSparkWL = spark.SparkWorkLoads(desc="event details scraper")

        ''' import dataio utils to read and write data '''
        from utils.modules.etl.load import filesRW as rw
        clsRW = rw.FileWorkLoads(desc=self.__desc__)
        clsRW.storeMode = config.get("DATASTORE","MODE")
        clsRW.storeRoot = config.get("DATASTORE","ROOT")
        self.storePath = os.path.join(
            self.__app__,
            "data/",
            self.__module__,
            self.__package__,
        )
        
        
        
        ''' set package specific path to the input and output data '''
#         self.dataDir = os.path.join(config.get("CWDS","DATA"),__bookings_data__)
        self.dataDir = os.path.join(self.storePath,__events_data__)

        ''' Set the wrangler root directory '''
        self.pckgDir = config.get("CWDS",self.__package__)
        self.appDir = config.get("CWDS",self.__app__)
        
        
        
        
            
            ''' innitialize the logger '''
            from rezaware import Logger as logs
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)

            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info(self.__name__,self.__package__)

            
            
            
            
            ''' import mongo work load utils to read and write data '''
#             from utils.modules.etl.load import noSQLwls as nosql
            from utils.modules.etl.load import sparkNoSQLwls as nosql
            clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)
            ''' import sparkDBwls to write collections to db '''
            from utils.modules.etl.load import sparkDBwls
            clsSpark = sparkDBwls.SQLWorkLoads(desc=self.__desc__)
            
#             ''' import file work load utils to read and write data '''
#             from utils.modules.etl.load import filesRW as rw
#             clsRW = rw.FileWorkLoads(desc=self.__desc__)
#             clsRW.storeMode = pkgConf.get("DATASTORE","MODE")
#             clsRW.storeRoot = pkgConf.get("DATASTORE","ROOT")
#             logger.info("Files RW mode %s with root %s set",clsRW.storeMode,clsRW.storeRoot)

            ''' set the package specific storage path '''
            ''' TODO change to use the utils/FileRW package '''
            self.storePath = pkgConf.get("CWDS","DATA")
#             self.storePath = os.path.join(
#                 self.__app__,
#                 "data/",
#                 self.__module__,
#                 self.__package__,
#             )
    
            logger.info("%s package files stored in %s",self.__package__,self.storePath)

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            ''' set the tmp dir to store large data to share with other functions
                if self.tmpDIR = None then data is not stored, otherwise stored to
                given location; typically specified in app.conf
            '''
            self.tmpDIR = None
            if "WRITE_TO_TMP":
                self.tmpDIR = os.path.join(self.storePath,"tmp/")
                if not os.path.exists(self.tmpDIR):
                    os.makedirs(self.tmpDIR)

            self.scrape_start_date = date.today()
            self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
            self.scrapeTimeGap = 10
            print("%s Class initialization complete" % self.__name__)
            
            self.destination_id = ["Niagara Falls"]
            

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return None


# In[30]:


''' Function --- Scrape Event URLS ---

            name: _scrape_event_urls_to_csv
            parameters:

author: <nileka.desilva@colombo.rezgateway.com>
'''

    #10times site url - main page
    url = "https://10times.com/niagarafalls-ca"

    def _scrape_event_urls_to_csv(self, url):

        ''' Description: Scrape all event names and respective event page URLs from 10times.com. 
        Events have been filtered to events located in Niagara Falls, Canada

        Scraped data is stored in a csv file ('10timesNiagaraFallsEventURLs-RawData.csv')
        '''

         
        saveTo = None
        _save_df = pd.DataFrame()

        __s_fn_id__ = "function <_scrape_event_url_to_csv>"
#        logger.info("Executing %s", __s_fn_id__)

        try:
            headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise RuntimeError("Invalid response with code %d" % response.status_code)



            # Make it a soup
            soup = BeautifulSoup(page.text,'html.parser')
            lists= soup.find_all(class_='row py-2 mx-0 mb-3 bg-white deep-shadow event-card')

            saveTo = os.path.join(path, file_name)
            
            if len(lists) <= 0:
                raise ValueError("No data received for %s" % (url))
                
            
            for event_title in lists:
                _data_err = False
                _data_dict = {}
               # _data_dict['destination_id'] = destination_id,
                
                events= event_title.find_all('a', class_='text-decoration-none c-ga xn')

                for _event in events:
                    try:
                        _data_dict['event_name'] = _event.get('data-ga-label')
                        
                    except Exception as text_err:
                        _data_dict['event_name'] = None
                        _data_err = True
                        logger.warning('event_name - %s',text_err)
                        pass    
                        
                        
                    try:
                        _data_dict['event_url'] = _event.get('href')
                        
                    except Exception as text_err:
                        _data_dict['event_url'] = None
                        _data_err = True
                        logger.warning('event_url - %s',text_err)
                        pass 

                    
                    
                    
                    if bool(_data_dict):
                        _save_df = pd.concat([_save_df,pd.DataFrame(_data_dict)])
                    if _data_err:
                        logger.warning("Above text extraction errors were caused by url: \n %s \n",url)


                if _save_df.shape[0] > 0:
                    if self.storeMethod == 'local':
    #                    print(saveTo)
                        _save_df.to_csv(saveTo,index=False, sep=',')
                    elif self.storeMethod == 'AWS-S3':
                        print("todo")
                    else:
                        raise ValueError("%s is an undefined storage location in **kwargs"
                                         % (kwargs['storageLocation']))

    #                _save_df.to_csv(saveTo,index=False, sep=',')

        except Exception as err:
            logger.error("%s %s \n", __s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return saveTo


# In[31]:


''' Function
            name: extract_event_url_list
            parameters:
            
            author: <nileka.desilva@colombo.rezgateway.com>
           
    '''

    def extract_event_url_list(self, 
                          eventLinks10Times_df,   
                          event_col_name:str="Event name",   # column name with event title
                          url_col_name:str="URL",   # column name with event url
                          **kwargs,       # kwargs = {}
                         ):
        
        ''' Description: Open csv file and '''
        
        __s_fn_id__ = "function <extract_room_rate>"
        logger.info("Executing %s %s" % (self.__package__, __s_fn_id__))

        eventLinks10TimesNF_df = pd.DataFrame()
        
        
        

# eventLinks10Times_df = pd.read_csv('10timesNiagaraFallsEventURLs-RawData.csv')
# eventLinks10Times_df


# In[32]:


''' Function --- Create df with event URLS ---
author: <nileka.desilva@colombo.rezgateway.com>
'''

def _clean_df_with_events_and_urls():
    
    ''' Description: Clean dataframe with event title and event url
        '''
    
    eventLinks10TimesNF_df = eventLinks10TimesNF_df.dropna()
    eventLinks10TimesNF_df= eventLinks10TimesNF_df[eventLinks10TimesNF_df["Event name"].str.contains("Event name")==False]
    eventLinks10TimesNF_df["Event name"] = eventLinks10TimesNF_df["Event name"].apply(lambda x: x.replace("To", ""))
    eventLinks10TimesNF_df = eventLinks10TimesNF_df.reset_index(drop=True)
    


# In[28]:


''' Function --- Scrape event details from each URL ---
author: <nileka.desilva@colombo.rezgateway.com>
'''


def _open_event_urls_and_scrape_data_to_csv(self, file_name, path):
    
    saveTo = None
    _save_df = pd.DataFrame()

    __s_fn_id__ = "function <_open_event_urls_and_scrape_data_to_csv>"
#        logger.info("Executing %s", __s_fn_id__)

    try:
        links = eventLinks10TimesNF_df["URL"]
        headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
        response = requests.get(links, headers=headers)
        # check the response code
        if response.status_code != 200:
            raise RuntimeError("Invalid response with code %d" % response.status_code)





        for link in links:


            saveTo = os.path.join(path, file_name)


            response = requests.get(link)
            soup = BeautifulSoup(response.text, 'html.parser')
            currentDate = datetime.now().strftime("%m-%d-%Y")


            detail=soup.find(class_='position-relative col-md-8 pe-3')

            _data_err = False
            _data_dict = {}

            try:
                _data_dict['event_title']=detail.find('h1', class_='mb-0').text

            except Exception as text_err:
            _data_dict['event_title'] = None
            _data_err = True
            logger.warning('event_title - %s',text_err)
            pass


            try:
                _data_dict['eventDate']=detail.find('div', class_='header_date position-relative text-orange').text
            except Exception as text_err:
            _data_dict['eventDate'] = None
            _data_err = True
            logger.warning('eventDate - %s',text_err)
            pass


            try:
                _data_dict['labels']=detail.find('div', class_='mt-1 text-muted').text
            except Exception as text_err:
            _data_dict['labels'] = None
            _data_err = True
            logger.warning('labels - %s',text_err)
            pass



            information=soup.find('table', class_='table noBorder mng w-100 trBorder')



            try:
                _data_dict['turnout']=information.find('a', class_='text-decoration-none').text
            except Exception as text_err:
            _data_dict['turnout'] = None
            _data_err = True
            logger.warning('turnout - %s',text_err)
            pass

            try:
                _data_dict['category']=information.find(id='hvrout2').text
            except Exception as text_err:
            _data_dict['category'] = None
            _data_err = True
            logger.warning('category - %s',text_err)
            pass



            location=soup.find('div', class_='row fs-14 box p-0')

            try:
                _data_dict['latitude']=location.find('span', id='event_latitude').text
            except Exception as text_err:
            _data_dict['latitude'] = None
            _data_err = True
            logger.warning('latitude - %s',text_err)
            pass


            try:
                _data_dict['longitude']=location.find('span', id='event_longude').text
            except Exception as text_err:
            _data_dict['longitude'] = None
            _data_err = True
            logger.warning('longitude - %s',text_err)
            pass


            try:
                _data_dict['venue']=location.find('div', class_='mb-1').text
            except Exception as text_err:
            _data_dict['venue'] = None
            _data_err = True
            logger.warning('venue - %s',text_err)
            pass

            if bool(_data_dict):
                        _save_df = pd.concat([_save_df,pd.DataFrame(_data_dict)])
            if _data_err:
                logger.warning("Above text extraction errors were caused by url: \n %s \n",url)


        if _save_df.shape[0] > 0:
            if self.storeMethod == 'local':
    #                    print(saveTo)
                _save_df.to_csv(saveTo,index=False, sep=',')
            elif self.storeMethod == 'AWS-S3':
                print("todo")
            else:
                raise ValueError("%s is an undefined storage location in **kwargs"
                                 % (kwargs['storageLocation']))

    #                _save_df.to_csv(saveTo,index=False, sep=',')

    except Exception as err:
        logger.error("%s %s \n", __s_fn_id__, err)
        print("[Error]"+__s_fn_id__, err)
        print(traceback.format_exc())

        


# In[ ]:


''' Function
            name: extract_event_deyails_list
            parameters:
            
            author: <nileka.desilva@colombo.rezgateway.com>
           
    '''
({'Search date': currentDate, 'Event title': event_title, 'Date': eventDate, 'Labels': labels, 'Turnout': turnout, 'Latitude': latitude, 'Longitude': longitude, 'Address': venue, 'Category': category}, ignore_index=True)    

    def extract_event_details_list(self,
                                   ['Search date'], ['Event title'],
                                   ['Date'], ['Labels'], ['Turnout'], ['Latitude'],
                                   ['Longitude'], ['Address'], ['Category'],  
                          **kwargs,       # kwargs = {}
                         ):
        
        ''' Description: Open csv file and '''
        
        __s_fn_id__ = "function <extract_room_rate>"
        logger.info("Executing %s %s" % (self.__package__, __s_fn_id__))

        eventDetails10Times_df = pd.DataFrame()
        
        
        


# In[29]:


''' Function --- Open and clean df with event details ---
author: <nileka.desilva@colombo.rezgateway.com>
'''

def _clean_df_with_events_data():
    
    #remove unwanted characters and clean dataframe

    eventDetails10Times_df=eventDetails10Times_df.drop_duplicates()
    eventDetails10Times_df=eventDetails10Times_df.replace('N/A',np.NaN)
    eventDetails10Times_df['Address']=eventDetails10Times_df['Address'].str.slice(start=2)
    eventDetails10Times_df= eventDetails10Times_df.dropna(subset = ['Event title', 'Date'])
    eventDetails10Times_df['Address'] = df2['Address'].replace("nue to be announced", "Venue to be announced")
    eventDetails10Times_df = eventDetails10Times_df.reset_index(drop=True)
    
    #clean date column and define start date and end date 
    eventDetails10Times_df['Date']=eventDetails10Times_df['Date'].str.replace("Add a Review","")
    dateRange= pd.DataFrame(eventDetails10Times_df['Date'].str.split('-',1).to_list(),columns = ['Start date','End date'])
    dateRange['End date'].fillna(dateRange['Start date'], inplace=True)
    dateRange[['End day','End month','End year']] = dateRange['End date'].str.extract(r"^(.*)\s+(\S+)\s+(\d+)$", expand=True)
    dateRange[['Start day','Start month','Start year']] = dateRange['Start date'].str.extract(r"^(.*)\s+(\S+)*\s+(\d+)*$", expand=True)
    dateRange['Start day'].fillna(dateRange['Start date'], inplace=True)
    dateRange['Start month'].fillna(dateRange['End month'], inplace=True)
    dateRange['Start year'].fillna(dateRange['End year'], inplace=True)
    dateRange['Start']=dateRange['Start day'].astype(str)+dateRange['Start month'].astype(str)+ dateRange['Start year'].astype(str)
    dateRange['End']=dateRange['End day'].astype(str)+dateRange['End month'].astype(str)+dateRange['End year'].astype(str)
    dateRange['Start']=dateRange['Start day']+dateRange['Start month']+ dateRange['Start year']
    dateRange['End date']=dateRange['End']+dateRange['End month']+dateRange['End year']
    eventDetails10Times_df['Start date']= pd.to_datetime(dateRange['Start'])
    eventDetails10Times_df['End date']= pd.to_datetime(dateRange['End'])
    del eventDetails10Times_df['Date']
    
    #clean and sort turnout, category and labels column
    eventDetails10Times_df['Category']=eventDetails10Times_df['Category'].str.replace("IT","Information Technology")
    eventDetails10Times_df['Category']=eventDetails10Times_df['Category'].str.replace("Category & Type","")
    eventDetails10Times_df['Labels']=eventDetails10Times_df['Labels'].str.replace('[^a-zA-Z]+', '')
    eventDetails10Times_df['Labels']=eventDetails10Times_df['Labels'].str.replace('Ratings', '')
    eventDetails10Times_df['Labels']=eventDetails10Times_df['Labels'].str.replace('Rating', '')

    eventDetails10Times_df['Category']=eventDetails10Times_df['Category'].str.replace('Conference', '')
    eventDetails10Times_df['Category']=eventDetails10Times_df['Category'].str.replace('Trade Show', '')

    eventDetails10Times_df['Category'] = eventDetails10Times_df['Category'].replace(r"(\w)([A-Z])", r"\1 | \2", regex=True)
    eventDetails10Times_df= eventDetails10Times_df.assign(Category=eventDetails10Times_df['Category'].str.split('|')).explode('Category')
    eventDetails10Times_df = eventDetails10Times_df.rename(columns = {'index':'Event No.'})
    eventDetails10Times_df.reset_index(inplace=True)
    eventDetails10Times_df.index = np.arange(1, len(eventDetails10Times_df) + 1)

    eventDetails10Times_df['Turnout']=eventDetails10Times_df['Turnout'].str.replace("&","").replace("([A-Z][a-z]+)", "").replace("IT", "")
    eventDetails10Times_df['Turnout']=eventDetails10Times_df['Turnout'].str.replace("([A-Z][a-z]+)", "")
    eventDetails10Times_df['Turnout']=eventDetails10Times_df['Turnout'].str.replace("IT", "")
    eventDetails10Times_df['Turnout']=eventDetails10Times_df['Turnout'].replace(r'^\s*$', np.nan, regex=True)

    
    return eventDetails10Times_df


# In[ ]:




