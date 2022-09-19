#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

class otaETL():

    '''
        CLASS ETL class will contain the functions required to run the various
        
        author(s): <nuwan.waidyanatha@rezgateway.com>
    '''


    ''' Load necessary and sufficient python librairies that are used throughout the class'''
    try:
        ''' standard python packages '''
        import os
        import sys
        import logging
        import traceback
        import configparser
        import pandas as pd
        from datetime import datetime, date, timedelta

        ''' inhouse packages '''
        ROOT_DIR = "~/workspace/rezgate/wrangler"
        MODULE_PATH = os.path.join(ROOT_DIR, 'modules/ota/')
        sys.path.insert(1,MODULE_PATH)

        import otaWebScraper as otaws

        ''' read the conicuration data'''
        CONFIG_PATH = os.path.join(MODULE_PATH, 'app.cfg')
        CONFIG_PATH = "app.cfg"
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH)
        ''' get the path to the input and output data '''
        DATA_PATH = os.path.join(ROOT_DIR,config.get('STORES','DATA'))
        print("Data files path: ", DATA_PATH)
        print("All otaETL software packages loaded successfully!")
        
        config = configparser.ConfigParser()
        config.read('app.cfg')
        print(config['hosts'])

    except Exception as e:
        print("Some software packages in otaETL didn't load\n{}".format(e))


    def __init__(module,name, **kwards):

        
        
        module.name = "ota"
        if name:
            module.name = name
        print(module.name)
        
        return None

    class extract():
        def __init__(self,**kwards):
            return None
        
    class transform():
        def __init__(self,**kwards):
            return None

    class transform():
        def __init__(self,**kwards):
            return None

    ''' Function
            name: get_url_list
            parameters:
                dirPath - the relative or direct path to the file with urls
                fileName - the name of the file containing all the urls for scraping
            procedure: read the list of urls from the CSV file and compile a list
            return list (url_list)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def load_ota_list(self, dirPath:str, fileName:str):

        import os         # apply directory read functions
        import csv        # to read the csv
        import json       # to read the json file

        url_list = []     # initialize the return parameter
        property_data = {}
        
        _s_fn_id = "function <load_ota_list>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:

            ''' Get the list of urls from the CSV file '''        
            if dirPath:
                self.path = dirPath
            _l_files = os.listdir(self.path)
            ''' if fileName is not null check if it is in the director '''
            if fileName and fileName in _l_files:
                self.file = fileName
            else:
                raise ValueError("Invalid file name %s in dir: %s. Does not exist!" % (fileName, self.path))

            ''' read the list of urls from the file '''
            with open(self.path+"/"+self.file, newline='') as f:
                property_data = json.load(f)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id,err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return property_data

    ''' Function
            name: get_scrape_input_params
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)
            
            author: <nuwan.waidyanatha@rezgateway.com>

            TODO - change the ota_scrape_tags_df to a list of dictionaries
    '''
    def get_scrape_input_params(self, inputs_dict:dict):

        _s_fn_id = "function <get_scrape_input_params>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            ''' check for property dictionary '''
            if not inputs_dict:
                raise ValueError("Invalid dictionary")

            ''' loop through the dict to construct the scraper parameters '''
            ota_param_list = []
            _l_tag=[]
            for prop_detail in inputs_dict:
                param_dict = {}
                tag_dict = {}
                ''' create a dict with input params '''
                param_dict['ota'] = prop_detail
                for detail in inputs_dict[prop_detail]:
                    param_dict['url'] = detail['url']
                    param_dict['inputs'] = detail['inputs']
#                    param_dict['destinations'] = detail['destinations']
                    ''' append the input parameters into a list'''
                    ota_param_list.append(param_dict)
      
        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return ota_param_list #, ota_scrape_tags_df

    ''' Function
            name: get_scrape_output_params
            parameters:
                airline_dict - obtained from loading the property scraping parameters from the JSON

            procedure: loop through the loaded dictionary to retrieve the output variable names, tags, and values.
                        Then construcct and return a dataframe for all corresponding OTAs
            return dataframe (_scrape_tags_df)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_scrape_html_tags(self, airline_dict:dict):

        _scrape_tags_df = pd.DataFrame()

        _s_fn_id = "function <get_scrape_output_params>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if not airline_dict:
                raise ValueError("Invalid properties dictionary")

            ''' loop through the dict to construct html tags to retrieve the data elements '''
            for prop_detail in airline_dict:
                for _prop_params in airline_dict[prop_detail]:
                    for _out_vars in _prop_params['outputs']:
                        _out_vars['ota'] = prop_detail
                        _scrape_tags_df = pd.concat([_scrape_tags_df,\
                                                     pd.DataFrame([_out_vars.values()], columns=_out_vars.keys())],
                                                   ignore_index=False)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id,err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _scrape_tags_df

    ''' Function
            name: insert_params_in_url
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def insert_params_in_url(self, url: str, **kwargs ):

        import re

        url_w_params = None

        _s_fn_id = "function <insert_params_in_url>"
#        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if not url:
                raise ValueError("Invalid url string %s" % (url))
            url_w_params = url

            ''' match the keys in dict with the placeholder string in the url''' 
            for key in kwargs.keys():
                _s_regex = r"{"+key+"}"
                urlRegex = re.compile(_s_regex, re.IGNORECASE)
                param = urlRegex.search(url_w_params)
                if param:
                    _s_repl_val = str(kwargs[key]).replace(" ","%20")
                    url_w_params = re.sub(_s_regex, _s_repl_val, url_w_params)
            
        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return url_w_params

    ''' Function
            name: read_folder_csv_to_df
            parameters:
                dirPath - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: give the relative root strage location to amend a data & time
                        specific folder for the current time
                        
            return string (_s3Storageobj or localhost directory path)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_search_data_dir_path(self,dirPath, **kwargs):

        _SearchDataDir = None
        _search_dt = datetime.now()
        
        _s_fn_id = "function <scrape_data_to_csv>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            ''' establish the storage block '''
            if dirPath:
                self.ratesStoragePath = dirPath
            ''' add the folder if not exists '''
            if self.ratesStoragePath[-1] != "/":
                self.ratesStoragePath +="/"
#            if not os.path.exists(self.ratesStoragePath):
#                os.makedirs(self.ratesStoragePath)

            if "searchDateTime" in kwargs.keys():
                _search_dt = kwargs["searchDateTime"]

            ''' TODO - change the function to round search datetime to nearest 30 min
                using _distinct_df['search_datetime']=_distinct_df['search_datetime'].dt.round('30min') '''
            ''' pick the year, month, day, hour, min from search date time ''' 
            _minute = _search_dt.minute
            if _minute < 30:
                _minute = 0
            else:
                _minute = 30

            ''' folder is a concaternation of date hour and minute; where minute < 30 --> 0 and 30 otherwise'''
            _SearchDataDir = self.ratesStoragePath+\
                            str(_search_dt.year)+"-"+str(_search_dt.month)+"-"+str(_search_dt.day)\
                            +"-"+str(_search_dt.hour)+"-"+str(_minute)+"/"     # csv file name

            ''' add the folder if not exists '''
            if kwargs['storageLocation'] == 'local':
                if not os.path.exists(self.ratesStoragePath):
                    os.makedirs(self.ratesStoragePath)
                if not os.path.exists(_SearchDataDir):
                    os.makedirs(_SearchDataDir)
            elif kwargs['storageLocation'] == 'AWS_S3':
                print("todo")
            else:
                raise ValueError("%s is an undefined storage location in **kwargs"
                                 % (kwargs['storageLocation']))

            logger.info("Extracting data into %s storage", kwargs['storageLocation'])
            logger.info("OTA price data storage location: %s", _SearchDataDir)
            logger.info("Search datetime set to: %s", str(_search_dt))

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _SearchDataDir

    ''' Function
            name: scrape_url_list
            parameters:
                otaURLlist - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def scrape_url_list(self,otaURLlist, searchDT: datetime, dirPath:str):

        saveTo = None   # init file name
        _l_saved_files = []

        _s_fn_id = "function <scrape_url_list>"
#        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if len(otaURLlist) > 0:
                logger.info("loading parameterized urls from list %d records", len(otaURLlist))
                print("loading parameterized urls from list %d records" % len(otaURLlist))
            else:
                raise ValueError("List of URLs required to proceed; non defined in list.")

            ''' loop through the list of urls to scrape and save the data'''
            for ota_dict in otaURLlist:
#                _ota_tags_df = _scrape_tags_df.loc[_scrape_tags_df['ota']==ota_dict['ota']]

                ''' file name is concaternation of ota name + location + checkin date + page offset and .csv file extension'''
                _fname = str(ota_dict['ota'])+"."+\
                        str(ota_dict['destination_id'])+"."+\
                        str(ota_dict['checkin'])+"."+\
                        str(ota_dict['page_offset']).zfill(3)+\
                        ".csv"
                _fname=_fname.replace(" ",".")

#                print("Processsing ota=%s location=%s for checkin=%s and page=%s" 
#                      % (ota_dict['ota'],ota_dict['location'],str(ota_dict['checkin']),str(ota_dict['page_offset'])))
                ''' TODO add search_datetime'''
                if ota_dict['ota'] == 'booking.com':
                    saveTo = self._scrape_bookings_to_csv(ota_dict['url'],      # constructed url with parameters
                                                          ota_dict['checkin'],  # booking intended checkin date
                                                          searchDT,   # date & time scraping was executed
                                                          ota_dict['destination_id'],  # destingation id to lookup the name
                                                          _fname,       # csv file name to store in
                                                          dirPath     # folder name to save the files
                                                         )
                    _l_saved_files.append(saveTo)
#                    print("Data saved to %s" % saveTo)
#                else:
#                    print("Define a scraper function for %s, no data saved" % ota_dict['ota'])
#    _scraped_data_df = clsScraper.scrape_data_to_csv(ota_dict['url'],_ota_tags_df,_fname, _s3object)
#    _scraped_data_df = clsScraper._scrape_data_to_csv(ota_dict['url'],_fname, _s3object)
#            print("Scraping and data save to csv complete!")

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_saved_files

    ''' Function
            name: remove_empty_files
            parameters:
                dirPath - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def remove_empty_files(self,path):

        _s_fn_id = "function <remove_empty_files>"
#        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        _l_removed_files = []
        try:
            if not path:
                path = self.path

            for (dirpath, folder_names, files) in os.walk(path):
                for filename in files:
                    file_location = dirpath + '/' + filename  #file location is location is the location of the file
                    if os.path.isfile(file_location):
                        if os.path.getsize(file_location) == 0:#Checking if the file is empty or not
                            os.remove(file_location)  #If the file is empty then it is deleted using remove method
                            _l_removed_files.append(filename)


        except Exception as err:
            logger.error("%s %s", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_removed_files
