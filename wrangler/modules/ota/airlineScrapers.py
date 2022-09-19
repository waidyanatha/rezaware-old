#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

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
    
    __name__ = "airlineScrapers"
    __package__ = "AirlineScraper"

    print("All {0} software packages loaded successfully!".format(__package__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))



'''
    CLASS spefic to scraping date from Online Travel Ageny web sites; e.g. booking.com, hotel.com,
    Use a scheduler to run main.py which will execute the relavent functions to scrape and file the data
        1) Load the ota_input_data.json comprising the url with parameter list and specific data find html tags
        2) separate the scrape inputr paramenter list for the url from the scrape data find html tags
        3) Inset the input parameters into the url to build a list of scrapable urls
        4) Scraped data is parsed, using the html tags find
        5) data is stored according to the specified folder and structure
'''

class AirlineScraper():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="data", **kwargs):
        
        self.__name__ = __name__
        self.__package__ = __package__
        self.__desc__ = desc

        ''' Set the wrangler root directory '''
        self.rootDir = "./wrangler"
        if "ROOT_DIR" in kwargs.keys():
            self.rootDir = kwargs['ROOT_DIR']
        if self.rootDir[-1] != "/":
            self.rootDir +="/"

        self.modulePath = os.path.join(self.rootDir, 'modules/ota/')
        self.configPath = os.path.join(self.modulePath, 'app.cfg')
        global config
        config = configparser.ConfigParser()
        config.read(self.configPath)

        ''' get the file and path for the logger '''
        self.logPath = os.path.join(self.rootDir,config.get('LOGGING','LOGPATH'))
        if not os.path.exists(self.logPath):
            os.makedirs(self.logPath)
        self.logFile = os.path.join(self.logPath,config.get('LOGGING','LOGFILE'))

        ''' innitialize the logger '''
        global logger
        logger = logging.getLogger(self.__package__)
        logger.setLevel(logging.DEBUG)
        if (logger.hasHandlers()):
            logger.handlers.clear()
        # create file handler which logs even debug messages
        fh = logging.FileHandler(self.logFile, config.get('LOGGING','LOGMODE'))
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        ''' set a new logger section '''
        logger.info('########################################################')
        logger.info(self.__package__)
        logger.info('Module Path = %s', self.modulePath)
        ''' get the path to the input and output data '''
        if "DATA_PATH" in kwargs.keys():
            self.dataPath = os.path.join(self.rootDir,kwargs["DATA_PATH"])
        else:
            self.dataPath = os.path.join(self.rootDir,config.get('STORES','DATA'))
        self.path = os.path.join(self.rootDir,config.get('STORES','DATA'))
        logger.info("Data store path: %s", self.dataPath)
        ''' select the storate method '''
        self.storeMethod = config.get('STORES','METHOD')
        ''' default: ../../data/hospitality/bookings/scraper/rates '''
        self.ratesStoragePath = self.path+'rates'
        self.file = "ota_input_urls.json"
        
        ''' set the tmp dir to store large data to share with other functions
            if self.tmpDIR = None then data is not stored, otherwise stored to
            given location; typically specified in app.conf
        '''
        self.tmpDIR = None
        if "WRITE_TO_FILE":
            self.tmpDIR = os.path.join(self.rootDir,config.get('STORES','TMPDATA'))
            if not os.path.exists(self.tmpDIR):
                os.makedirs(self.tmpDIR)

        self.scrape_start_date = date.today()
        self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
#         self.page_offset = 10
#         self.page_upper_limit = 150
        self.return_flight_offset = 1

        self.destination_id = ["Las Vegas",
                               "New York",
                               "Orlando",
#                          "Boston, USA",
#                          "Colombo, Sri Lanka",
                         ]
        ''' DEPRATED -- because read_folder_csv_to_df is DEPRECATED '''
#         self.scrape_columns = ["search_dt", # date&time scraping ran
#                                "property_name",   # hotel name
#                                "checkin_date",    # anticipated checkin date
#                                "destination_id",  # hotel destination id
#                                "destination_name",   # hotel located city
#                                "destination_country",# hotel located country
#                                "room_type",    # room type; e.g., double bed, queen bed
#                                "adult_count",  # number of adults
#                                "child_count",  # numbe of children
#                                "room_rate",    # price per night
#                                "review_score", # rating
#                                "other_info",   # any other relevant text
#                               ]
        print("Initialing %s class for %s with instance %s" 
              % (self.__package__, self.__name__, self.__desc__))
        return None

    ''' Function
            name: get_url_info
            parameters:
                dirPath - the relative or direct path to the file with urls
                fileName - the name of the file containing all the urls for scraping
            procedure: read the list of urls from the CSV file and compile a list
            return list (url_info)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def load_ota_info(self, dirPath:str, fileName:str):

        import os         # apply directory read functions
        import csv        # to read the csv
        import json       # to read the json file

        url_info = []     # initialize the return parameter
        property_data = {}
        
        _s_fn_id = "function <load_ota_info>"
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
    def get_scrape_input_params(self, airline_dict:dict):

        _s_fn_id = "function <get_scrape_input_params>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            ''' check for property dictionary '''
            if not airline_dict:
                raise ValueError("Invalid dictionary")

            ''' loop through the dict to construct the scraper parameters '''
            ota_param_info = []
            _l_tag=[]
            for prop_detail in airline_dict:
                param_dict = {}
                tag_dict = {}
                ''' create a dict with input params '''
                param_dict['ota'] = prop_detail
                for detail in airline_dict[prop_detail]:
                    param_dict['url'] = detail['url']
                    param_dict['inputs'] = detail['inputs']
#                    param_dict['destinations'] = detail['destinations']
                    ''' append the input parameters into a list'''
                    ota_param_info.append(param_dict)
      
        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return ota_param_info #, ota_scrape_tags_df

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
            name: get_flight_sectors
            parameters:
                dirPath - path to the directory with property parameters JSON
                fileName - JSON file containing those parameters
                kwargs - 
                    codeColName
                    departAirportCode
                    arriveAirportCode

            procedure: use the get_scrape_input_params function to load the the JSON file. 
                        Thenloop through all the OTAs to extract the input parameters
                        For each OTA template url use the insert_params_in_url function to
                        construct the list of parameterized URLs
            return: list with the set of urls (scrape_url_info)

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def get_flight_sectors(self, file_path:str, **kwargs):

        _l_sectors = []

        _s_fn_id = "function <get_flight_sectors>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if not os.path.exists(file_path):
                raise ValueError("File %s does not exisit" %(file_path))

            ''' see if the file exists '''
#            if not dirPath:
#                dirPath = self.path
#             if dirPath[-1] != "/":
#                 dirPath +="/"
#            file_path = os.path.join(dirPath,fileName)

            ''' read the destination ids into the list '''
            airports_df = pd.read_csv(file_path, sep=',', quotechar='"')
            if airports_df.shape[0] <=0:
                raise ValueError("No airport data found in %s" %(file_path))
            ''' set the airport column name '''
            if "codeColName" in kwargs.keys():
                col_name = kwargs["codeColName"]
            else:
                col_name = "airportCode"
            ''' if given in kwargs set depart and arrive columns from list '''
            if "departAirportCode" in kwargs.keys():
                _l_depart_cols = kwargs['departAirportCode']
            else:
                _l_depart_cols = list(airports_df[col_name])
            if "arriveAirportCode" in kwargs.keys():
                _l_arrive_cols = kwargs['arriveAirportCode']
            else:
                _l_arrive_cols = list(airports_df[col_name])
            print(_l_depart_cols,_l_arrive_cols)
            ''' create a tuples of depart arrive combinations from the list '''
            for departure in _l_depart_cols:
                for arrival in _l_arrive_cols:
                    if departure != arrival:
                        _l_sectors.append(tuple([departure,arrival]))

            if len(_l_sectors) > 0:
                logger.info("Create %d departure and arrival sector tuples" % len(_l_sectors))
            else:
                raise ValueError("No destination and arrival tuples created!")

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id,err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_sectors


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
            name: build_scrape_url_info
            parameters:
                dirPath - path to the directory with property parameters JSON
                fileName - JSON file containing those parameters

            procedure: use the get_scrape_input_params function to load the the JSON file. 
                        Thenloop through all the OTAs to extract the input parameters
                        For each OTA template url use the insert_params_in_url function to
                        construct the list of parameterized URLs
            return: list with the set of urls (scrape_url_info)

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def build_scrape_url_info(self, fileName:str, dirPath=None, **kwargs):

        _scrape_url_dict = {}
        _ota_parameterized_url_info = [] 

        err_count = 0      # logging the number of errors
        _tmpFPath = None   # tempory file path to store the parameterized URL list
        
        _s_fn_id = "function <build_scrape_url_info>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if not dirPath:
                ''' TODO: get from libOTAETL main lib '''
                dirPath = os.path.join(self.rootDir,config.get('STORES','INPUTDATA'))
            
            ''' check and initialize **kwargs '''
            if 'startDate' in kwargs:
                self.scrape_start_date = kwargs['startDate']
            else:
                logger.warning("Invalid scrape start date. Setting start date to today %s", str(self.scrape_start_date))
                print("Invalid scrape start date. Setting start date to today %s" %(str(self.scrape_start_date)))
            if 'endDate' in kwargs:
                if self.scrape_start_date < kwargs['endDate']:
                    self.scrape_end_date = kwargs['endDate']
                else:
                    self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
                    logger.warning("Invalid scrape end date. Setting end date to %s %s",
                                str(self.scrape_start_date),str(self.scrape_end_date))
                    print("Invalid scrape end date. Setting end date to %s %s" 
                          %(str(self.scrape_start_date),str(self.scrape_end_date)))
            if 'returnFlightOffset' in kwargs:
                self.return_flight_offset = kwargs['returnFlightOffset']

            ''' retrieve OTA input and output property data from json '''
            airline_dict = self.load_ota_info(dirPath, fileName)
            if len(airline_dict) <= 0:
                raise ValueError("No data found with %s with defined scrape properties"
                                 % (path+"/"+fileName))
            else:
                logger.info("Loaded %d properties to begin scraping OTA data.", len(airline_dict))
                print("Loaded %d properties to begin scraping OTA data." % (len(airline_dict)))

            ''' set the directory path to csv files with airport codes '''
            if "airportsFile" in kwargs.keys():
                airports_file_path = os.path.join(dirPath,kwargs["airportsFile"])
                logger.info("Airports code file path set to %s" % airports_file_path)
            else:
                raise ValueError("No csv file with airport codes defined in **kwargs")
            ''' get the tuples of departure and arrival airport code combinations '''
            _l_flight_sectors = self.get_flight_sectors(file_path=airports_file_path, **kwargs)
            logger.info("Airport codes with %d combinations loaded successfully." % len(_l_flight_sectors))

            ''' get the input parameters from the properties file '''
            _ota_input_param_info = self.get_scrape_input_params(airline_dict)
            logger.info("Input parameter list loaded successfully.")
            
            ''' loop through the  ota list to create the url list for each ota '''
            for ota in _ota_input_param_info:
                logger.info("Processing %s ...", ota['ota'])
                print("Processing %s ..." % (ota['ota']))
                _inert_param_dict = {}
                try:
                    _ota_url = None
                    if not ota['url']:
                        raise ValueError("Invalid url skip to next")

                    ''' build the dictionary to replace the values in the url place holder '''
                    for _flight_sector in _l_flight_sectors:
                        _inert_param_dict['departurePort'] = _flight_sector[0]
                        _inert_param_dict['arrivalPort'] = _flight_sector[1]

                        ''' TODO: append additional attributes to _inert_param_dict instead of a new _scrape_url_dict '''
                        for day_count in range(0,(self.scrape_end_date - self.scrape_start_date).days):
                            _inert_param_dict['flightDate'] = self.scrape_start_date + timedelta(days=day_count)
                            _parameterized_url = None
                            _parameterized_url = self.insert_params_in_url(ota['url'],**_inert_param_dict)
                            _scrape_url_dict = {}     # init otherwise will overwrite the list
                            _scrape_url_dict['ota']=ota['ota']
                            _scrape_url_dict['departurePort']=_flight_sector[0]
                            _scrape_url_dict['arrivalPort']=_flight_sector[1]
                            _scrape_url_dict['flightDate']=_inert_param_dict['flightDate']
                            _scrape_url_dict['url']=_parameterized_url
                            _ota_parameterized_url_info.append(_scrape_url_dict)
                            
                except Exception as err:
                    ''' skip to processing the next OTA url if this one is None '''
                    err_count += 1
                    logger.warning("%s", _s_fn_id+" "+err)
                    print(err)
                    
                logger.info("Build %s completed %d error ota urls", _s_fn_id, err_count)
                logger.info("Parameterized %d urls.",len(_ota_parameterized_url_info))
                if len(_ota_parameterized_url_info) > 0 and self.tmpDIR:
                    tmpDF = pd.DataFrame(_ota_parameterized_url_info)
                    _tmp_fname = self.__package__+"-"+"build-scrape-url-list.csv"
                    _tmpFPath = os.path.join(self.tmpDIR,_tmp_fname)
                    tmpDF.to_csv(_tmpFPath, sep=',', index=False)
                    logger.info("Data written to %s",_tmpFPath)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _tmpFPath, _ota_parameterized_url_info


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

#     ''' Function DEPRECATED
#             name: scrape_ota_to_csv
#             parameters:
#                 url - string comprising the url with place holders
#                 **kwargs - contain the plance holder key value pairs

#             procedure: build the url by inserting the values from the **kwargs dict
#             return string (url)

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def scrape_data_to_csv(self,url,_scrape_tags_df,fileName, path):

#         from bs4 import BeautifulSoup # Import for Beautiful Soup
#         import requests # Import for requests
#         import lxml     # Import for lxml parser
#         import csv
#         from csv import writer

#         _s_fn_id = "function <scrape_data_to_csv>"
#         logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

#         try:
#             if _scrape_tags_df.shape[0] <= 0:
#                 raise ValueError("Invalid scrape tags no data scraped")
#             if not fileName:
#                 raise ValueError("Invalid file name no data scraped")
#             if not path:
#                 raise ValueError("Invalid path name no data scraped")

#             ''' define generic header '''
#             headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
#             response = requests.get(url, headers=headers)
#             # Make it a soup
#             soup = BeautifulSoup(response.text,"lxml")
# #            soup = BeautifulSoup(response.text,"html.parser")

#             ''' extract the list of values from content block '''
#             _cont_block = (_scrape_tags_df.loc[_scrape_tags_df['variable']=='content_block']).head(1)
#             _l_scrape_text = soup.select(_cont_block.tag.item())

#             if len(_l_scrape_text) <= 0:
#                 raise ValueError("no content block (area) for %s" %(_cont_block))

#             ''' get the attribute list '''
#             _l_col_names = list(_scrape_tags_df.variable)
#             _l_col_names.remove('content_block')

#             ''' init dataframe to store the scraped categorical text '''
#             _prop_data_df = pd.DataFrame()

#             ''' loop through the list to retrieve values from tags '''
#             for row in _l_scrape_text:
#                 _scraped_data_dict = {}
#                 for colName in _l_col_names:
#                     _tag = _scrape_tags_df.loc[_scrape_tags_df.variable==colName, 'tag'].item()
#                     _code = _scrape_tags_df.loc[_scrape_tags_df.variable==colName, 'code'].item()

#                     try:
#                         _scraped_data_dict[colName] = row.find(_tag, class_ = _code).text

#                     except Exception as err:
#                         pass
                        
#                 if _scraped_data_dict:
#                     _prop_data_df = pd.concat([_prop_data_df, pd.DataFrame(_scraped_data_dict, index=[0])])

#         except Exception as err:
#             logger.error("%s %s \n", _s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return _prop_data_df

    ''' Function
            name: _scrape_kayak_to_csv
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)

            author: <nileka.desilva@rezgateway.com>
    '''
    def _scrape_kayak_to_csv(self,
                                url,   # parameterized url
                                flight_date, # intended flight date
                                search_dt,    # scrape run date time
                                departure_port,
                                arrival_port,
                                fileName,     # store in csv file
                                path          # directory path to store csv
                               ):

        from bs4 import BeautifulSoup # Import for Beautiful Soup
        import requests # Import for requests
        import lxml     # Import for lxml parser
        import csv
        from csv import writer
        import re

        saveTo = None
        _save_df = pd.DataFrame()

        _s_fn_id = "function <_scrape_kayak_to_csv>"
#        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            response = requests.get(url, headers=headers)
            # check the response code
            if response.status_code != 200:
                raise RuntimeError("Invalid response with code %d" % response.status_code)

            # Make it a soup
            soup = BeautifulSoup(response.text,"lxml")

            _find_infos = soup.select(".resultWrapper")

            if len(_find_infos) <= 0:
                raise ValueError("No data received for %s" % (url))

            ''' name of the csv file to save scraped data to '''
            saveTo = path+"/"+fileName

            ''' extrac strings and cleanup the \n with spaces '''
            for _info in _find_infos:
                _data_dict = {}
                _data_dict['search_dt'] = search_dt,
                _data_dict['depart_port_code'] = departure_port,
                _data_dict['arrive_port_code'] = arrival_port,
                ''' departure and arrival times '''
                _flight_times = _info.find('div', class_='section times').text,
                _data_dict['flight_times'] = re.sub('\n+', ' ', (_flight_times[0]).lstrip().rstrip())
                ''' number of stops '''
                num_stops = _info.find('div', class_='section stops').text,
                _data_dict['num_stops'] = re.sub('\n+', ' ', (num_stops[0]).lstrip().rstrip())
                ''' flight travel time '''
                _duration = _info.find('div', class_='section duration allow-multi-modal-icons').text,
                _data_dict['duration'] = re.sub('\n+', ' ', (_duration[0]).lstrip().rstrip())
                ''' trip price '''
                _unit_rate = _info.find('span', class_='price-text').text,
                _data_dict['booking_rate'] = re.sub('\n+', ' ', (_unit_rate[0]).lstrip().rstrip())
                _data_dict['num_passengers'] = 1
                ''' cabin type '''
                _flight_type = _info.find('div', class_='above-button').text,
                _data_dict['cabin_type'] = re.sub('\n+', ' ', (_flight_type[0]).lstrip().rstrip())
                ''' airline name '''
                _airline = _info.find('span', class_='name-only-text').text,
                _data_dict['airline_name'] = re.sub('\n+', ' ', (_airline[0]).lstrip().rstrip())

                if bool(_data_dict):
                    _save_df = pd.concat([_save_df,pd.DataFrame(_data_dict)])

            if _save_df.shape[0] > 0:

                ''' split the departure and arrival times '''
                _save_df.flight_times = _save_df.flight_times.replace(r'\s+', ' ', regex=True)
                dtime_regex = "(?i)(\d?\d:\d\d (a|p)m)"
                atime_regex = "\s(\d?\d:\d\d (a|p)m)"
                _save_df[['depart_time','dtmp']] = _save_df.flight_times.str.extract(dtime_regex, expand=False)
                _save_df['depart_time'] = pd.to_datetime(_save_df['depart_time'], format='%I:%M %p').dt.strftime('%H:%M')
                _save_df['depart_time'] = _save_df['depart_time'].astype('string')
                _save_df[['arrive_time','atmp']] = _save_df.flight_times.str.extract(atime_regex, expand=False)
                _save_df['arrive_time'] = pd.to_datetime(_save_df['arrive_time'], format='%I:%M %p').dt.strftime('%H:%M')
                _save_df['arrive_time'] = _save_df['arrive_time'].astype('string')
                _save_df=_save_df.drop(['dtmp','atmp'],axis=1)

                ''' extract the booking price '''
                _save_df.booking_rate = _save_df.booking_rate.replace(r'\s+', ' ', regex=True)
#                price_regex = "(?i)(\d+)"
                price_regex = "(?i)(?:\d+,*\d+)"
                _save_df['booking_price'] = _save_df.booking_rate.str.extract(price_regex, expand=False)
                _save_df['currency'] = 'US$'

                ''' save using storage method 
                    TODO -- move this to libOTAETL to handle storage '''
                if self.storeMethod == 'local':
                    _save_df.to_csv(saveTo,index=False, sep=',')
                elif self.storeMethod == 'AWS-S3':
                    print("todo")
                else:
                    raise ValueError("%s is an undefined storage location in **kwargs"
                                     % (kwargs['storageLocation']))

#                _save_df.to_csv(saveTo,index=False, sep=',')
            
        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return saveTo


    ''' Function DEPRECATED moved to libOTAETL to see if this can be generalized
            name: scrape_url_info
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

    def scrape_url_info(self,otaURLlist, searchDT: datetime, dirPath:str):

        saveTo = None   # init file name
        _l_saved_files = []

        _s_fn_id = "function <scrape_url_info>"
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
                        str(ota_dict['departurePort'])+"."+\
                        str(ota_dict['arrivalPort'])+"."+\
                        str(ota_dict['flightDate'])+\
                        ".csv"
                _fname=_fname.replace(" ",".")

#                print("Processsing ota=%s location=%s for checkin=%s and page=%s" 
#                      % (ota_dict['ota'],ota_dict['location'],str(ota_dict['checkin']),str(ota_dict['page_offset'])))
                ''' TODO add search_datetime'''
                if ota_dict['ota'] in ['kayak.com','momondo.com']:
                    saveTo = self._scrape_kayak_to_csv(
                        url=ota_dict['url'],   # constructed url with parameters
                        flight_date=ota_dict['flightDate'],  # intended departure date
                        search_dt=searchDT,    # date & time scraping was executed
                        departure_port=ota_dict['departurePort'],  # departure airport code
                        arrival_port=ota_dict['arrivalPort'],      # arrival airport code
                        fileName=_fname,        # csv file name to store in
                        path=dirPath,           # folder name to save the files
                                                         )
                    _l_saved_files.append(saveTo)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_saved_files


    ''' Function - DEPRECATED moved to libOTAETL
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

#     def remove_empty_files(self,path):

#         _s_fn_id = "function <remove_empty_files>"
# #        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

#         _l_removed_files = []
#         try:
#             if not path:
#                 path = self.path
            
# #            print(list(os.walk(path)))
#             for (dirpath, folder_names, files) in os.walk(path):
#                 for filename in files:
#                     file_location = dirpath + '/' + filename  #file location is location is the location of the file
#                     if os.path.isfile(file_location):
#                         if os.path.getsize(file_location) == 0:#Checking if the file is empty or not
#                             os.remove(file_location)  #If the file is empty then it is deleted using remove method
#                             _l_removed_files.append(filename)
# #                        else:
# #                            with open(file_location, 'r') as infile, \
# #                                open(file_location, 'w') as outfile:
# #                                data = infile.read()
# #                                data = data.replace("•", " ")
# #                                outfile.write(data)


#         except Exception as err:
#             logger.error("%s %s", _s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return _l_removed_files
