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

    ''' Initialize with default environment variables '''
    __name__ = "eventsScraper"
    __package__ = "EventsScraper"
    __root_dir__ = "/home/nuwan/workspace/rezgate/wrangler"
    __module_path__ = os.path.join(__root_dir__, 'modules/ota/')
    __config_path__ = os.path.join(__module_path__, 'app.cfg')
    __data_path__ = os.path.join(__root_dir__, 'data/events/scraper/')

    sys.path.insert(1,__module_path__)
    import otaUtils as otau

    print("All {0} software packages loaded successfully!".format(__package__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

'''
    CLASS spefic to scraping property OTA data
'''

class EventScraper():

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

        ''' initialize util class to use common functions '''
        global clsUtil
        clsUtil = otau.Utils(desc='Utilities class for property data scraping')

        ''' Set the wrangler root directory '''
        self.rootDir = __root_dir__
        if "ROOT_DIR" in kwargs.keys():
            self.rootDir = kwargs['ROOT_DIR']

        self.modulePath = os.path.join(self.rootDir, __module_path__)     
        if "MODULE_PATH" in kwargs.keys():
            self.modulePath=kwargs['MODULE_PATH']

        self.configPath = os.path.join(self.modulePath, 'app.cfg')
        if "CONFIG_PATH" in kwargs.keys():
            self.configPath=kwargs['CONFIG_PATH']
        global config
        config = configparser.ConfigParser()
        config.read(self.configPath)

        ''' get the file and path for the logger '''
        self.logPath = os.path.join(self.rootDir,config.get('LOGGING','LOGPATH'))
        if not os.path.exists(self.logPath):
            os.makedirs(self.logPath)
#        self.logFile = os.path.join(self.logPath,config.get('LOGGING','LOGFILE'))
        ''' innitialize the logger '''
        global logger
        logger = logging.getLogger(__package__)
        logger.setLevel(logging.DEBUG)
        if (logger.hasHandlers()):
            logger.handlers.clear()
        ''' create file handler which logs even debug messages '''
        fh = logging.FileHandler(self.logFile, config.get('LOGGING','LOGMODE'))
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        ''' set a new logger section '''
        logger.info('########################################################')
        logger.info(__name__)
        logger.info('Module Path = %s', self.modulePath)

        ''' get the path to the input and output data '''
        self.dataPath = __data_path__
        try:
            if "DATA_PATH" in kwargs.keys():
                ''' first preference given to kwargs '''
                self.dataPath = kwargs['DATA_PATH']
                logger.info("Appending data path from kwargs %s" % self.dataPath)
            else:
                ''' next try the app.cfg file '''
                self.dataPath = os.path.join(self.rootDir,config.get('STORES','DATA'))
                if not self.dataPath:
                    raise ValueError("Data location not defined in %s" % self.configPath)
                else:
                    logger.info("Appending data path from configuration %s" % self.dataPath)
            
        except Exception as err:
            logger.warning("%s %s \n", _s_fn_id,err)
            logger.warning("Using default data path %s" % self.dataPath)
            print("[Warning]"+_s_fn_id, err)

        ''' select the storate method '''
        self.storeMethod = config.get('STORES','METHOD')
        
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
        self.page_offset = 10
        self.page_upper_limit = 150
        self.checkout_offset = 1

        self.destination_id = ["Las Vegas",
                               "New York",
                               "Orlando",
#                          "Boston, USA",
#                          "Colombo, Sri Lanka",
                         ]
        print("Initialing %s class for %s with instance %s" 
              % (self.__package__, self.__name__, self.__desc__))
        print("Logging info, warnings, and error to %s" % self.logPath)
        print("Data path set to %s" % self.dataPath)
        return None


    ''' Function
            name: build_scrape_url_list
            parameters:
                dir_path - path to the directory with property parameters JSON
                file_name - JSON file containing those parameters

            procedure: use the get_scrape_input_params function to load the the JSON file. 
                        Thenloop through all the OTAs to extract the input parameters
                        For each OTA template url use the insert_params_in_url function to
                        construct the list of parameterized URLs
            return: list with the set of urls (scrape_url_list)

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def get_destination_ids(self, file_name, dir_path=None, col_name=None):

        _l_dests = []

        _s_fn_id = "function <get_scrape_output_params>"
#        logger.info("Executing %s", _s_fn_id)

        try:
            if not file_name:
                raise ValueError("Invalid file name")

            ''' see if the file exists '''
            if not dir_path:
                dir_path = self.dataPath
            if dir_path[-1] != "/":
                dir_path +="/"
            file_path = dir_path+file_name
            if not os.path.exists(file_path):
                raise ValueError("File %s does not exisit in folder %s" %(file_name,dir_path))

            ''' read the destination ids into the list '''
            dest_df = pd.read_csv(file_path, sep=',', quotechar='"')
            if not col_name:
                col_name = "destinationID"
            _l_dests = list(dest_df[col_name])


        except Exception as err:
            logger.error("%s %s \n", _s_fn_id,err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_dests


    ''' Function
            name: build_scrape_url_list
            parameters:
                dir_path - path to the directory with property parameters JSON
                file_name - JSON file containing those parameters

            procedure: use the get_scrape_input_params function to load the the JSON file. 
                        Thenloop through all the OTAs to extract the input parameters
                        For each OTA template url use the insert_params_in_url function to
                        construct the list of parameterized URLs
            return: list with the set of urls (scrape_url_list)

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def build_scrape_url_list(self, file_name:str, dir_path=None, **kwargs):

        _scrape_url_dict = {}
        _ota_parameterized_url_list = [] 

        err_count = 0      # logging the number of errors
        _tmpFPath = None   # tempory file path to store the parameterized URL list
        
        _s_fn_id = "function <build_scrape_url_list>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            ''' validate and if given the dir path, else revert to defaul '''
            if not file_name:
                raise ValueError("Invalid file to fetch scraper property dictionary")
            if not dir_path:
                dir_path = os.path.join(self.rootDir,config.get('STORES','INPUTDATA'))
            
            ''' check and initialize **kwargs '''
            if 'pageOffset' in kwargs:
                self.page_offset = kwargs['pageOffset']
            if 'pageUpperLimit' in kwargs:
                self.page_upper_limit = kwargs['pageUpperLimit']
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
            if 'checkoutOffset' in kwargs:
                self.checkout_offset = kwargs['checkoutOffset']

            ''' set the directory path to csv files with destination ids '''
            if dir_path[-1] != "/":
                _dest_dir_path = dir_path+"/"+"destinations"
            else:
                _dest_dir_path = dir_path+"destinations"

            ''' retrieve OTA input and output property data from json '''
            property_dict = clsUtil.load_ota_list(os.path.join(dir_path, file_name))
            if len(property_dict) <= 0:
                raise ValueError("No data found with %s with defined scrape properties"
                                 % os.path.join(dir_path, file_name))
            else:
                logger.info("Loaded %d properties to begin scraping OTA data.", len(property_dict))
                print("Loaded %d properties to begin scraping OTA data." % (len(property_dict)))

            ''' get the input parameters from the properties file '''
            _ota_input_param_list = clsUtil.get_scrape_input_params(property_dict)
            logger.info("Input parameter list loaded successfully.")
            
            ''' loop through the  ota list to create the url list for each ota '''
            for ota in _ota_input_param_list:
                logger.info("Processing %s ...", ota['ota'])
                print("Processing %s ..." % (ota['ota']))
                _inert_param_dict = {}
                try:
                    _ota_url = None
                    if not ota['url']:
                        raise ValueError("Invalid url skip to next")

                    ''' get the list of destination ids '''
                    if "destinations" in ota.keys():
                        _l_dest = self.get_destination_ids(file_name=ota["destinations"],   # filename with destination ids
                                                           dir_path=_dest_dir_path,   # path to the desitnation folder
                                                           col_name="destinationID"  # column name with destination ids
                                                          )
                        if len(_l_dest) > 0:
                            self.destination_id = _l_dest
                            logger.info("Loaded %d destnation ids", len(_l_dest))

                    ''' build the dictionary to replace the values in the url place holder '''
                    for destID in self.destination_id:
                        _inert_param_dict['destinationID'] = destID

                        if 'checkIn' in ota['inputs']:
                            for day_count in range(0,(self.scrape_end_date - self.scrape_start_date).days):
                                _inert_param_dict['checkIn'] = self.scrape_start_date + timedelta(days=day_count)
                                ''' if checkout date is a necessary parameter then add 1 day to checkout date'''
                                if 'checkOut' in ota['inputs']:
                                    _inert_param_dict['checkOut'] = self.scrape_start_date \
                                                                    + timedelta(days=day_count+self.checkout_offset)
                                if "page" in ota['inputs']:
                                    page_offset = 0
                                    _parameterized_url = None
                                    while page_offset <= self.page_upper_limit:
                                        _inert_param_dict['page'] = page_offset
                                        _parameterized_url = clsUtil.insert_params_in_url(ota['url'],**_inert_param_dict)
#                                        scrape_url_list.append(_parameterized_url)
                                        _scrape_url_dict = {}     # init otherwise will overwrite the list
                                        _scrape_url_dict['ota']=ota['ota']
                                        _scrape_url_dict['destination_id']=destID
                                        _scrape_url_dict['checkin']=_inert_param_dict['checkIn']
                                        _scrape_url_dict['page_offset']=page_offset
                                        _scrape_url_dict['url']=_parameterized_url
                                        _ota_parameterized_url_list.append(_scrape_url_dict)
                                        page_offset += self.page_offset

                    ''' add to dict the paramterized url list for OTA as key '''
#                    print(scrape_url_list)
#                    _scrape_url_dict[ota] = scrape_url_list

                except Exception as err:
                    ''' skip to processing the next OTA url if this one is None '''
                    err_count += 1
                    logger.warning("%s", _s_fn_id+" "+err)
                    print(err)
                    
                logger.info("Build %s completed %d error ota urls", _s_fn_id, err_count)
                logger.info("Parameterized %d urls.",len(_ota_parameterized_url_list))
                if len(_ota_parameterized_url_list) > 0 and self.tmpDIR:
                    tmpDF = pd.DataFrame(_ota_parameterized_url_list)
#                    _tmpFPath = os.path.join(self.tmpDIR,"build_scrape_url_list.csv")
                    _tmp_fname = self.__package__+"-"+"build-scrape-url-list.csv"
                    _tmpFPath = os.path.join(self.tmpDIR,_tmp_fname)
                    tmpDF.to_csv(_tmpFPath, sep=',', index=False)
                    logger.info("Data written to %s",_tmpFPath)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _tmpFPath, _ota_parameterized_url_list


    ''' Function
            name: make_storage_dir
            parameters:
                dir_path - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: give the relative root strage location to amend a data & time
                        specific folder for the current time
                        
            return string (_s3Storageobj or localhost directory path)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def make_storage_dir(self, **kwargs):
        
        _s_fn_id = "function <make_storage_dir>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))
        
        _prop_search_folder = None
        
        try:
            if "PARENT_DIR" in kwargs.keys():
                parent_dir_name = kwargs['PARENT_DIR']
            else:
                parent_dir_name = "rates/"
                
            _prop_search_folder = clsUtil.get_search_data_dir_path(
                data_dir_path = self.dataPath,
                parent_dir_name = parent_dir_name,
                **kwargs)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]",+self.__package__+_s_fn_id, err)
            print(traceback.format_exc())

        return _prop_search_folder


    ''' Function
            name: _scrape_bookings_to_csv
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)

            author: <nileka.desilva@rezgateway.com>
    '''
    def _scrape_bookings_to_csv(self,
                                url,   # parameterized url
                                checkin_date, # intended checkin date
                                search_dt,    # scrape run date time
                                destination_id, # location searched for
                                file_name,     # store in csv file
                                path          # directory path to store csv
                               ):

        from bs4 import BeautifulSoup # Import for Beautiful Soup
        import requests # Import for requests
        import lxml     # Import for lxml parser
        import csv
        from csv import writer

        saveTo = None
        _save_df = pd.DataFrame()

        _s_fn_id = "function <_scrape_bookings_to_csv>"
#        logger.info("Executing %s", _s_fn_id)

        try:
            headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            response = requests.get(url, headers=headers)
            # Make it a soup
            soup = BeautifulSoup(response.text,"lxml")

            lists = soup.select(".d20f4628d0")
            lists2 = soup.select(".c8305f6688")

            saveTo = path+"/"+file_name

            if len(lists) <= 0:
                raise ValueError("No data received for %s" % (url))

            for _list in lists:
                _data_dict = {}
                _data_dict['search_dt'] = search_dt,
                _data_dict['checkin_date'] = checkin_date,
                _data_dict['property_name'] = _list.find('div', class_='fcab3ed991 a23c043802').text
                _data_dict['room_type'] = _list.find('span', class_='df597226dd').text
                _data_dict['room_rate'] = _list.find('span', class_='fcab3ed991 bd73d13072').text
                _data_dict['review_score'] = _list.find('div', class_='b5cd09854e d10a6220b4').text
                _data_dict['destination_id'] = destination_id,
                _data_dict['location_desc'] = _list.find('div', class_='a1fbd102d9').text
                _data_dict['other_info'] = _list.find('div', class_='d22a7c133b').text

                if bool(_data_dict):
                    _save_df = pd.concat([_save_df,pd.DataFrame(_data_dict)])

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
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return saveTo


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

    def scrape_url_list(self,otaURLlist, searchDT: datetime, dir_path:str):

        saveTo = None   # init file name
        _l_saved_files = []

        _s_fn_id = "function <scrape_url_list>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if len(otaURLlist) > 0:
                logger.info("loading parameterized urls from list %d records", len(otaURLlist))
                print("loading parameterized urls from list %d records" % len(otaURLlist))
            else:
                raise ValueError("List of URLs required to proceed; non defined in list.")

            ''' loop through the list of urls to scrape and save the data'''
            for ota_dict in otaURLlist:

                ''' file name is concaternation of ota name + location + checkin date + page offset and .csv file extension'''
                _fname = str(ota_dict['ota'])+"."+\
                        str(ota_dict['destination_id'])+"."+\
                        str(ota_dict['checkin'])+"."+\
                        str(ota_dict['page_offset']).zfill(3)+\
                        ".csv"
                _fname=_fname.replace(" ",".")

                ''' TODO add search_datetime'''
                if ota_dict['ota'] == 'booking.com':
                    saveTo = self._scrape_bookings_to_csv(ota_dict['url'],      # constructed url with parameters
                                                          ota_dict['checkin'],  # booking intended checkin date
                                                          searchDT,   # date & time scraping was executed
                                                          ota_dict['destination_id'],  # destingation id to lookup the name
                                                          _fname,       # csv file name to store in
                                                          dir_path     # folder name to save the files
                                                         )
                    _l_saved_files.append(saveTo)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_saved_files
