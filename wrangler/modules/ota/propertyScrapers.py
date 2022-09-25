#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "propertyScraper"
__package__ = "PropertyScraper"
__root_dir__ = "/home/nuwan/workspace/rezgate/wrangler"
__module_dir__ = 'modules/ota/'
__data_dir__ = 'data/hospitality/bookings/scraper/'
__conf_fname__ = 'app.cfg'
__logs_dir__ = 'logs/module/ota/'
__log_fname__ = 'app.log'

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

#     ''' Initialize with default environment variables '''
#     __name__ = "propertyScraper"
#     __package__ = "PropertyScraper"
#     __root_dir__ = "/home/nuwan/workspace/rezgate/wrangler"
#     __module_dir__ = os.path.join(__root_dir__, 'modules/ota/')
#     __data_dir__ = os.path.join(__root_dir__, 'data/hospitality/bookings/scraper/')
#     __conf_fname__ = os.path.join(__module_dir__, 'app.cfg')
#     __logs_dir__ = os.path.join(__root_dir__, 'logs/module/ota/')
#     __log_fname__ = 'app.log'

    sys.path.insert(1,__module_dir__)
    import otaUtils as otau

    print("All {0} software packages loaded successfully!".format(__package__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

'''
    CLASS spefic to scraping property OTA data
'''

class PropertyScraper():

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

        self.moduleDir = os.path.join(self.rootDir, __module_dir__)
        if "MODULE_DIR" in kwargs.keys():
            self.moduleDir=kwargs['MODULE_DIR']

        self.confFPath = os.path.join(self.moduleDir, __conf_fname__)
        if "CONFIG_PATH" in kwargs.keys():
            self.confFPath=kwargs['CONFIG_PATH']
        global config
        config = configparser.ConfigParser()
        config.read(self.confFPath)

        ''' get the file and path for the logger '''
        self.logDir = os.path.join(self.rootDir,__logs_dir__)
        self.logFPath = os.path.join(self.logDir,__log_fname__)
        try:
            self.logDir = os.path.join(self.rootDir,config.get('LOGGING','LOGPATH'))
            self.logFPath = os.path.join(self.logDir,config.get('LOGGING','LOGFILE'))
        except:
            pass
        if not os.path.exists(self.logDir):
            os.makedirs(self.logDir)
#        self.logFPath = os.path.join(self.logDir,config.get('LOGGING','LOGFILE'))

        ''' innitialize the logger '''
        global logger
        logger = logging.getLogger(__package__)
        logger.setLevel(logging.DEBUG)
        if (logger.hasHandlers()):
            logger.handlers.clear()
        # create file handler which logs even debug messages
        fh = logging.FileHandler(self.logFPath, config.get('LOGGING','LOGMODE'))
#        fh = logging.FileHandler(self.logDir, config.get('LOGGING','LOGMODE'))
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        ''' set a new logger section '''
        logger.info('########################################################')
        logger.info(__name__)
        logger.info('Module Path = %s', self.moduleDir)

        ''' get the path to the input and output data '''
        self.dataDir = __data_dir__
        try:
            if "DATA_DIR" in kwargs.keys():
                ''' first preference given to kwargs '''
                self.dataDir = kwargs['DATA_DIR']
                logger.info("Appending data path from kwargs %s" % self.dataDir)
            else:
                ''' next try the app.cfg file '''
                self.dataDir = os.path.join(self.rootDir,config.get('STORES','DATA'))
                if not self.dataDir:
                    raise ValueError("Data location not defined in %s" % self.confFPath)
                else:
                    logger.info("Appending data path from configuration %s" % self.dataDir)
            
        except Exception as err:
            logger.warning("%s %s \n", _s_fn_id,err)
            logger.warning("Using default data path %s" % self.dataDir)

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
        print("Logging %s info, warnings, and error to %s" % (self.__package__, self.logFPath))
        print("Data path set to %s" % self.dataDir)
        return None


    ''' Function
            name: build_scrape_url_list
            parameters:
                inp_data_dir - path to the directory with property parameters JSON
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
    def get_destination_ids(self, file_name, destins_dir=None, col_name=None):

        _l_dests = []

        _s_fn_id = "function <get_scrape_output_params>"
#        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if not file_name:
                raise ValueError("Invalid file name")

            ''' see if the file exists '''
            if not destins_dir:
                destins_dir = self.dataDir
#             if inp_data_dir[-1] != "/":
#                 inp_data_dir +="/"
            file_path = os.path.join(destins_dir,file_name)
            if not os.path.exists(file_path):
                raise ValueError("File %s does not exisit in folder %s" %(file_name,destins_dir))

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
                inp_data_dir - path to the directory with property parameters JSON
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
    def build_scrape_url_list(self, file_name:str, inp_data_dir=None, **kwargs):

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
            if not inp_data_dir:
#                 inp_data_dir = os.path.join(self.rootDir,config.get('STORES','INPUTDATA'))
                inp_data_dir = self.dataDir
            logger.info("Directory path for loading input data %s" % inp_data_dir)
            
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
            _destins_dir = os.path.join(inp_data_dir, "destinations/")

            ''' retrieve OTA input and output property data from json '''
            property_dict = clsUtil.load_ota_list(os.path.join(inp_data_dir, file_name))

            if len(property_dict) <= 0:
                raise ValueError("No data found with %s with defined scrape properties"
                                 % os.path.join(inp_data_dir, file_name))
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

                    ''' get the list of destination ids for the particular OTA'''
                    if "destinations" in ota.keys():
                        
                        _l_dest = self.get_destination_ids(file_name=ota["destinations"],   # filename with destination ids
                                                           destins_dir=_destins_dir,   # path to the desitnation folder
                                                           col_name="destinationID"    # column name with destination ids
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
                inp_data_dir - string with folder path to the csv files
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
                
            _prop_search_folder = clsUtil.get_extract_data_stored_path(
                data_store_path = self.dataDir,
                parent_dir_name = parent_dir_name,
                **kwargs)
            logger.info("Folder %s ready for storing scraped data" % _prop_search_folder)

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

            procedure: specific to bookings.com scrape, extract, and load the data in a csf file

            author: <nileka.desilva@rezgateway.com>
    '''
    def _scrape_bookings_to_csv(self,
                                url,   # parameterized url
                                ota_name, # ota name
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
#            lists2 = soup.select(".c8305f6688")

            saveTo = path+"/"+file_name

            if len(lists) <= 0:
                raise ValueError("No data received for %s" % (url))

            for _list in lists:
                _data_err = False
                _data_dict = {}
                _data_dict['ota_name'] = ota_name,
                _data_dict['search_dt'] = search_dt,
                _data_dict['checkin_date'] = checkin_date,
                _data_dict['destination_id'] = destination_id,
                try:
                    _data_dict['property_name'] = _list.find('div', class_='fcab3ed991 a23c043802').text
                except Exception as text_err:
                    _data_dict['property_name'] = None
                    _data_err = True
                    logger.warning('property_name - %s',text_err)
                    pass
                try:
                    _data_dict['room_type'] = _list.find('span', class_='df597226dd').text
                except Exception as text_err:
                    _data_dict['room_type'] = None
                    _data_err = True
                    logger.warning('room_type - %s',text_err)
                    pass
                try:
                    _data_dict['room_rate'] = _list.find('span', class_='fcab3ed991 bd73d13072').text
                except Exception as text_err:
                    _data_dict['room_rate'] = None
                    _data_err = True
                    logger.warning('room_rate - %s',text_err)
                    pass
                try:
                    _data_dict['review_score'] = _list.find('div', class_='b5cd09854e d10a6220b4').text
                except Exception as text_err:
                    _data_dict['review_score'] = None
                    _data_err = True
                    logger.warning('review_score - %s',text_err)
                    pass
                try:
                    _data_dict['location_desc'] = _list.find('div', class_='a1fbd102d9').text
                except Exception as text_err:
                    _data_dict['location_desc'] = None
                    _data_err = True
                    logger.warning('location_desc - %s',text_err)
                    pass
                try:
                    _data_dict['other_info'] = _list.find('div', class_='d22a7c133b').text
                except Exception as text_err:
                    _data_dict['other_info'] = None
                    _data_err = True
                    logger.warning('other_info',text_err)

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

    def scrape_url_list(self,otaURLlist, searchDT: datetime, data_store_dir:str):

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
                    saveTo = self._scrape_bookings_to_csv(ota_dict['url'],   # constructed url with parameters
                                                          ota_dict['ota'],   # ota name data source
                                                          ota_dict['checkin'],  # booking intended checkin date
                                                          searchDT,   # date & time scraping was executed
                                                          ota_dict['destination_id'],  # destingation id to lookup the name
                                                          _fname,       # csv file name to store in
                                                          data_store_dir     # folder name to save the files
                                                         )
                    _l_saved_files.append(saveTo)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_saved_files


#     ''' Function DEPRECATED moved to otaUtils class
#             name: get_url_list
#             parameters:
#                 inp_data_dir - the relative or direct path to the file with urls
#                 file_name - the name of the file containing all the urls for scraping
#             procedure: read the list of urls from the CSV file and compile a list
#             return list (url_list)

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def load_ota_list(self, inp_data_dir:str, file_name:str):

#         import os         # apply directory read functions
#         import csv        # to read the csv
#         import json       # to read the json file

#         url_list = []     # initialize the return parameter
#         property_data = {}
        
#         _s_fn_id = "function <get_url_list>"
#         logger.info("Executing %s", _s_fn_id)

#         try:

#             ''' Get the list of urls from the CSV file '''        
#             if inp_data_dir:
#                 self.dataDir = inp_data_dir
#             _l_files = os.listdir(self.dataDir)
#             ''' if file_name is not null check if it is in the director '''
#             if file_name and file_name in _l_files:
#                 self.file = file_name
#             else:
#                 raise ValueError("Invalid file name %s in dir: %s. Does not exist!" % (file_name, self.dataDir))

#             ''' read the list of urls from the file '''
#             with open(self.dataDir+"/"+self.file, newline='') as f:
#                 property_data = json.load(f)

#         except Exception as err:
#             logger.error("%s %s \n", _s_fn_id,err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return property_data

#     ''' Function  DEPRECATED -- moved to otaUtils
#             name: get_scrape_input_params
#             parameters:
#                 url - string comprising the url with place holders
#                 **kwargs - contain the plance holder key value pairs

#             procedure: build the url by inserting the values from the **kwargs dict
#             return string (url)
            
#             author: <nuwan.waidyanatha@rezgateway.com>

#             TODO - change the ota_scrape_tags_df to a list of dictionaries
#     '''
#     def get_scrape_input_params(self, property_dict:dict):

#         _s_fn_id = "function <get_scrape_input_params>"
#         logger.info("Executing %s", _s_fn_id)

#         try:
#             ''' check for property dictionary '''
#             if not property_dict:
#                 raise ValueError("Invalid properties dictionary")

#             ''' loop through the dict to construct the scraper parameters '''
#             ota_param_list = []
#             _l_tag=[]
#             for prop_detail in property_dict:
#                 param_dict = {}
#                 tag_dict = {}
#                 ''' create a dict with input params '''
#                 param_dict['ota'] = prop_detail
#                 for detail in property_dict[prop_detail]:
#                     param_dict['url'] = detail['url']
#                     param_dict['inputs'] = detail['inputs']
#                     param_dict['destinations'] = detail['destinations']
#                     ''' append the input parameters into a list'''
#                     ota_param_list.append(param_dict)
      
#         except Exception as err:
#             logger.error("%s %s \n", _s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return ota_param_list #, ota_scrape_tags_df

#     ''' Function DEPRECTAED -- moved to otaUtils
#             name: get_scrape_output_params
#             parameters:
#                 property_dict - obtained from loading the property scraping parameters from the JSON

#             procedure: loop through the loaded dictionary to retrieve the output variable names, tags, and values.
#                         Then construcct and return a dataframe for all corresponding OTAs
#             return dataframe (_scrape_tags_df)

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def get_scrape_html_tags(self, property_dict:dict):

#         _scrape_tags_df = pd.DataFrame()

#         _s_fn_id = "function <get_scrape_output_params>"
#         logger.info("Executing %s", _s_fn_id)

#         try:
#             if not property_dict:
#                 raise ValueError("Invalid properties dictionary")

#             ''' loop through the dict to construct html tags to retrieve the data elements '''
#             for prop_detail in property_dict:
#                 for _prop_params in property_dict[prop_detail]:
#                     for _out_vars in _prop_params['outputs']:
#                         _out_vars['ota'] = prop_detail
#                         _scrape_tags_df = pd.concat([_scrape_tags_df,\
#                                                      pd.DataFrame([_out_vars.values()], columns=_out_vars.keys())],
#                                                    ignore_index=False)

#         except Exception as err:
#             logger.error("%s %s \n", _s_fn_id,err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return _scrape_tags_df


#     ''' Function  DEPRECATED -- moved to otaUtils
#             name: insert_params_in_url
#             parameters:
#                 url - string comprising the url with place holders
#                 **kwargs - contain the plance holder key value pairs

#             procedure: build the url by inserting the values from the **kwargs dict
#             return string (url)

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def insert_params_in_url(self, url: str, **kwargs ):

#         import re

#         url_w_params = None

#         _s_fn_id = "function <insert_params_in_url>"
# #        logger.info("Executing %s", _s_fn_id)

#         try:
#             if not url:
#                 raise ValueError("Invalid url string %s" % (url))
#             url_w_params = url

#             ''' match the keys in dict with the placeholder string in the url''' 
#             for key in kwargs.keys():
#                 _s_regex = r"{"+key+"}"
#                 urlRegex = re.compile(_s_regex, re.IGNORECASE)
#                 param = urlRegex.search(url_w_params)
#                 if param:
#                     _s_repl_val = str(kwargs[key]).replace(" ","%20")
#                     url_w_params = re.sub(_s_regex, _s_repl_val, url_w_params)
            
#         except Exception as err:
#             logger.error("%s %s \n", _s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return url_w_params

#     def _get_search_data_inp_data_dir(self,inp_data_dir, **kwargs):

#         _SearchDataDir = None
#         _search_dt = datetime.now()
        
#         _s_fn_id = "function <scrape_data_to_csv>"
#         logger.info("Executing %s", _s_fn_id)
        
#         kwargs = {
#             "PARENT_DIR": "rates",
#             "SCRAPE_TIME_GAP": 15,
#         }

#         try:
#             ''' establish the storage block '''
#             if inp_data_dir:
#                 self.ratesStoragePath = inp_data_dir
#             ''' add the folder if not exists '''
#             if self.ratesStoragePath[-1] != "/":
#                 self.ratesStoragePath +="/"
# #            if not os.path.exists(self.ratesStoragePath):
# #                os.makedirs(self.ratesStoragePath)

#             if "searchDateTime" in kwargs.keys():
#                 _search_dt = kwargs["searchDateTime"]

#             ''' TODO - change the function to round search datetime to nearest 30 min
#                 using _distinct_df['search_datetime']=_distinct_df['search_datetime'].dt.round('30min') '''
#             ''' pick the year, month, day, hour, min from search date time ''' 
#             _minute = _search_dt.minute
#             if _minute < 30:
#                 _minute = 0
#             else:
#                 _minute = 30

#             ''' folder is a concaternation of date hour and minute; where minute < 30 --> 0 and 30 otherwise'''
#             _SearchDataDir = self.ratesStoragePath+\
#                             str(_search_dt.year)+"-"+str(_search_dt.month)+"-"+str(_search_dt.day)\
#                             +"-"+str(_search_dt.hour)+"-"+str(_minute)+"/"     # csv file name

#             ''' add the folder if not exists '''
#             if kwargs['storageLocation'] == 'local':
#                 if not os.path.exists(self.ratesStoragePath):
#                     os.makedirs(self.ratesStoragePath)
#                 if not os.path.exists(_SearchDataDir):
#                     os.makedirs(_SearchDataDir)
#             elif kwargs['storageLocation'] == 'AWS_S3':
#                 print("todo")
#             else:
#                 raise ValueError("%s is an undefined storage location in **kwargs"
#                                  % (kwargs['storageLocation']))

#             logger.info("Extracting data into %s storage", kwargs['storageLocation'])
#             logger.info("OTA price data storage location: %s", _SearchDataDir)
#             logger.info("Search datetime set to: %s", str(_search_dt))

#         except Exception as err:
#             logger.error("%s %s \n", _s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return _SearchDataDir

#     ''' Function
#             name: scrape_ota_to_csv
#             parameters:
#                 url - string comprising the url with place holders
#                 **kwargs - contain the plance holder key value pairs

#             procedure: build the url by inserting the values from the **kwargs dict
#             return string (url)

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def scrape_data_to_csv(self,url,_scrape_tags_df,file_name, path):

#         from bs4 import BeautifulSoup # Import for Beautiful Soup
#         import requests # Import for requests
#         import lxml     # Import for lxml parser
#         import csv
#         from csv import writer

#         _s_fn_id = "function <scrape_data_to_csv>"
#         logger.info("Executing %s", _s_fn_id)

#         try:
#             if _scrape_tags_df.shape[0] <= 0:
#                 raise ValueError("Invalid scrape tags no data scraped")
#             if not file_name:
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
#     ''' Function  -- DEPRECATED -- moved to otaUtils
#             name: remove_empty_files
#             parameters:
#                 inp_data_dir - string with folder path to the csv files
#                 **kwargs - contain the plance holder key value pairs
#                             columns: list
#                             start_date: datetime.date
#                             end_date: datetime.date
#             procedure: reads the all the csv files in the entire folder and
#                         appends the data for the relevant columns defined in
#                         the dictionary into a dataframe
#             return dataframe (ota_bookings_df)

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''

#     def remove_empty_files(self,path):

#         _s_fn_id = "function <remove_empty_files>"
#         logger.info("Executing %s", _s_fn_id)

#         _l_removed_files = []
#         try:
#             if not path:
#                 path = self.dataDir
            
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

    
#     ''' Function DEPRACATE -- moved to utils/sparkWorkLoads
#             name: read_folder_csv_to_df
#             parameters:
#                 inp_data_dir - string with folder path to the csv files
#                 **kwargs - contain the plance holder key value pairs
#                             columns: list
#                             start_date: datetime.date
#                             end_date: datetime.date
#             procedure: reads the all the csv files in the entire folder and
#                         appends the data for the relevant columns defined in
#                         the dictionary into a dataframe
#             return dataframe (ota_bookings_df)

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''

#     def read_folder_csv_to_df(self,inp_data_dir: str, **kwargs):

#         ota_rates_df = pd.DataFrame()     # initialize the return var
#         _tmp_df = pd.DataFrame()
#         _start_dt = None
#         _end_dt = None
#         _l_cols = []

#         _s_fn_id = "function <read_folder_csv_to_df>"
#         logger.info("Executing %s", _s_fn_id)

#         try:
#             ''' check if the folder and files exists '''
#             if not inp_data_dir:
#                 raise ValueError("Invalid folder path %s" % inp_data_dir)
#             filelist = os.listdir(inp_data_dir)
#             if not (len(filelist) > 0):
#                 raise ValueError("No data files found in director: %s" % (inp_data_dir))

#             ''' extract data from **kwargs if exists '''
#             if 'columns' in kwargs.keys():
#                 self.scrape_columns = kwargs['columns']
#             if 'start_datetime' in kwargs.keys():
#                 _start_dt = kwargs['start_datetime']
#             if 'end_datetime' in kwargs.keys():
#                 _start_dt = kwargs['end_datetime']

#             '''' loop through files to get the data  '''
#             for _s_file in filelist:
#                 if _s_file.endswith(".csv"):
#                     _s_csv_file = inp_data_dir+_s_file
#                     _tmp_df = pd.read_csv(_s_csv_file, 
#                                           sep=",",
#                                           quotechar='"',
#                                           skip_blank_lines=True,
#                                          )
#                     ota_rates_df = pd.concat([ota_rates_df,_tmp_df])
#             ota_rates_df.columns = self.scrape_columns
#             ota_rates_df.reset_index(drop=True)

#         except Exception as err:
#             logger.error("%s %s \n", _s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return ota_rates_df