#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "propertyScraper"
__module__ = "ota"
__package__ = "scraper"
__app__ = "wrangler"
__ini_fname__ = "app.ini"
__bookings_data__ = "hospitality/bookings/"

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
            
            resources: to implement s3 logger - 
            https://medium.com/nerd-for-tech/simple-aws-s3-logging-in-python3-using-boto3-cfbd345ef65b
    '''
    def __init__(self, desc : str="data", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__desc__ = desc
        _s_fn_id = "__init__"

        global config
        global logger
        global clsUtil
        global clsNLP
        global clsSparkWL

        self.cwd=os.path.dirname(__file__)
        sys.path.insert(1,self.cwd)
        import scraperUtils as otasu

        config = configparser.ConfigParser()
        config.read(os.path.join(self.cwd,__ini_fname__))

        self.rezHome = config.get("CWDS","REZAWARE")
        sys.path.insert(1,self.rezHome)
        
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
        self.dataDir = os.path.join(self.storePath,__bookings_data__)

        ''' Set the wrangler root directory '''
        self.pckgDir = config.get("CWDS",self.__package__)
        self.appDir = config.get("CWDS",self.__app__)

        from rezaware import Logger as logs
        ''' innitialize the logger '''
        logger = logs.get_logger(
            cwd=self.rezHome,
            app=self.__app__, 
            module=self.__module__,
            package=self.__package__,
            ini_file=self.__ini_fname__)
        ''' set a new logger section '''
        logger.info('########################################################')
        logger.info(self.__name__,self.__package__)

        from utils.modules.etl.load import sparkwls as spark
        from utils.modules.ml.natlang import nlp
        ''' initialize util class to use common functions '''
        clsUtil = otasu.Utils(desc='Utilities class for property data scraping')
        clsNLP = nlp.NatLanWorkLoads(desc="classifying ota room types")
        clsSparkWL = spark.SparkWorkLoads(desc="ota property price scraper")

#         ''' innitialize the logger '''
#         logger = logs.get_logger(
#             cwd=self.rezHome,
#             app=self.__app__, 
#             module=self.__module__,
#             package=self.__package__,
#             ini_file=self.__ini_fname__)
#         ''' set a new logger section '''
#         logger.info('########################################################')
#         logger.info(self.__name__,self.__package__)

        ''' select the storate method '''
        self.storeMethod = "local"
        
        ''' set the tmp dir to store large data to share with other functions
            if self.tmpDIR = None then data is not stored, otherwise stored to
            given location; typically specified in app.conf
        '''
        self.tmpDIR = None
        if "WRITE_TO_FILE" in kwargs.keys():
            self.tmpDIR = os.path.join(self.dataDir,"tmp/")
            logger.debug("Set tmp file storage path to %s",self.tmpDIR)
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
        logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                     %(self.__app__,
                       self.__module__,
                       self.__package__,
                       self.__name__,
                       self.__desc__))
#         print("Initialing %s class for %s with instance %s" 
#               % (self.__package__, self.__name__, self.__desc__))
# #         print("Data path set to %s" % self.dataDir)
        return None


    ''' Function
            name: build_scrape_url_list
            parameters:
                inp_data_dir - path to the directory with property parameters JSON
                file_name - JSON file containing those parameters
                kwargs - 
                        

            procedure: use the get_scrape_input_params function to load the the JSON file. 
                        Thenloop through all the OTAs to extract the input parameters
                        For each OTA template url use the insert_params_in_url function to
                        construct the list of parameterized URLs
            return: list with the set of urls (scrape_url_list)

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-27

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def get_destination_ids(self, file_name:str, destins_dir=None, col_name="destinationID", **kwargs):

        _l_dests = []

        _s_fn_id = "function <get_scrape_output_params>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if not file_name:
                raise ValueError("Invalid file name")

            ''' see if the file exists '''
            if not destins_dir:
                destins_dir = self.dataDir
            file_path = os.path.join(destins_dir,file_name)
            if not os.path.exists(file_path):
                raise ValueError("File %s does not exisit in folder %s" %(file_name,destins_dir))

            ''' read the destination ids into the list '''
            dest_df = pd.read_csv(file_path, sep=',', quotechar='"')
            if dest_df.shape[0] <=0:
                raise ValueError("No destination data recovered from %s" %(file_path))

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
            modified: 2022-09-27

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
                inp_data_dir = self.dataDir
            logger.debug("Directory path for loading input data %s" % inp_data_dir)
            
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
                    if "locations" in ota.keys():
                        
                        _l_dest = self.get_destination_ids(file_name=ota["locations"],   # filename with destination ids
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
                    print(len(_ota_parameterized_url_list))
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
            modified: 2022-09-22
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

            author(s): <nileka.desilva@rezgateway.com>
                        <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-10
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
            # check the response code
            if response.status_code != 200:
                raise RuntimeError("Invalid response with code %d" % response.status_code)

            # Make it a soup
            soup = BeautifulSoup(response.text,"lxml")

            lists = soup.select(".d20f4628d0")
#            lists2 = soup.select(".c8305f6688")

            saveTo = os.path.join(path, file_name)

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
                otasuRLlist - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-20
    '''

    def scrape_url_list(self,otasuRLlist, searchDT: datetime, data_store_dir:str):

        saveTo = None   # init file name
        _l_saved_files = []

        _s_fn_id = "function <scrape_url_list>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if len(otasuRLlist) > 0:
                logger.info("loading parameterized urls from list %d records", len(otasuRLlist))
                print("loading parameterized urls from list %d records" % len(otasuRLlist))
            else:
                raise ValueError("List of URLs required to proceed; non defined in list.")

            ''' loop through the list of urls to scrape and save the data'''
            for ota_dict in otasuRLlist:

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

    ''' Function
            name: extract_room_rate
            parameters:
                otasuRLlist - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-29
    '''

    def extract_room_rate(self, 
                          room_data_df,   # data frame with the room rates
                          rate_col_name:str="room_rate",   # column name with the room rates
                          aug_col_name:str="room_price",   # column name to save the room category
                          curr_col_name:str="currency",   # column to store the currency abbr
                          **kwargs,       # kwargs = {}
                         ):
        
        _s_fn_id = "function <extract_room_rate>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        aug_rate_df = pd.DataFrame()

        try:
            if room_data_df.shape[0] <= 0:
                raise ValueError("Invalid dataframe with %d rows" % room_data_df.shape[0])
            aug_rate_df = room_data_df.copy()

            price_regex = r'(?i)(?:(\d+,*\d+))'
            ''' TODO extract currency abbreviation from the room_rate '''
            aug_rate_df[curr_col_name] = 'US$'
            aug_rate_df[aug_col_name] = aug_rate_df[rate_col_name].str.extract(price_regex, expand=False)
            aug_rate_df[aug_col_name] = aug_rate_df[aug_col_name].str.replace(',','')
            aug_rate_df[aug_col_name] = aug_rate_df[aug_col_name].astype('float64')
            logger.debug("room price extracted from %s and stored in %s with currency abbreviation" 
                         %(rate_col_name,aug_col_name))

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return aug_rate_df

    ''' Function
            name: merge_similar_room_cate
            parameters:
                otasuRLlist - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-29
    '''

    def merge_similar_room_cate(self, room_data_df, col_name, **kwargs):
        
        _s_fn_id = "function <merge_similar_room_cate>"
        logger.info("Executing %s %s",self.__package__, _s_fn_id)

        emb_kwargs = {
            "LOWER":True,
            "NO_STOP_WORDS":False,
            "METRIC":'COSIN',
            "MAX_SCORES":2,
        }
#         aug_rtype_df = pd.DataFrame()
        sim_scores_df = pd.DataFrame()    # to get the similarity with maximum score
        _assignment_df = pd.DataFrame()   # concat the simiarity mappings
        _merged_room_cate_df = pd.DataFrame()   # merge the similarity with main dataset

        try:
            if room_data_df.shape[0] <= 0:
                raise ValueError("Invalid dataframe with %d rows" % room_data_df.shape[0])
            logger.debug("loaded %d rows property price search data", room_data_df.shape[0])
            
            ''' set the cut-off score for selecting best room type matching '''
            if 'TOLERANCE' in kwargs.keys():
                _tolerance = kwargs['TOLERANCE']
            else:
                _tolerance = 0.05
            if 'ROOM_CATE_FNAME' in kwargs.keys():
                _room_cate_file = kwargs['ROOM_CATE_FNAME']
            else:
                _room_cate_file="room_descriptions.csv"
            
            ''' get the catergorized room type data '''
            _room_taxo = pd.read_csv(os.path.join(self.dataDir,_room_cate_file), delimiter=',')
            logger.debug("loaded %d room descriptors", _room_taxo.shape[0])
            ''' iterate through each room type to get the similarity scores against the room type taxonomy '''
            for rowIdx, rowVal in room_data_df.iterrows():
                ''' get the similarity scores from nlp utils calss'''
                sim_scores_df = clsNLP.get_similarity_scores(
                    input_sentences = [rowVal.room_type],   # cannot use list() it will split into letters
                    lookup_sentences =list(_room_taxo.description),
                    **emb_kwargs)
                sim_scores_df=sim_scores_df.reset_index()
                sim_scores_df.score = sim_scores_df.score.astype('float64')
                sim_scores_idxmax = sim_scores_df.loc[sim_scores_df.score.idxmax()]

                if sim_scores_idxmax.score > _tolerance:
                    _guess_dict = {'room_type':sim_scores_idxmax.input_sentence,
                                   'room_cate':sim_scores_idxmax['lookup sentence'],
                                   'similarity':sim_scores_idxmax.score,
                                  }
                    logger.debug("%s matches %s with similarity score %s", 
                                 sim_scores_df.input_sentence,
                                 sim_scores_idxmax['lookup sentence'],
                                 str(sim_scores_df.score))
                else:
                    _guess_dict = {'room_type':sim_scores_idxmax.room_type,
                                   'room_cate':"UNKNOWN",
                                   'similarity' : sim_scores_idxmax.score,
                                  }
                    logger.warning("%s with similarity score %d < tolerated cut-off %d, assigining UNKNOWN",
                                   rowVal.room_type, sim_scores_idxmax.score, _tolerance)
                _assignment_df = pd.concat([_assignment_df,pd.DataFrame([_guess_dict])])
                
            if _assignment_df.shape[0] > 0:
                _merged_room_cate_df = room_data_df.merge(_assignment_df,
                                                          how='left', 
                                                          left_on=['room_type'], 
                                                          right_on=['room_type'])
                logger.info(("Merged %d rows with categorized room type information"
                             % _assignment_df.shape[0]))
            else:
                raise ValueError("No similarity room categories assigned")

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _merged_room_cate_df

    ''' Function
            name: assign_lx_name
            parameters:
                otasuRLlist - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-10-03
            
    '''

    def assign_lx_name(self, data_df,   # pandas dataframe with room price data
                       dest_dir_name:str="destinations/",   # set to default in folder
                       **kwargs):

        import glob

        _s_fn_id = "function <assign_lx_name>"
        logger.info("Executing %s %s",self.__package__, _s_fn_id)

        _lx_assign_df = pd.DataFrame()
        _dest_df = pd.DataFrame()

        try:
            ''' check if dataframe is null '''
            if data_df.shape[0] <= 0:
                raise ValueError("Invalid dataframe with % rows" % data_df.shape[0])

            ''' set the directory path to csv files with destination ids '''
            _destins_dir = os.path.join(self.dataDir, dest_dir_name)
#             if dest_dir_name:
#                 _destins_dir = os.path.join(self.dataDir, dest_dir_name)
            ''' read all the destination files in dir '''   
            if not os.path.exists(_destins_dir):
                raise ValueError("Folder %s does not exisit" %(_destins_dir))

            ''' read the destination ids into the list '''
            all_files = glob.glob(os.path.join(_destins_dir,"*.csv"))
#             dest_df = pd.read_csv(file_path, sep=',', quotechar='"')
            _dest_df = pd.concat((pd.read_csv(_fname, sep=',', quotechar='"') for _fname in all_files))
            if _dest_df.shape[0] <=0:
                raise ValueError("No destination data recovered from %s" %(file_path))
            _lx_assign_df = data_df.merge(_dest_df,
                                          how='left',
                                          left_on=['destination_id'], 
                                          right_on=['destinationID'])
            logger.info(("Merged %d rows with destination information"
                         % _lx_assign_df.shape[0]))
            ''' drop columns state and destinationID '''
            ''' TODO pass all the column names as **kwargs '''
            _lx_assign_df.drop(['state','destinationID'],axis=1,inplace=True)
            _lx_assign_df.rename(columns={'city':"dest_lx_name"},inplace=True)
            _lx_assign_df["dest_lx_type"] = "city"

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _lx_assign_df


    ''' Function
            name: save_to_db
            parameters:
                otasuRLlist - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-10-03
            
       TODO - build the function and use it in the notebook
    '''

    def save_to_db(self, data_df, table_name:str="ota_property_prices", **kwargs):
        
        _s_fn_id = "function <merge_similar_room_cate>"
        logger.info("Executing %s %s",self.__package__, _s_fn_id)

        count = 0

        try:
            if data_df.shape[0] <= 0:
                raise ValueError("Invalid dataframe with % rows" % data_df.shape[0])

            if "data_source" in data_df.columns:
                data_df.loc[data_df["data_source"].isnull(),'data_source'] = data_df.ota_name
            if "data_owner" in data_df.columns:
                data_df.loc[data_df["data_owner"].isnull(),'data_owner'] = data_df.ota_name
            data_df["created_dt"] = date.today()
            data_df["created_by"] = os.getlogin()             
            data_df["created_proc"] = self.__package__

            ''' save dataframe with spark '''
            count = clsSparkWL.insert_sdf_into_table(save_sdf=data_df, dbTable=table_name)
            # print("%d rows saved to %s" % (aug_search_sdf.count(), _tmp_fname))
            logger.info("Data %d rows loaded into %s table",count,table_name)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return count, data_df
