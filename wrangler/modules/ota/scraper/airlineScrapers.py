#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "airlineScrapers"
__module__ = "ota"
__package__ = "scraper"
__app__ = "wrangler"
__ini_fname__ = "app.ini"
__airline_data__ = "transport/airlines/"
__inputs_file_name__ = "ota_input_urls.json"

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
    
#     sys.path.insert(1,__module_dir__)
#     import otaUtils as otau

    print("All {0} software packages loaded successfully!".format(__package__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

'''
    CLASS spefic to scraping airline data from Online Travel Ageny (OTA) web sites; e.g. kayak.com, momondo.com,
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
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__desc__ = desc
        _s_fn_id = "__init__"

        global config
        global logger
        global clsUtil
#         global clsNLP
        global clsSparkWL

        self.cwd=os.path.dirname(__file__)
        sys.path.insert(1,self.cwd)
        import scraperUtils as otasu
        clsUtil = otasu.Utils(desc='Inheriting Utilities class for airline itinerary scraping')

        config = configparser.ConfigParser()
        config.read(os.path.join(self.cwd,__ini_fname__))

        self.rezHome = config.get("CWDS","REZAWARE")
        sys.path.insert(1,self.rezHome)
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
        clsSparkWL = spark.SparkWorkLoads(desc="ota property price scraper")

        ''' Set the wrangler root directory '''
        self.pckgDir = config.get("CWDS",self.__package__)
        self.appDir = config.get("CWDS",self.__app__)
        ''' get the path to the input and output data '''
        self.dataDir = os.path.join(config.get("CWDS","DATA"),__airline_data__)
        ''' get the path to the input and output data '''
        ''' select the storate method '''
        self.storeMethod = "local"
        
        self.inpParamsFile = __inputs_file_name__
        
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
        self.return_flight_offset = 1

        logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                     %(self.__app__,
                       self.__module__,
                       self.__package__,
                       self.__name__,
                       self.__desc__))
        print("Initialing %s class for %s with instance %s" 
              % (self.__package__, self.__name__, self.__desc__))
        print("Data path set to %s" % self.dataDir)
        return None


    ''' Function
            name: get_flight_segments
            parameters:
                dir_path - path to the directory with property parameters JSON
                file_name - JSON file containing those parameters
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
    def get_flight_segments(self, file_name:str, segments_dir=None, **kwargs):

        _l_segments = []

        _s_fn_id = "function <get_flight_segments>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            if not file_name:
                raise ValueError("Invalid file name")

            ''' see if the file exists '''
            if not segments_dir:
                segments_dir = self.dataDir
#             if inp_data_dir[-1] != "/":
#                 inp_data_dir +="/"
            file_path = os.path.join(segments_dir,file_name)
            if not os.path.exists(file_path):
                raise ValueError("File %s does not exisit in folder %s" %(file_name,segments_dir))

#             if not destins_dir:
#                 destins_dir = self.dataDir

#             if not os.path.exists(file_path):
#                 raise ValueError("File %s does not exisit" %(file_path))

#             ''' see if the file exists '''
#            if not dir_path:
#                dir_path = self.path
#             if dir_path[-1] != "/":
#                 dir_path +="/"
#            file_path = os.path.join(dir_path,file_name)

            ''' read the destination ids into the list '''
            airports_df = pd.read_csv(file_path, sep=',', quotechar='"')
            if airports_df.shape[0] <=0:
                raise ValueError("No airport segments data recovered from %s" %(file_path))
            ''' set the airport column name '''
            if "codeColName" in kwargs.keys():
                col_name = kwargs["codeColName"]
            else:
                col_name = "airportCode"
            ''' if given in kwargs set depart and arrive columns from list '''
            if "departAirportCode" in kwargs.keys():
                _l_depart_codes = kwargs['departAirportCode']
            else:
                _l_depart_codes = list(airports_df[col_name])
            if "arriveAirportCode" in kwargs.keys():
                _l_arrive_codes = kwargs['arriveAirportCode']
            else:
                _l_arrive_codes = list(airports_df[col_name])
            print(_l_depart_codes,_l_arrive_codes)
            ''' create a tuples of depart arrive combinations from the list '''
            for departure in _l_depart_codes:
                for arrival in _l_arrive_codes:
                    if departure != arrival:
                        _l_segments.append(tuple([departure,arrival]))

            if len(_l_segments) > 0:
                logger.info("Create %d departure and arrival sector tuples" % len(_l_segments))
            else:
                raise ValueError("No destination and arrival tuples created!")

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id,err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_segments


    ''' Function
            name: build_scrape_url_info
            parameters:
                dir_path - path to the directory with property parameters JSON
                file_name - JSON file containing those parameters

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
    def build_scrape_url_info(self, file_name:str, inp_data_dir=None, **kwargs):

        _scrape_url_dict = {}
        _ota_parameterized_url_info = [] 

        err_count = 0      # logging the number of errors
        _tmpFPath = None   # tempory file path to store the parameterized URL list
        
        _s_fn_id = "function <build_scrape_url_info>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
#             if not file_name:
#                 raise ValueError("Invalid file to fetch scraper property dictionary")
#             if not dir_path:
#                 dir_path = os.path.join(self.rootDir,config.get('STORES','INPUTDATA'))
            ''' validate and if given the dir path, else revert to defaul '''
            if not file_name:
                raise ValueError("Invalid file to fetch scraper property dictionary")
            if not inp_data_dir:
                inp_data_dir = self.dataDir
            logger.info("Directory path for loading input data %s" % inp_data_dir)
            
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

            ''' set the directory path to csv files with destination ids '''
            _segments_dir = os.path.join(inp_data_dir, "segments/")

            ''' retrieve OTA input and output property data from json '''
#            airline_dict = self.load_ota_info(os.path.join(dir_path, file_name))
            airline_dict = clsUtil.load_ota_list(os.path.join(inp_data_dir, file_name))
            if len(airline_dict) <= 0:
                raise ValueError("No data found with %s with defined scrape airline"
                                 % os.path.join(inp_data_dir, file_name))
            else:
                logger.info("Loaded %d airline agencies to begin scraping OTA data.", len(airline_dict))
                print("Loaded %d airline agencies to begin scraping OTA data." % (len(airline_dict)))

#             ''' set the directory path to csv files with airport codes '''
#             if "airportsFile" in kwargs.keys():
#                 airports_file_path = os.path.join(dir_path,kwargs["airportsFile"])
#                 logger.info("Airports code file path set to %s" % airports_file_path)
#             else:
#                 raise ValueError("No csv file with airport codes defined in **kwargs")
            
            ''' get the input parameters from the properties file '''
            _ota_input_param_info = clsUtil.get_scrape_input_params(airline_dict)
            logger.info("Input parameter list loaded successfully.")
            
#             ''' get the tuples of departure and arrival airport code combinations '''
#             _l_flight_segments = self.get_flight_segments(file_path=airports_file_path, **kwargs)
#             logger.info("Airport codes with %d combinations loaded successfully." % len(_l_flight_segments))

            ''' loop through the  ota list to create the url list for each ota '''
            for ota in _ota_input_param_info:
                logger.info("Processing %s ...", ota['ota'])
                print("Processing %s ..." % (ota['ota']))
                _inert_param_dict = {}
                try:
                    _ota_url = None
                    if not ota['url']:
                        raise ValueError("Invalid url skip to next")

                    ''' get the list of destination ids for the particular OTA'''
                    if "locations" in ota.keys():
                        _l_flight_segments = self.get_flight_segments(
                            file_name=ota["locations"],    # filename with destination ids
                            segments_dir=_segments_dir,   # path to the desitnation folder
                            **kwargs,
#                            col_name="airportCode"    # column name with destination ids
                        )

                        if len(_l_flight_segments) <= 0:
                            raise ValueError("No flight segments received for %s" % ota['ota'])

                    ''' build the dictionary to replace the values in the url place holder '''
                    for _flight_sector in _l_flight_segments:
                        _inert_param_dict['departurePort'] = _flight_sector[0]
                        _inert_param_dict['arrivalPort'] = _flight_sector[1]

                        ''' TODO: append additional attributes to _inert_param_dict instead of a new _scrape_url_dict '''
                        for day_count in range(0,(self.scrape_end_date - self.scrape_start_date).days):
                            _inert_param_dict['flightDate'] = self.scrape_start_date + timedelta(days=day_count)
                            _parameterized_url = None
                            _parameterized_url = clsUtil.insert_params_in_url(ota['url'],**_inert_param_dict)
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
                parent_dir_name = "itinerary/"
                
            _prop_search_folder = clsUtil.get_extract_data_stored_path(
                data_store_path = self.dataDir,
                parent_dir_name = parent_dir_name,
                **kwargs)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _prop_search_folder


    ''' Function
            name: _scrape_kayak_to_csv
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)

            author(s): <nileka.desilva@rezgateway.com>
                        <nuwan.waidyanatha@rezgateway.com>
    '''
    def _scrape_kayak_to_csv(self,
                                url,   # parameterized url
                                ota_name,    # ota name
                                flight_date, # intended flight date
                                search_dt,    # scrape run date time
                                departure_port,
                                arrival_port,
                                file_name,     # store in csv file
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
            logger.debug("response status code: %d" % response.status_code)
            # check the response code
            if response.status_code != 200:
                raise RuntimeError("Invalid response with code %d" % response.status_code)

            # Make it a soup
            soup = BeautifulSoup(response.text,"lxml")
            _find_infos = soup.select(".resultWrapper")

            logger.debug("Number of results received from the response: %d" % len(_find_infos))
            if len(_find_infos) <= 0:
                raise ValueError("No data received for %s" % (url))

            ''' name of the csv file to save scraped data to '''
            saveTo = os.path.join(path, file_name)

            ''' extrac strings and cleanup the \n with spaces '''
            for _info in _find_infos:
                _data_dict = {}
                _data_dict['ota_name'] = ota_name,
                _data_dict['search_dt'] = search_dt,
                _data_dict['depart_port_code'] = departure_port,
                _data_dict['arrive_port_code'] = arrival_port,
                ''' departure and arrival times '''
                try:
                    _flight_times = _info.find('div', class_='section times').text,
                    _data_dict['flight_times'] = re.sub('\n+', ' ', (_flight_times[0]).lstrip().rstrip())
                except Exception as text_err:
                    _data_dict['flight_times'] = None
                    _data_err = True
                    logger.warning('flight_times - %s',text_err)
                    pass
                ''' number of stops '''
                try:
                    num_stops = _info.find('div', class_='section stops').text,
                    _data_dict['num_stops'] = re.sub('\n+', ' ', (num_stops[0]).lstrip().rstrip())
                except Exception as text_err:
                    _data_dict['num_stops'] = None
                    _data_err = True
                    logger.warning('num_stops - %s',text_err)
                    pass
                ''' flight travel time '''
                try:
                    _duration = _info.find('div', class_='section duration allow-multi-modal-icons').text,
                    _data_dict['duration'] = re.sub('\n+', ' ', (_duration[0]).lstrip().rstrip())
                except Exception as text_err:
                    _data_dict['duration'] = None
                    _data_err = True
                    logger.warning('duration - %s',text_err)
                    pass
                ''' trip price '''
                try:
                    _unit_rate = _info.find('span', class_='price-text').text,
                    _data_dict['booking_rate'] = re.sub('\n+', ' ', (_unit_rate[0]).lstrip().rstrip())
                    _data_dict['num_passengers'] = 1
                except Exception as text_err:
                    _data_dict['booking_rate'] = None
                    _data_err = True
                    logger.warning('booking_rate - %s',text_err)
                    pass
                ''' cabin type '''
                try:
                    _flight_type = _info.find('div', class_='above-button').text,
                    _data_dict['cabin_type'] = re.sub('\n+', ' ', (_flight_type[0]).lstrip().rstrip())
                except Exception as text_err:
                    _data_dict['cabin_type'] = None
                    _data_err = True
                    logger.warning('cabin_type - %s',text_err)
                    pass
                ''' airline name '''
                try:
                    _airline = _info.find('span', class_='name-only-text').text,
                    _data_dict['airline_name'] = re.sub('\n+', ' ', (_airline[0]).lstrip().rstrip())
                except Exception as text_err:
                    _data_dict['airline_name'] = None
                    _data_err = True
                    logger.warning('airline_name - %s',text_err)
                    pass

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
                price_regex = r'(?i)(?:(\d+,*\d+))'
                _save_df['booking_price'] = _save_df['booking_rate'].str.extract(price_regex, expand=False)
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

    def scrape_url_info(self,otaURLlist, searchDT, data_store_dir:str):

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
                        ota_name=ota_dict['ota'],   # ota name data source
                        flight_date=ota_dict['flightDate'],  # intended departure date
                        search_dt=searchDT,    # date & time scraping was executed
                        departure_port=ota_dict['departurePort'],  # departure airport code
                        arrival_port=ota_dict['arrivalPort'],      # arrival airport code
                        file_name=_fname,        # csv file name to store in
                        path=data_store_dir,           # folder name to save the files
                                                         )
                    _l_saved_files.append(saveTo)

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_saved_files


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
                       segments_dir_name:str="segments/",   # set to default in folder
                       **kwargs):

        import glob

        _s_fn_id = "function <assign_lx_name>"
        logger.info("Executing %s %s",self.__package__, _s_fn_id)

        _lx_assign_dep_df = pd.DataFrame()
        _lx_assign_arr_df = pd.DataFrame()
        _segments_df = pd.DataFrame()

        try:
            ''' check if dataframe is null '''
            if data_df.shape[0] <= 0:
                raise ValueError("Invalid dataframe with % rows" % data_df.shape[0])

            ''' set the directory path to csv files with destination ids '''
            _segments_dir = os.path.join(self.dataDir, "segments/")
#             if dest_dir_name:
#                 _destins_dir = os.path.join(self.dataDir, dest_dir_name)
            ''' read all the destination files in dir '''   
#             file_path = os.path.join(destins_dir,file_name)
            if not os.path.isdir(_segments_dir):
                raise ValueError("Folder %s does not exisit" %(_destins_dir))

            ''' read the destination ids into the list '''
            all_files = glob.glob(os.path.join(_segments_dir,"*.csv"))
#             dest_df = pd.read_csv(file_path, sep=',', quotechar='"')
            _segments_df = pd.concat(
                (pd.read_csv(_fname,
                             sep=',',
                             quotechar='"')
                 for _fname in all_files))
            if _segments_df.shape[0] <=0:
                raise ValueError("No destination data recovered from %s" %(file_path))
            '''assign departure port names '''
            _lx_assign_dep_df = data_df.merge(_segments_df,
                                              how='left',
                                              left_on=['depart_port_code'],
                                              right_on=['airportCode'],
                                              suffixes=None)
            _lx_assign_dep_df.rename(columns={'city':"depart_port_name"},inplace=True)
            _lx_assign_dep_df["depart_lx_type"] = "city"
            logger.info(("Merged %d rows with departure port data"
                         % _lx_assign_dep_df.shape[0]))

            '''assign arrival port names '''
            _lx_assign_arr_df = _lx_assign_dep_df.merge(_segments_df,
                                                        how='left',
                                                        left_on=['arrive_port_code'],
                                                        right_on=['airportCode'],
                                                        suffixes=('_x','_y'))
            _lx_assign_arr_df.rename(columns={'city':"arrive_port_name"},inplace=True)
            _lx_assign_arr_df["arrive_lx_type"] = "city"
#             _lx_assign_arr_df.drop(columns=['state','airportCode'])
            logger.info(("Merged %d rows with arrival port data"
                         % _lx_assign_arr_df.shape[0]))
            _lx_assign_arr_df.drop(
                columns=['state_x','airportCode_x','state_y','airportCode_y'],
                inplace=True)
           
            ''' TODO pass all the column names as **kwargs '''
#             _lx_assign_df.drop(['state','destinationID'],axis=1,inplace=True)
#             _lx_assign_arrive_df.rename(columns={'city':"dest_lx_name"},inplace=True)
#             _lx_assign_arrive_df["dest_lx_type"] = "city"

        except Exception as err:
            logger.error("%s %s \n", _s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _lx_assign_arr_df


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

    def save_to_db(self, data_df, table_name:str="ota_airline_routes", **kwargs):
        
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
