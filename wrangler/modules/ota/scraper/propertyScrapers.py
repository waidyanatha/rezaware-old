#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "propertyScraper"
__module__ = "ota"
__package__ = "scraper"
__app__ = "wrangler"
__ini_fname__ = "app.ini"
__booking_data_ext__ = "hospitality/bookings/"
__booking_destin_ext__="destinations/"
__tmp_data_dir_ext__ = "tmp/"

''' Load necessary and sufficient python libraries that are used throughout the class'''
try:
    ''' standard python packages '''
    import os
    import sys
    import logging
    import traceback
    import configparser
    import pandas as pd
    from datetime import datetime, date, timedelta, timezone

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
        if desc is None:
            self.__desc__ = " ".join([self.__app__,
                                      self.__module__,
                                      self.__package__,
                                      self.__name__])
        else:
            self.__desc__ = desc
        __s_fn_id__ = "__init__"

        global config
        global logger
        global clsUtil
        global clsNLP
        global clsFile

        self._data = None    # property to hold class datasets
        self._dataDir=None   # property sets the scraped data storage relative path
        self._destDir=None   # property sets the destination directory path relative to _dataDir
        self._tmpDir =None   # property sets the temporary scraped data storate relative pather
        self._searchTimestamp=None # datetime the booking search was made; i.e. scraping started
        self._checkInDate= None  # Starting date of the scrape dataset timestamp
        self._checkOutDate=None  # Ending date of the scrape dataset timestamp
        self._numOfNights= None  # Number of days to offset the checkout date
        self._pageUpLimit= None  # Upper limit of the number of web pages to scrape
        self._pageOffset = None  # Number of pages to retrieve for each request

        self.cwd=os.path.dirname(__file__)
        sys.path.insert(1,self.cwd)
        import scraperUtils as otasu

        config = configparser.ConfigParser()
        config.read(os.path.join(self.cwd,__ini_fname__))

        self.rezHome = config.get("CWDS","REZAWARE")
        sys.path.insert(1,self.rezHome)
        
        from utils.modules.etl.load import sparkFILEwls as spark
#         from utils.modules.etl.load import sparkwls as spark
        from utils.modules.ml.natlang import nlp
        ''' initialize util class to use common functions '''
        clsUtil = otasu.Utils(desc=self.__desc__)
        clsNLP = nlp.NatLanWorkLoads(desc=self.__desc__)
        clsFile = spark.FileWorkLoads(desc=self.__desc__)
        clsFile.storeMode=config.get("DATASTORE","MODE")
        clsFile.storeRoot=config.get("DATASTORE","ROOT")

#         self.storePath = os.path.join(
# #             self.cwd,
#             self.__app__,
#             "data/",
#             self.__module__,
#             self.__package__
#         )
                
        ''' set package specific path to the input and output data '''
#         self.dataDir = os.path.join(config.get("CWDS","DATA"),__booking_data_ext__)
#         self.dataDir = os.path.join(self.storePath,__booking_data_ext__)

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
        logger.info("%s %s",self.__name__,self.__package__)

        ''' select the storage method '''
        self.storeMethod = "local"
        
        ''' set the tmp dir to store large data to share with other functions
            if self.tmpDIR = None then data is not stored, otherwise stored to
            given location; typically specified in app.conf
        '''
#         self._tmpDIR = None
#         if "WRITETOTMP" in kwargs.keys():
#             self.tmpDIR = os.path.join(self.dataDir,"tmp/")
#             logger.debug("Set tmp file storage path to %s",self.tmpDIR)
#             if not os.path.exists(self.tmpDIR):
#                 os.makedirs(self.tmpDIR)

#         self.scrape_start_date = date.today()
#         self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
#         self.page_offset = 10
#         self.page_upper_limit = 150
#         self.checkout_offset = 1

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

    ''' Function --- CLASS PROPERTY SETTER/GETTER ---
            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    ''' Function -- DATA --
            TODO: 
            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):
        """ @propert data function

            gets the class instance set data such as the parameterized
            url lists and the data collected from scraping the urls.

            return self._data (any)
        """

        __s_fn_id__ = "function <@property data>"

        try:
            if self._data is None:
                raise AttributeError("%s property data cannot be None-type to use in class")
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):
        """ @data.setter function

            sets the class instance set data such as the parameterized
            url lists and the data collected from scraping the urls.

            return self._data (any)
        """

        __s_fn_id__ = "function <@data.setter>"

        try:
            if data is None:
                raise AttributeError("Cannot set an empty dataset")
            self._data = data
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    ''' --- DATA DIR --- '''
    @property
    def dataDir(self) -> str:
        """
        Description:
            Class property retuns the relative directory path for storing scraped. 
        Attributes:
            None
        Returns:
            self._dataDir (str)
        Exceptions:
            If the property value is None then will attempt to create a directory
            with the self.__app__, self.__module__, self.__package__, & __booking_data_ext__
            WARNING - do not create the absolute path because sparkFILEwls does that
        """

        __s_fn_id__ = "function <@property dataDir>"

        try:
            if self._dataDir is None:
                self._dataDir = os.path.join(self.__app__,'data',self.__module__,
                                             self.__package__,__booking_data_ext__)
                logger.warning("%s NoneType dataDIR set to default value %s",
                               __s_fn_id__,self._dataDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dataDir

    @dataDir.setter
    def dataDir(self,data_dir_path:str="")->str:
        """
        Description:
            Class property is set with the dataDir path for storing the scrapped data.
        Attributes:
            data_dir_path (str) - a valid os.path
        Return:
            self._dataDir
        Exception:
            Invalid string with improper director path formats will be rejected
        """

        __s_fn_id__ = "function <@dataDir.setter>"

        try:
            if data_dir_path is None or "".join(data_dir_path.split())=="":
                raise AttributeError("Invalid data dir path")
            self._dataDir = data_dir_path
            logger.debug("%s dataDir relative path set to %s",
                         __s_fn_id__,self._dataDir)
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dataDir

    ''' --- DESTINATIONS DIR --- '''
    @property
    def destDir(self) -> str:
        """
        Description:
            Class property retuns the relative directory path for the destinations directory
             that holds the data files with destination codes.
        Attributes:
            None
        Returns:
            self._destDir (str)
        Exceptions:
            If self._destDir is None, it will set the default value by joining 
            __booking_destin_ext__ to self._dataDir.
            WARNING - do not create the absolute path because sparkFILEwls does that
        """

        __s_fn_id__ = "function <@property destDir>"

        try:
            if self._destDir is None:
                self._destDir = os.path.join(self.dataDir,__booking_destin_ext__)
                logger.warning("%s NoneType destDir set to default value %s",
                               __s_fn_id__,self._destDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._destDir

    @destDir.setter
    def destDir(self,destination_dir:str="")->str:
        """
        Description:
            Class property augments the directory path to the self.dataDir path to
            create a temporary storage of scraped data from each functional step.
            This is mainly to support data sharing between airflow process functions
        Attributes:
            tmp_dir_path (str) - a valid os.path
        Return:
            self._tmpDir
        Exception:
            Invalid string with improper director path formats will be rejected
        """

        __s_fn_id__ = "function <@destDir.setter>"

        try:
            if destination_dir is None or "".join(destination_dir.split())=="":
                raise AttributeError("Invalid destination directory name")
            self._destDir = destination_dir
            logger.debug("%s destDir relative path set to %s",
                         __s_fn_id__,self._destDir)
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._destDir

    ''' --- TEMP DIR --- '''
    @property
    def tmpDir(self) -> str:
        """
        Description:
            Class property retuns the relative directory path for temporary storage of
             data generated from the scraper functions. This is mainly to support data
            sharing between airflow process functions
        Attributes:
            None
        Returns:
            self._tmpDir (str)
        Exceptions:
            If the property value is None, then create detfault directory by joining
            self._dataDir with __tmp_data_dir_ext__.
            WARNING - do not create the path because sparkFILEwls does that
        """

        __s_fn_id__ = "function <@property tmpDir>"

        try:
            if self._tmpDir is None:
                self._tmpDir = os.path.join(self.dataDir,__tmp_data_dir_ext__)
                logger.warning("%s NoneType tmpDIR set to default value %s",
                               __s_fn_id__,self._tmpDir)
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._tmpDir

    @tmpDir.setter
    def tmpDir(self,tmp_dir_path:str="")->str:
        """
        Description:
            Class property augments the directory path to the self.dataDir path to
            create a temporary storage of scraped data from each functional step.
            This is mainly to support data sharing between airflow process functions
        Attributes:
            tmp_dir_path (str) - a valid os.path
        Return:
            self._tmpDir
        Exception:
            Invalid string with improper director path formats will be rejected
        """

        __s_fn_id__ = "function <@tmpDir.setter>"

        try:
            if tmp_dir_path is None or "".join(tmp_dir_path.split())=="":
                raise AttributeError("Invalid tmp dir path")
            self._tmpDir = tmp_dir_path
            logger.debug("%s tmpDir relative path set to %s",
                         __s_fn_id__,self._tmpDir)
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._tmpDir

    ''' --- BOOKING SEARCH DATETIME --- '''
    @property
    def searchTimestamp(self) -> datetime:
        """
        Description:
            Class property retuns the starting date of the data with timestamp to be scraped.
            e.g., booking search datetime starting period
        Attributes:
            None
        Returns:
            self._dataDir (str)
        Exceptions:
            If self._searchTimestamp in None it will set to today
        """

        __s_fn_id__ = "function <@property searchTimestamp>"

        try:
            if self._searchTimestamp is None:
                self._searchTimestamp = datetime.now()
                logger.warning("%s NoneType searchTimestamp by default set to today %s",
                               __s_fn_id__,str(self._searchTimestamp))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return (self._searchTimestamp.replace(tzinfo=timezone.utc)).isoformat()

    @searchTimestamp.setter
    def searchTimestamp(self,search_datetime:datetime=None)->datetime:
        """
        Description:
            Class property retuns the starting date of the data with timestamp to be scraped.
            e.g., booking search datetime
        Attributes:
            search_datetime (datetime) - yyyy-mm-dd
        Return:
            self._searchTimestamp
        Exception:
            If search_datetime is None raise AttributeError
        """

        __s_fn_id__ = "function <@searchTimestamp.setter>"

        try:
            if not isinstance(search_datetime,datetime):
                raise AttributeError("Invalid search_datetime; must if a valid datetime.date")
            self._searchTimestamp = search_datetime
            logger.debug("%s searchTimestamp set to %s",
                         __s_fn_id__,str(self._searchTimestamp))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return (self._searchTimestamp.replace(tzinfo=timezone.utc)).isoformat()


    ''' --- SCRAPE FROM DATE --- '''
    @property
    def checkInDate(self) -> date:
        """
        Description:
            Class property retuns the starting date of the data with timestamp to be scraped.
            e.g., booking search datetime starting period
        Attributes:
            None
        Returns:
            self._dataDir (str)
        Exceptions:
            If self._checkInDate in None it will set to today
        """

        __s_fn_id__ = "function <@property checkInDate>"

        try:
            if self._checkInDate is None:
                self._checkInDate = date.today()
                logger.warning("%s NoneType checkInDate by default set to today %s",
                               __s_fn_id__,str(self._checkInDate))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._checkInDate

    @checkInDate.setter
    def checkInDate(self,check_in_date:datetime=None)->date:
        """
        Description:
            Class property retuns the starting date of the data with timestamp to be scraped.
            e.g., booking search datetime
        Attributes:
            check_in_date (date) - yyyy-mm-dd
        Return:
            self._checkInDate
        Exception:
            If check_in_date is None raise AttributeError
        """

        __s_fn_id__ = "function <@checkInDate.setter>"

        try:
            if not isinstance(check_in_date,date):
                raise AttributeError("Invalid check_in_date; must if a valid datetime.date")
            self._checkInDate = check_in_date
            logger.debug("%s checkInDate set to %s",
                         __s_fn_id__,str(self._checkInDate))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._checkInDate


    ''' --- SCRAPE END DATE --- '''
    @property
    def checkOutDate(self) -> date:
        """
        Description:
            Class property retuns the ending date of the data with timestamp to be scraped.
            e.g., booking search datetime ending period
        Attributes:
            None
        Returns:
            self._dataDir (str)
        Exceptions:
            If self._checkInDate in None it will set to today
        """

        __s_fn_id__ = "function <@property checkOutDate>"

        try:
            if self._checkOutDate is None:
                self._checkOutDate = self.checkInDate + timedelta(days=self.numOfNights)
                logger.warning("%s NoneType checkOutDate by default set to today %s",
                               __s_fn_id__,str(self._checkOutDate))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._checkOutDate

    @checkOutDate.setter
    def checkOutDate(self,scrape_to_date:date=None)->date:
        """
        Description:
            Class property retuns the ending date of the data with timestamp to be scraped.
            e.g., booking search datetime ending period
        Attributes:
            scrape_to_date (datetime.date) - yyyy-mm-dd
        Return:
            self._checkOutDate
        Exception:
            If scrape_to_date is None raise AttributeError
        """

        __s_fn_id__ = "function <@checkInDate.setter>"

        try:
            if not isinstance(scrape_to_date,date):
                raise AttributeError("Invalid scrape_to_date; must if a valid datetime.date")
            if scrape_to_date <= self.checkInDate:
                raise ValueError("%s is an invalid checkOutDate; must be greater that checkInDate:"
                                 % (scrape_to_date,elf.checkInDate))
            self._checkOutDate = scrape_to_date
            logger.debug("%s checkOutDate set to %s",
                         __s_fn_id__,str(self._checkOutDate))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._checkOutDate


    ''' --- NUMBER OF NIGHTS OFFSET --- '''
    @property
    def numOfNights(self) -> int:
        """
        Description:
            Class property retuns the value for offsetting the number of days from
            checkout to checkin.
        Attributes:
            None
        Returns:
            self._numOfNights (int)
        Exceptions:
            If self._numOfNights in None default will be set to 1
        """

        __s_fn_id__ = "function <@property numOfNights>"

        try:
            if self._numOfNights is None:
                self._numOfNights = 1
                logger.warning("%s NoneType numOfNights by default set to %s",
                               __s_fn_id__,str(self._numOfNights))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._numOfNights

    @numOfNights.setter
    def numOfNights(self,num_of_nights:int=1) -> int:
        """
        Description:
            Class property sets the value for offsetting the number of days from
            checkin to checkout.
        Attributes:
            offset (int)
        Return:
            self._numOfNights
        Exception:
            If num_of_nights is not an int > 0, then raise AttributeError
        """

        __s_fn_id__ = "function <@numOfNights.setter>"

        try:
            if not (isinstance(num_of_nights,int) and num_of_nights>0):
                raise AttributeError("Invalid num_of_nights; must be an INT > 0")
            self._numOfNights = num_of_nights
            logger.debug("%s numOfNights set to %s",__s_fn_id__,self._numOfNights)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._numOfNights


    ''' --- PAGE UPPER LIMIT --- '''
    @property
    def pageUpLimit(self) -> int:
        """
        Description:
            Class property retuns the upper limit to control the number of scrapped web pages.
            e.g., pageUpLimit = 10
        Attributes:
            None
        Returns:
            self._pageUpLimit (int)
        Exceptions:
            If self._pageUpLimit in None default will be set to 1
        """

        __s_fn_id__ = "function <@property pageUpLimit>"

        try:
            if self._pageUpLimit is None:
                self._pageUpLimit = 1
                logger.warning("%s NoneType pageUpLimit by default set to %s",
                               __s_fn_id__,str(self._pageUpLimit))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._pageUpLimit

    @pageUpLimit.setter
    def pageUpLimit(self,page_upper_limit:date=None) -> int:
        """
        Description:
            Class property set the upper limit to control the number of scrapped web pages.
            e.g., page_upper_limit = 10
        Attributes:
            page_upper_limit (int)
        Return:
            self._pageUpLimit
        Exception:
            If page_upper_limit is not an int > 0, then raise AttributeError
        """

        __s_fn_id__ = "function <@pageUpLimit.setter>"

        try:
            if not (isinstance(page_upper_limit,int) and page_upper_limit>0):
                raise AttributeError("Invalid page_upper_limit; must be an INT > 0")
            self._pageUpLimit = page_upper_limit
            logger.debug("%s pageUpLimit set to %s",
                         __s_fn_id__,self._pageUpLimit)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._checkOutDate


    ''' --- PAGE UPPER LIMIT --- '''
    @property
    def pageOffset(self) -> int:
        """
        Description:
            Class property retuns the offset to control the number of web pages
            to load for scrape at one time; e.g., offset = 10 will break down 
            the uppe limitinto pageUpLimit/offset number of iterations.
        Attributes:
            None
        Returns:
            self._pageOffset (int)
        Exceptions:
            If self._pageOffset in None default will be set to 1
        """

        __s_fn_id__ = "function <@property pageOffset>"

        try:
            if self._pageOffset is None:
                self._pageOffset = 1
                logger.warning("%s NoneType pageOffset by default set to %s",
                               __s_fn_id__,str(self._pageOffset))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._pageOffset

    @pageOffset.setter
    def pageOffset(self,page_offset:date=None) -> int:
        """
        Description:
            Class property sets the offset to control the number of web pages
            to load for scrape at one time; e.g., offset = 10 will break down 
            the uppe limit into pageUpLimit/offset number of iterations.
        Attributes:
            offset (int)
        Return:
            self._pageOffset
        Exception:
            If page_offset is not an int > 0, then raise AttributeError
        """

        __s_fn_id__ = "function <@pageOffset.setter>"

        try:
            if not (isinstance(page_offset,int) and page_offset>0):
                raise AttributeError("Invalid page_upper_limit; must be an INT > 0")
            self._pageOffset = page_offset
            logger.debug("%s pageOffset set to %s",__s_fn_id__,self._pageOffset)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._pageOffset


    ''' Function --- GET DESTINATION IDS ---

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-27

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def get_destination_ids(
        self,
        file_name:str,
        destins_dir=None,
        col_name="destinationID",
        **kwargs
    ):
        """
        Description:
            use the get_scrape_input_params function to load the the JSON file. 
            Then loop through all the OTAs to extract the input parameters
            For each OTA template url use the insert_params_in_url function to
            construct the list of parameterized URLs
        Attributes:
            inp_data_dir - path to the directory with property parameters JSON
            file_name - JSON file containing those parameters
            kwargs - 
        Returns (list) with the set of urls (scrape_url_list)
        Exceptions
        """

        __s_fn_id__ = "function <get_destination_ids>"
        _l_dests = []

        try:
            if file_name is None or "".join(file_name.split())=="":
                raise AttributeError("Invalid file_name attribute")

            ''' see if the file exists '''
            if destins_dir is not None: # or "".join(destins_dir.split())!="":
                self.destDir = destins_dir

            ''' read the destination ids into the list '''
            options = {"inferSchema":True,"header":True,"delimiter":",",}
            dest_df=clsFile.read_files_to_dtype(
                as_type='pandas',
                folder_path=self.destDir,
                file_name=file_name,
                file_type=None,
                **options,
            )
            if dest_df.shape[0] <=0:
                raise ValueError("Empty dataframe, NO destination data recovered from %s"
                                 %(file_path))

            _l_dests = list(dest_df[col_name])
            if len(_l_dests)>0:
                logger.info("%s recovered %d destinations from file %s in %s",
                            __s_fn_id__,len(_l_dests),file_name,destins_dir)



        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _l_dests


    ''' Function --- BUILD SCRAPE URL LIST ---
            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-27

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def build_scrape_url_list(
        self,
        file_name:str,
        inp_data_dir=None, 
        **kwargs
    ):
        """
        Description:
            use the get_scrape_input_params function to load the the JSON file. 
            Then loop through all the OTAs to extract the input parameters
            For each OTA template url use the insert_params_in_url function to
            construct the list of parameterized URLs
        Attributes:
            inp_data_dir - path to the directory with property parameters JSON
            file_name - JSON file containing those parameters
        Returns: 
            _tmpFPath (str) - 
            _ota_parameterized_url_list (list) - with the set of urls
        """

        __s_fn_id__ = "function <build_scrape_url_list>"

        _scrape_url_dict = {}
        _ota_parameterized_url_list = [] 

        err_count = 0      # logging the number of errors
        _tmpFPath = None   # tempory file path to store the parameterized URL list
        
        logger.info("Executing %s %s" % (self.__name__, __s_fn_id__))

        try:
            ''' validate and if given the dir path, else revert to defaul '''
            if file_name is None or "".join(file_name.split())=="":
                raise AttributeError("Invalid file_name attribute")

            if inp_data_dir is not None: # or "".join(inp_data_dir.split())!="":
                self.dataDir=inp_data_dir
            
            ''' check and initialize **kwargs '''
            if 'PAGEOFFSET' in kwargs:
                self.pageOffset = kwargs['PAGEOFFSET']
            if 'PAGEUPLIMIT' in kwargs:
                self.pageUpLimit = kwargs['PAGEUPLIMIT']
            if 'FROMDATE' in kwargs:
                self.checkInDate = kwargs['FROMDATE']
            if 'TODATE' in kwargs:
                self.checkOutDate=kwargs['TODATE']
            if 'CHECKOUTOFFSET' in kwargs:
                self._numOfNights = kwargs['CHECKOUTOFFSET']

            ''' retrieve OTA input and output property data from json '''
            property_dict = clsUtil.load_ota_list(
                folder_path=self.dataDir,
                file_name=file_name,
                **kwargs)

            if len(property_dict) <= 0:
                raise ValueError("No data found with %s with defined scrape properties"
                                 % os.path.join(inp_data_dir, file_name))
            logger.debug("Loaded %d properties to begin scraping OTA data.", len(property_dict))

            ''' get the input parameters from the properties file '''
            _ota_input_param_list = clsUtil.get_scrape_input_params(property_dict)
            logger.info("Input parameter list loaded %d rows successfully.",
                        len(_ota_input_param_list))
            
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
                        
                        _l_dest = self.get_destination_ids(
                            file_name=ota["locations"],   # filename with destination ids
                            destins_dir=self.destDir, #_destins_dir,   path to the desitnation folder
                            col_name="destinationID"    # column name with destination ids
                        )

                        if len(_l_dest) > 0:
                            self.destination_id = _l_dest
                            logger.info("Loaded %d destination ids", len(_l_dest))

                    ''' build the dictionary to replace the values in the url place holder '''
                    for destID in self.destination_id:
                        _inert_param_dict['destinationID'] = destID

                        if 'checkIn' in ota['inputs']:
                            for day_count in range(0,(self.checkOutDate - \
                                                      self.checkInDate).days):
                                _inert_param_dict['checkIn'] = self.checkInDate + \
                                                                    timedelta(days=day_count)
                                ''' if checkout date is a necessary parameter 
                                    then add 1 day to checkout date'''
                                if 'checkOut' in ota['inputs']:
                                    _inert_param_dict['checkOut'] = self.checkInDate + \
                                                    timedelta(days=day_count+self.numOfNights)
                                if "page" in ota['inputs']:
                                    next_page_offset = 0
                                    _parameterized_url = None
                                    while next_page_offset <= self.pageUpLimit:
                                        _inert_param_dict['page'] = next_page_offset
                                        _parameterized_url = clsUtil.insert_params_in_url(
                                            ota['url'],
                                            **_inert_param_dict)
#                                        scrape_url_list.append(_parameterized_url)
                                        _scrape_url_dict = {}     # init otherwise will overwrite the list
                                        _scrape_url_dict['ota']=ota['ota']
                                        _scrape_url_dict['destination_id']=destID
                                        _scrape_url_dict['checkin']=_inert_param_dict['checkIn']
                                        _scrape_url_dict['page_offset']=next_page_offset
                                        _scrape_url_dict['url']=_parameterized_url
                                        _ota_parameterized_url_list.append(_scrape_url_dict)
                                        next_page_offset += self.pageOffset

                    ''' add to dict the paramterized url list for OTA as key '''
#                    print(scrape_url_list)
#                    _scrape_url_dict[ota] = scrape_url_list

                except Exception as err:
                    ''' skip to processing the next OTA url if this one is None '''
                    err_count += 1
                    logger.warning("%s %s", __s_fn_id__,err)
                    print(err)

            if err_count>0:
                logger.info("%s skipped constructing %d ota urls with parameter errors",
                            __s_fn_id__, err_count)
            if len(_ota_parameterized_url_list) <=0:
                raise RuntimeError("%s was unable to parameterize %d input urls",
                                   __s_fn_id__, len(_ota_input_param_list))
            logger.info("%s parameterized %d urls with input values from %s.",
                        __s_fn_id__, len(_ota_parameterized_url_list),file_name)

#             if self._tmpDir:
#             if "WRITETOTMP" in kwargs and kwargs["WRITETOTMP"] is True:
            tmpDF = pd.DataFrame(_ota_parameterized_url_list)
#                    _tmpFPath = os.path.join(self.tmpDIR,"build_scrape_url_list.csv")
            _tmp_fname = self.__package__+"-"+"build-scrape-url-list.csv"
#                 _tmpFPath = os.path.join(self.tmpDir,_tmp_fname)
#                 tmpDF.to_csv(_tmpFPath, sep=',', index=False)
            tmpDF=clsFile.write_data(
                file_name=_tmp_fname,
                folder_path=self.tmpDir,
                data=tmpDF,
            )
            logger.info("%s wrote %d rows ota parameterized url lists to %s in %s",
                        __s_fn_id__,len(tmpDF),file_name,self._tmpDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._tmpDir,_tmp_fname, _ota_parameterized_url_list


    ''' Function --- MAKE STORGE DIRECTORY ---

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-22
    '''
    def make_storage_dir(self, **kwargs):
        """
        Description:
            give the relative root strage location to amend a date & time
            specific folder w.r.t the search datetime
        Attributes:
            inp_data_dir - string with folder path to the csv files
            **kwargs - contain the plance holder key value pairs
                PARENTDIR (str) - to append a directory path to the dataDir
                FOLDERPATH (str)- directly provide a folder path, relative to the root
                TIMESTAMP (datetime) - alterative timestamp to the default self_searchTimestamp
                PREFIX (str) - a string to append before the timestamp in the folder name
                POSTFIX (str) - a string to append after the timestamp in thefolder name
                INTERVAL (int) - specifies the searth time in minutes past the hour
        Returns:
            _prop_search_folder (str) the relative directory path with the timestamped folder
        Exceptions:
            None
        """
        
        __s_fn_id__ = "function <make_storage_dir>"
#         logger.info("Executing %s %s" % (self.__package__, __s_fn_id__))
        
        _prop_search_folder = None
        
        try:
            _par_dir = "search"   # initialse parent directory name to append to dataDir
            if "PARENTDIR" in kwargs.keys():
                _par_dir = kwargs['PARENTDIR']
            _dir_path=os.path.join(self.dataDir,_par_dir)   # append parent dir path to dataDir
            if "FOLDERPATH" in kwargs.keys() and isinstance(kwargs['FOLDERPATH'],str):
                _dir_path = kwargs['FOLDERPATH']   #   set to what's in the key value
            _timestamp=self.searchTimestamp   # initialize timestamp with searchTimestamp
            if "TIMESTAMP" in kwargs.keys() and isinstance(kwargs['TIMESTAMP'],datetime):
                _timestamp = kwargs['TIMESTAMP']
            _prefix=""   # initialize the folder name prefix
            if "PREFIX" in kwargs.keys():
                _prefix = kwargs['PREFIX']
            _postfix=""   # initialize the folder name postfix
            if "POSTFIX" in kwargs.keys():
                _postfix = kwargs['POSTFIX']
            _min_interval=0   # initialize the offset time interval
            if "MININTERVAL" in kwargs.keys() and kwargs['MININTERVAL'] != 0:
                _min_interval = kwargs['MININTERVAL']

            _prop_search_folder=clsUtil.get_timestamped_storage(
                folder_path=_dir_path,
                folder_prefix=_prefix,
                folder_postfix=_postfix,
                timestamp=_timestamp,
                minute_interval=_min_interval,
                **kwargs
            )

            logger.info("Folder %s ready for storing scraped data" % _prop_search_folder)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _prop_search_folder


    ''' Function --- SCRAPE BOOKINGS TO STORAGE ---

            author(s): <nileka.desilva@rezgateway.com>
                        <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-10
    '''
    def _scrape_bookings_to_csv(
        self,
        url,   # parameterized url
        ota_name, # ota name
        checkin_date, # intended checkin date
        search_dt,    # scrape run date time
        destination_id, # location searched for
        file_name, # store in csv file
        path       # directory path to store csv
    ):
        """
        Description:
            specific to bookings.com scrape, extract, and load the data in a csf file
        Attributes:
            url - string comprising the url with place holders
            **kwargs - contain the plance holder key value pairs
        Returns:
        Exceptions:
        """

        from bs4 import BeautifulSoup # Import for Beautiful Soup
        import requests # Import for requests
        import lxml     # Import for lxml parser
        import csv
        from csv import writer

        saveTo = None
        _save_df = pd.DataFrame()

        __s_fn_id__ = "function <_scrape_bookings_to_csv>"
#        logger.info("Executing %s", __s_fn_id__)

        try:
            headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            response = requests.get(url, headers=headers)
            # check the response code
            if response.status_code != 200:
                raise RuntimeError("Invalid response with code %d" % response.status_code)

            # Make it a soup
            soup = BeautifulSoup(response.text,"lxml")

            lists = soup.select(".d20f4628d0")

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
                    _data_dict['room_rate'] = _list.find('span', class_='fcab3ed991 fbd1d3018c e729ed5ab6').text
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
                    _data_dict['location_desc'] = _list.find('span', class_='f4bd0794db b4273d69aa').text
                except Exception as text_err:
                    _data_dict['location_desc'] = None
                    _data_err = True
                    logger.warning('location_desc - %s',text_err)
                    pass
                except Exception as text_err:
                    _data_dict['distance_desc'] = None
                    _data_err = True
                    logger.warning('distance_desc - %s',text_err)                 
                try:
                    _data_dict['room_desc'] = _list.find('div', class_='cb5b4b68a4').text
                except Exception as text_err:
                    _data_dict['room_desc'] = None
                    _data_err = True
                    logger.warning('room_desc - %s',text_err)
                try:
                    _data_dict['breakfast'] = _list.find('span', class_='e05969d63d').text
                except Exception as text_err:
                    _data_dict['breakfast'] = None
                    _data_err = True
                    logger.warning('breakfast - %s', text_err)
                try:
                    _data_dict['cancellations'] = _list.find('div', class_='d506630cf3').text
                except Exception as text_err:
                    _data_dict['cancellations'] = None
                    _data_err = True
                    logger.warning('cancellations - %s', text_err)
                try:
                    _data_dict['availability'] = _list.find('div', class_='cb1f9edcd4').text
                except Exception as text_err:
                    _data_dict['availability'] = None
                    _data_err = True
                    logger.warning('availability - %s', text_err)
                try:
                    _data_dict['_star_rating_info'] = _list.find('div', class_='e4755bbd60').text
                except Exception as text_err:
                    _data_dict['_star_rating_info'] = None
                    _data_err = True
                    logger.warning('_star_rating_info - %s', text_err)
                try:
                    _data_dict['star_rating'] = _star_rating_info.get('aria-label')
                except Exception as text_err:
                    _data_dict['star_rating'] = None
                    _data_err = True
                    logger.warning('star_rating - %s', text_err)

                if bool(_data_dict):
                    _save_df = pd.concat([_save_df,pd.DataFrame(_data_dict)])
                if _data_err:
                    logger.warning("Above text extraction errors were caused by url: \n %s \n",url)


            if _save_df.shape[0] > 0:
                saved_data_ = clsFile.write_data(
                    file_name=file_name,
                    folder_path=path,
                    data=_save_df,
                )
#                 if self.storeMethod == 'local':
# #                    print(saveTo)
#                     _save_df.to_csv(saveTo,index=False, sep=',')
#                 elif self.storeMethod == 'AWS-S3':
#                     print("todo")
#                 else:
#                     raise ValueError("%s is an undefined storage location in **kwargs"
#                                      % (kwargs['storageLocation']))

#                _save_df.to_csv(saveTo,index=False, sep=',')
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return saveTo


    ''' Function --- SCRAPE URL LIST ---
    
            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2023-03-17
    '''

    def scrape_url_list(
        self,
        ota_url_file_name:str=None,  # file name holding the parameterized urls
        ota_url_dir_path:str =None,  # directory path to the file with parameterized urls
#         otasURLlist:list = None,   # parameter inserted url list to run scraping 
        search_datetime:datetime=None,   # search datetime to search for rooms and prices
        save_in_dir:str=None,     # timestamped folder path to store data
        **kwargs,
    ):
        """
        Description:
            reads the all the csv files in the entire folder and appends the data
            for the relevant columns defined in the dictionary into a dataframe
        Attributes:
            otasURLlist (str) - parameter inserted url list to run scraping 
            search_datetime (datetime) - sets the searchTimestamp searching rooms and prices
            save_in_dir (str)- timestamped folder path to store data
        Returns:
            saved_file_list_ (list) - list of scraped data saved csv file names
        Exceptions:
            Do not proceed if otasURLlist has no values
        """

        __s_fn_id__ = "function <scrape_url_list>"

        saveTo = None   # init file name
        otasURLlist = None
        saved_file_list_ = []

#         logger.info("Executing %s %s" % (self.__package__, __s_fn_id__))

        try:
#                 urlDF = clsFile.read_csv(_otaURLfilePath, sep=",")
            urls_df = clsFile.read_files_to_dtype(
                as_type='pandas',
                folder_path=ota_url_dir_path,
                file_name=ota_url_file_name,#__local_file_name__,
                file_type=None,
                **kwargs,
            )
            otasURLlist = urls_df.to_dict('records')
            logger.debug("loaded %d parameterized urls from list %s",
                        len(otasURLlist),os.path.join(ota_url_dir_path,ota_url_file_name))

#             if len(otasURLlist) <= 0:
#                 raise ValueError("List of URLs required to proceed; non defined in list.")
#                 print("loading parameterized urls from list %d records" % len(otasuRLlist))
#             else:
            if isinstance(search_datetime,datetime):
                self.searchTimestamp=search_datetime
            if save_in_dir is None or "".join(save_in_dir.split())=="":
                save_in_dir=self.make_storage_dir(**kwargs)
                logger.warning("Unspecified storage location; default set to %s",save_in_dir)
                
            ''' loop through the list of urls to scrape and save the data'''
            for ota_dict in otasURLlist:
                ''' file name is concaternation of ota name + location + checkin date + page offset
                    and .csv file extension'''
#                 _fname = str(ota_dict['ota'])+"."+\
#                         str(ota_dict['destination_id'])+"."+\
#                         str(ota_dict['checkin'])+"."+\
#                         str(ota_dict['page_offset']).zfill(3)+\
#                         ".csv"
                _fname = ".".join([str(ota_dict['ota']),str(ota_dict['destination_id']),
                                   str(ota_dict['checkin']),str(ota_dict['page_offset']).zfill(3),
                                   "csv"])
                _fname=_fname.replace(" ",".")

                ''' TODO add search_datetime'''
                if ota_dict['ota'] == 'booking.com':
                    saveTo = self._scrape_bookings_to_csv(
                        url=ota_dict['url'],   # constructed url with parameters
                        ota_name=ota_dict['ota'],   # ota name data source
                        checkin_date=ota_dict['checkin'],  # booking intended checkin date
                        search_dt=self.searchTimestamp, # date & time scraping was executed
#                         searchDT,   # date & time scraping was executed
                        destination_id=ota_dict['destination_id'],  # destingation id to lookup the name
                        file_name=_fname,     # csv file name to store in
                        path=save_in_dir     # folder name to save the files
                    )
                    saved_file_list_.append(saveTo)


        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return saved_file_list_

    ''' Function

            author: <nuwan.waidyanatha@rezgateway.com>
            modified: 2022-09-29
    '''

    def extract_room_rate(
        self,
        room_data_df,   # data frame with the room rates
        rate_col_name:str="room_rate",   # column name with the room rates
        aug_col_name:str="room_price",   # column name to save the room category
        curr_col_name:str="currency",   # column to store the currency abbr
        **kwargs,       # kwargs = {}
    ):
        
        __s_fn_id__ = "function <extract_room_rate>"
        logger.info("Executing %s %s" % (self.__package__, __s_fn_id__))

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
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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
        
        __s_fn_id__ = "function <merge_similar_room_cate>"
        logger.info("Executing %s %s",self.__package__, __s_fn_id__)

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
            ''' iterate through each room type to get the similarity scores 
                against the room type taxonomy '''
            for rowIdx, rowVal in room_data_df.iterrows():
                ''' get the similarity scores from nlp utils calss'''
                sim_scores_df = clsNLP.get_similarity_scores(
                    input_sentences = [rowVal.room_type], # cannot use list() it will split into letters
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
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = "function <assign_lx_name>"
        logger.info("Executing %s %s",self.__package__, __s_fn_id__)

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
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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
        
        __s_fn_id__ = "function <merge_similar_room_cate>"
        logger.info("Executing %s %s",self.__package__, __s_fn_id__)

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
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return count, data_df

    '''Function
    name:_clean_columns
     procedure: clean and replace unwanted characters in room_data_df columns'''

    def _clean_columns(self, room_data_df, col_name, **kwargs):
        __s_fn_id__ = "function <_clean_columns>"
        logger.info("Executing %s %s", self.__package__, __s_fn_id__)

        emb_kwargs = {
            "LOWER": True,
            "NO_STOP_WORDS": False,
            "METRIC": 'COSIN',
            "MAX_SCORES": 2,
        }
        try:
            room_data_df['room_desc'] = room_data_df['room_desc'].str.replace("•", "|")
            room_data_df['cancellations'] = room_data_df['cancellations'].str.replace("•", "|")
            room_data_df['breakfast'] = room_data_df['breakfast'].replace("Breakfast included",
                                                                                              "yes").replace("None", "no")
            booking_com_details_df['Star Rating(out of 5)'] = booking_com_details_df['Star Rating(out of 5)'].str.replace(
                " out of 5", "")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _clean_columns