#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "RoomPrices"
__module__ = "property"
__package__ = "pricing"
__app__ = "mining"
__ini_fname__ = "app.ini"
__room_price_data__ = ""
# __inputs_file_name__ = "ota_input_urls.json"

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

    print("All %s-module %s-packages in function-%s imported successfully!"
          % (__module__,__package__,__name__))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__,__package__,__name__,e))

'''
    CLASS spefic to room price analysis and forecasting for 1,7,30, and 90 -days
    
    contributor(s):
            <nuwan.waidyanatha@rezgateway.com>
'''

class RoomPrice():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc, **kwargs):
        
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

        self._data = None
        
        _s_fn_id = "__init__"

        global config
        global logger
        global clsRW
        global clsSpark

        try:
#             sys.path.insert(1,self.cwd)

            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,self.__ini_fname__))
            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)
            ''' cluster data '''
            from utils.modules.ml.cluster import points
            clsCluster = points.ClusterWorkLoads(desc=self.__desc__)
            ''' read/write data from and to databases '''
            from utils.modules.etl.load import sparkDBwls as spark
            clsSpark = spark.SparkWorkLoads(desc=self.__desc__)
            ''' import file work load utils to read and write data '''
            from utils.modules.etl.load import filesRW as rw
            clsRW = rw.FileWorkLoads(desc=self.__desc__)
            clsRW.storeMode = pkgConf.get("DATASTORE","MODE")
            clsRW.storeRoot = pkgConf.get("DATASTORE","ROOT")
            
#             self.pckgDir = pkgConf.get("CWDS",self.__package__)
#             self.appDir = pkgConf.get("CWDS",self.__app__)
#             config = configparser.ConfigParser()
#             config.read(os.path.join(self.cwd,__ini_fname__))

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
            logger.info("%s Class",self.__name__)

            logger.info("Files RW mode %s with root %s set",
                        clsRW.storeMode,clsRW.storeRoot)
            
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

#             ''' set the tmp dir to store large data to share with other functions
#                 if self.tmpDIR = None then data is not stored, otherwise stored to
#                 given location; typically specified in app.conf
#             '''
#             self.tmpDIR = None
#             if "WRITE_TO_TMP":
#                 self.tmpDIR = os.path.join(self.storePath,"tmp/")
#                 if not os.path.exists(self.tmpDIR):
#                     os.makedirs(self.tmpDIR)

#             print("Initialing %s class for %s with instance %s" 
#                   % (self.__package__, self.__name__, self.__desc__))

#             ''' Set the wrangler root directory '''
#             self.pckgDir = config.get("CWDS",self.__package__)
#             self.appDir = config.get("CWDS",self.__app__)
#             ''' get the path to the input and output data '''
#             self.dataDir = os.path.join(config.get("CWDS","DATA"),__room_price_data__)

#     #         from utils.modules.etl.load import sparkwls as spark
#     #         clsSpark = spark.SparkWorkLoads(desc="ota property price scraper")
#             logger.debug("Data path is %s\nPackage path is %s\nApp path is %s"
#                          ,self.dataDir,self.pckgDir,self.appDir)
#             print("Data path set to %s" % self.dataDir)

            print("%s Class initialization complete" % self.__name__)

            return None
        
        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())
            return None


    @property
    def data(self, **kwargs):

        return self._data

    @data.setter
    def data(self, _data):

        __s_fn_id__ = "function <@data.setter>"

        try:
            if data is None:
                raise AttributeError("Invalid input parameter")
            self._data = data

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._data
                
#             self._data = self.get_room_prices()

# #             if not isinstance(para_dict,dict):
# #                 raise ValueError("Invalid parameter dictionary")
#             if isinstance(para_dict,dict):
#                 logger.debug("Filtering rooms data for %s" % str(para_dict.keys()))
#                 ''' set the data filter parameters '''
# #                 _locations_list = None
#                 if "LOCATIONS" in para_dict.keys() and \
#                     isinstance(para_dict['LOCATIONS'],list):
#                     _locations_list = para_dict['LOCATIONS']
#                     self._data = self._data[
#                         self._data['dest_lx_name'].str.lower().isin(
#                             [x.lower() for x in para_dict['LOCATIONS']])]

#                 ''' to filter by search date range '''
#                 if "SEARCH_DATETIME" in para_dict.keys() and \
#                     isinstance(para_dict['SEARCH_DATETIME'],dict):
#                     _search_start_dt=None
#                     if isinstance(para_dict['SEARCH_DATETIME']['START'],str):
#                         _search_start_dt = datetime.strptime(
#                             para_dict['SEARCH_DATETIME']['START'],
#                             date_format)
#                         self._data = self._data[self._data['search_dt'] >= _search_start_dt]
#                     _search_end_dt=None
#                     if isinstance(para_dict['SEARCH_DATETIME']['END'],str):
#                         _search_end_dt=datetime.strptime(
#                             para_dict['SEARCH_DATETIME']['END'],
#                             date_format)
#                         self._data = self._data[self._data['search_dt'] <= _search_end_dt]
#                 ''' to filter by checkin date '''
#                 if "CHECKIN_DATETIME" in para_dict.keys() and \
#                     isinstance(para_dict['CHECKIN_DATETIME'],dict):
#                     _checkin_start_dt=None
#                     if isinstance(para_dict['CHECKIN_DATETIME']['START'],str):
#                         _checkin_start_dt=datetime.strptime(
#                             para_dict['CHECKIN_DATETIME']['START'],
#                             date_format)
#                         self._data = self._data[self._data['checkin_date'] >= _checkin_start_dt]
#                     _checkin_end_dt=None
#                     if isinstance(para_dict['CHECKIN_DATETIME']['END'],str):
#                         _checkin_end_dt=datetime.strptime(
#                             para_dict['CHECKIN_DATETIME']['END'],
#                             date_format)
#                         self._data = self._data[self._data['checkin_date'] <= _checkin_end_dt]

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())
#             return None

#         return self._data


    ''' Function
            name: read_search_data data
            parameters:
                **kwargs - use the kwargs to filter the data by location, search_datetime, 
                            room_type, checkin_datetime,
                SEARCH_DATETIME (dict) - two key value pairs START and END datetime
                CHECKIN_DATETIME (dict) - two key value pairs START and END datetime
                LOCATION (dict) - key value pair with a location attribute and list to filter

            prerequisits: the app.ini section=DATASTORE must define the MODE & ROOT
            
            procedure: Read data from filepath relative to the ROOT
                        convert and return as a pandas dataframe
                        
            return self._data (dataframe)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def read_search_data(
        self,
        folder_path:str,
        file_name=None,
        file_type=None,
        **kwargs):

        __s_fn_id__ = "function <read_search_data>"
        date_format = "%Y-%m-%d"
        _room_df = pd.DataFrame()

        try:
            __as_type__ = "PANDAS"  # specify data type to return from read file

            if ''.join(folder_path.split())=="":
                raise AttributeError("Folder path to the search data is mandetory")
            if (not file_name is None) and (''.join(file_name.split())!=""):
                _file_type = None
            elif (not file_type is None) and (''.join(file_type.split())!=""):
                file_name = None
            else:
                raise AttributeError("Eithe a file_name or a file_type must be specified")

            _store_metadata = {
            #     "action":'read',
                "asType":__as_type__,
                "fileType":_file_type,
                "filePath":folder_path,
                "fileName":file_name
            }
            clsRW.storeData = _store_metadata   # sets class data property for the criteria
            self.data = clsRW.storeData   # assign the data to local data class property
            ''' filter the data by location, search_dt, and checkin_dt '''
            if isinstance(para_dict,dict):
                logger.debug("Filtering rooms data for %s" % str(para_dict.keys()))
                ''' set the data filter parameters '''
#                 _locations_list = None
                if "LOCATIONS" in para_dict.keys() and \
                    isinstance(para_dict['LOCATIONS'],list):
                    _locations_list = para_dict['LOCATIONS']
                    self._data = self.data[
                        self.data['dest_lx_name'].str.lower().isin(
                            [x.lower() for x in para_dict['LOCATIONS']])]

                ''' to filter by search date range '''
                if "SEARCH_DATETIME" in para_dict.keys() and \
                    isinstance(para_dict['SEARCH_DATETIME'],dict):
                    _search_start_dt=None
                    if isinstance(para_dict['SEARCH_DATETIME']['START'],str):
                        _search_start_dt = datetime.strptime(
                            para_dict['SEARCH_DATETIME']['START'],
                            date_format)
                        self._data = self.data[self.data['search_dt'] >= _search_start_dt]
                    _search_end_dt=None
                    if isinstance(para_dict['SEARCH_DATETIME']['END'],str):
                        _search_end_dt=datetime.strptime(
                            para_dict['SEARCH_DATETIME']['END'],
                            date_format)
                        self._data = self.data[self.data['search_dt'] <= _search_end_dt]
                ''' to filter by checkin date '''
                if "CHECKIN_DATETIME" in para_dict.keys() and \
                    isinstance(para_dict['CHECKIN_DATETIME'],dict):
                    _checkin_start_dt=None
                    if isinstance(para_dict['CHECKIN_DATETIME']['START'],str):
                        _checkin_start_dt=datetime.strptime(
                            para_dict['CHECKIN_DATETIME']['START'],
                            date_format)
                        self._data = self.data[self.data['checkin_date'] >= _checkin_start_dt]
                    _checkin_end_dt=None
                    if isinstance(para_dict['CHECKIN_DATETIME']['END'],str):
                        _checkin_end_dt=datetime.strptime(
                            para_dict['CHECKIN_DATETIME']['END'],
                            date_format)
                        self._data = self.data[self.data['checkin_date'] <= _checkin_end_dt]            

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data


    ''' Function
            name: get_room_type_clusters data
            parameters:
            
            procedure:
                        
            return self._data (dataframe)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_room_type_clusters(
        self,
        data,
        **kwargs):

        __s_fn_id__ = "function <get_room_type_clusters>"
        date_format = "%Y-%m-%d"

        try:
            if data:
                self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data


    ''' Function
            name: get_room_type_clusters data
            parameters:
            
            procedure:
                        
            return self._data (dataframe)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def write_data_to_db(
        self,
        tbl_Name:str,
        data=None,
        db_name=None,
        **kwargs):

        __s_fn_id__ = "function <write_data_to_db>"
        date_format = "%Y-%m-%d"

        try:
            if data:
                self._data = data
            if ''.join(tbl_Name.split())=="":
                raise AttributeError("Invalid table name")
            if (not db_name is None) and (''.join(db_name.split())!=""):
                clsSpark.dbName = db_name

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data


    ''' Function -- To Be Completed Experimental Code ---
            name: get data
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_room_prices(self, **kwargs):

        room_df = pd.DataFrame()
        _s_fn_id = "function <load_ota_list>"
        logger.debug("Executing %s %s" % (self.__package__, _s_fn_id))

        try:
            ''' instantiate spark '''
            _desc = "get room prices for analytics and forecasts"
            ''' optional - if not specified class will use the default values '''
            spark_kwargs = {"WRITE_TO_FILE":False,   # necessary to emulate the etl dag
                           "DATA_STORE":"LOCAL",   # values = aws-s3, local
                          }
            clsSpark = sparkwls.SparkWorkLoads(desc=_desc, **spark_kwargs)
            ''' read data from table '''
            get_data_kwargs = {
                "TO_PANDAS":True,
                "DROP_DUPLICATES":True
            }
            __tableName__ = "ota_property_prices"
            room_df = clsSpark.get_data_from_table(dbTable=__tableName__,**get_data_kwargs)
            logger.debug("Loaded %d rows from %s table" % (room_df.shape[0],__tableName__))
            
        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())
        
#         return room_df
        return room_df