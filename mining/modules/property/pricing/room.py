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

    print("All {0} software packages loaded successfully!".format(__package__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

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
    def __init__(self, desc : str="data", **kwargs):
        
        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__desc__ = desc
        self._data = None
        
        _s_fn_id = "__init__"

        global config
        global logger

        self.cwd=os.path.dirname(__file__)
        sys.path.insert(1,self.cwd)

        try:
            config = configparser.ConfigParser()
            config.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = config.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)

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
            logger.info("%s initialization for %s module package %s %s done. Starting the workloads for %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))
            print("Initialing %s class for %s with instance %s" 
                  % (self.__package__, self.__name__, self.__desc__))

            ''' Set the wrangler root directory '''
            self.pckgDir = config.get("CWDS",self.__package__)
            self.appDir = config.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
            self.dataDir = os.path.join(config.get("CWDS","DATA"),__room_price_data__)

    #         from utils.modules.etl.load import sparkwls as spark
    #         clsSparkWL = spark.SparkWorkLoads(desc="ota property price scraper")
            logger.debug("Data path is %s\nPackage path is %s\nApp path is %s"
                         ,self.dataDir,self.pckgDir,self.appDir)
            print("Data path set to %s" % self.dataDir)
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
    def data(self, para_dict):

        _s_fn_id = "function @data.setter"
        logger.debug("Executing %s %s" % (self.__package__, _s_fn_id))
        date_format = "%Y-%m-%d"
        _room_df = pd.DataFrame()

        try:
            self._data = self.get_room_prices()

#             if not isinstance(para_dict,dict):
#                 raise ValueError("Invalid parameter dictionary")
            if isinstance(para_dict,dict):
                logger.debug("Filtering rooms data for %s" % str(para_dict.keys()))
                ''' set the data filter parameters '''
#                 _locations_list = None
                if "LOCATIONS" in para_dict.keys() and \
                    isinstance(para_dict['LOCATIONS'],list):
                    _locations_list = para_dict['LOCATIONS']
                    self._data = self._data[
                        self._data['dest_lx_name'].str.lower().isin(
                            [x.lower() for x in para_dict['LOCATIONS']])]

                ''' to filter by search date range '''
                if "SEARCH_DATETIME" in para_dict.keys() and \
                    isinstance(para_dict['SEARCH_DATETIME'],dict):
                    _search_start_dt=None
                    if isinstance(para_dict['SEARCH_DATETIME']['START'],str):
                        _search_start_dt = datetime.strptime(
                            para_dict['SEARCH_DATETIME']['START'],
                            date_format)
                        self._data = self._data[self._data['search_dt'] >= _search_start_dt]
                    _search_end_dt=None
                    if isinstance(para_dict['SEARCH_DATETIME']['END'],str):
                        _search_end_dt=datetime.strptime(
                            para_dict['SEARCH_DATETIME']['END'],
                            date_format)
                        self._data = self._data[self._data['search_dt'] <= _search_end_dt]
                ''' to filter by checkin date '''
                if "CHECKIN_DATETIME" in para_dict.keys() and \
                    isinstance(para_dict['CHECKIN_DATETIME'],dict):
                    _checkin_start_dt=None
                    if isinstance(para_dict['CHECKIN_DATETIME']['START'],str):
                        _checkin_start_dt=datetime.strptime(
                            para_dict['CHECKIN_DATETIME']['START'],
                            date_format)
                        self._data = self._data[self._data['checkin_date'] >= _checkin_start_dt]
                    _checkin_end_dt=None
                    if isinstance(para_dict['CHECKIN_DATETIME']['END'],str):
                        _checkin_end_dt=datetime.strptime(
                            para_dict['CHECKIN_DATETIME']['END'],
                            date_format)
                        self._data = self._data[self._data['checkin_date'] <= _checkin_end_dt]

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())
            return None

        return self._data


    ''' Function
            name: get data
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_room_prices(self, **kwargs):

        sys.path.insert(1,self.rezHome)
        from utils.modules.etl.load import sparkwls

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
            clsSparkWL = sparkwls.SparkWorkLoads(desc=_desc, **spark_kwargs)
            ''' read data from table '''
            get_data_kwargs = {
                "TO_PANDAS":True,
                "DROP_DUPLICATES":True
            }
            __tableName__ = "ota_property_prices"
            room_df = clsSparkWL.get_data_from_table(dbTable=__tableName__,**get_data_kwargs)
            logger.debug("Loaded %d rows from %s table" % (room_df.shape[0],__tableName__))
            
        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())
        
#         return room_df
        return room_df