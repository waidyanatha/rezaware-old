#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "logReturns"
__package__ = "etp"
__module__ = "assets"
__app__ = "mining"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages '''
    import os
    import sys
    import configparser    
    import logging
    import traceback
    ''' function specific python packages '''
#     import pandas as pd
    from datetime import datetime, date, timedelta

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))


'''
    CLASS spefic to preparing the data based on Ration Of Returns (ROR).
    
'''

class RatioOfReturns():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="market cap data prep", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        self._data = None

        global pkgConf
        global appConf
        global logger
        global clsSpark

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)

#             self.pckgDir = pkgConf.get("CWDS",self.__package__)
#             self.appDir = pkgConf.get("CWDS",self.__app__)
#             ''' DEPRECATED: get the path to the input and output data '''
#             self.dataDir = pkgConf.get("CWDS","DATA")

#             ''' set app configparser '''
#             appConf = configparser.ConfigParser()
#             appConf.read(os.path.join(self.appDir, self.__conf_fname__))

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

            ''' import file work load utils to read and write data '''
            from utils.modules.etl.load import sparkDBwls as db
            clsSpark = db.SQLWorkLoads(desc=self.__desc__)
            from utils.modules.ml.timeseries import rollingstats as stats
            clsStats = stats.RollingStats(desc=self.__desc__)
#             clsRW.storeMode = pkgConf.get("DATASTORE","MODE")
#             clsRW.storeRoot = pkgConf.get("DATASTORE","ROOT")
#             logger.info("Files RW mode %s with root %s set",clsRW.storeMode,clsRW.storeRoot)
#             ''' set the package specific storage path '''
#             self.storePath = os.path.join(
#                 self.__app__,
#                 "data/",
#                 self.__module__,
#                 self.__package__,
#             )
#             logger.info("%s package files stored in %s",self.__package__,self.storePath)

#             ''' import mongo work load utils to read and write data '''
#             from utils.modules.etl.load import noSQLwls as nosql
#             clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)
            
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None



    ''' Function
            name: data @property and @setter functions
            parameters: data in @setter will instantiate self._data

            procedure: make sure it is a valid spark dataframe
            
            return (dataframe) self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):

        __s_fn_id__ = "function <@property data>"

        try:
            if self._data is None:
                raise ValueError("Data is of NoneType; cannot be used in any %s computations"
                                 %self.__name__)
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data
    
    @data.setter
    def data(self,data):

        __s_fn_id__ = "function <@setter data>"

        try:
            if data is None:
                raise AttributeError("Invalid data attribute, must be a valid pyspark dataframe")

            self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    ''' Function
            name: read_past_data
            parameters:

            procedure: 
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
#     @countNulls
#     @impute
    def read_n_clean_mcap(self, query:str="", **kwargs):

        __s_fn_id__ = "function <read_past_data>"
        _table ='warehouse.mcap_past'
        _column='mcap_date'
        _to_date =date.today()
        _from_date=_to_date-timedelta(days=7)
        __strategy__='mean'

        try:
            if "TABLENAME" in kwargs.keys():
                _column=kwargs['TABLENAME']
            if "COLUMN" in kwargs.keys():
                _column=kwargs['COLUMN']
            if "FROMDATETIME" in kwargs.keys():
                _from_date=kwargs['FROMDATETIME']
            if "TODATETIME" in kwargs.keys():
                _to_date=kwargs['TODATETIME']

            if not query is None and "".join(query.split())!="":
                self._data = clsSpark.read_data_from_table(select=query, **kwargs)
            else:
                self._data = clsSpark.read_data_from_table(
                    db_table=_table,
                    db_column=_column,
                    lower_bound=_from_date,
                    upper_bound=_to_date,
                    **kwargs)

            if self._data.count() > 0:
                logger.debug("%s loaded %d rows",__s_fn_id__,self._data.count())
            else:
                raise ValueError("%s did not read any data",__s_fn_id__)

            ''' transpose to get assets in the columns '''
            pivot_mcap=self._data.groupBy("mcap_date").pivot("asset_name").sum("mcap_value")
            ''' impute to fill the gaps '''
            _col_subset = pivot_mcap.columns
            _col_subset.remove('mcap_date')
            self._data = clsSpark.impute_data(
                data=pivot_mcap,
                column_subset=_col_subset,
                strategy=__strategy__,
            )
            ''' ensure there are no non-numeric values '''
#             _col_subset = mcap_sdf.columns
#             _col_subset.remove('mcap_date')
            _nan_counts_sdf = clsSpark.count_column_nulls(
                data=self._data,
                column_subset=_col_subset
            )

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    ''' Function
            name: pivot and clean data
            parameters:

            procedure: 
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

