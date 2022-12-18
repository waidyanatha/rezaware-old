#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "clusterQuality"
__package__ = "cluster"
__module__ = "ml"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages '''
    import os
    import sys
    import logging
    import traceback
    import functools
    import configparser
    ''' function specific python packages '''
    import pandas as pd
    import json
    from datetime import datetime, date, timedelta
    from requests import Request, Session
    from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
    import time   # to convert datetime to unix timestamp int
    import re


    print("All %s-module %s-packages in function-%s imported successfully!"
          % (__module__,__package__,__name__))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__,__package__,__name__,e))


'''
    CLASS spefic to providing reusable functions for scraping ota data
'''

class Measures():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="clusterQuality Class", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc
        
        self._data = None
        self._connection = None
        self._prices = None

        global pkgConf
#         global appConf
        global logger
        global clsRW
        global clsNoSQL

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,self.__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)
            
            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
            ''' DEPRECATED: get the path to the input and output data '''
            self.dataDir = pkgConf.get("CWDS","DATA")

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
            from utils.modules.etl.load import filesRW as rw
            clsRW = rw.FileWorkLoads(desc=self.__desc__)
            clsRW.storeMode = pkgConf.get("DATASTORE","MODE")
            clsRW.storeRoot = pkgConf.get("DATASTORE","ROOT")
            logger.info("Files RW mode %s with root %s set",clsRW.storeMode,clsRW.storeRoot)

            ''' set the package specific storage path '''
            ''' TODO change to use the utils/FileRW package '''
            self.storePath = pkgConf.get("CWDS","DATA")
#             self.storePath = os.path.join(
#                 self.__app__,
#                 "data/",
#                 self.__module__,
#                 self.__package__,
#             )
    
            logger.info("%s package files stored in %s",self.__package__,self.storePath)

            ''' import mongo work load utils to read and write data '''
#             from utils.modules.etl.load import noSQLwls as nosql
            from utils.modules.etl.load import sparkNoSQLwls as nosql
            clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)
            
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            ''' set the tmp dir to store large data to share with other functions
                if self.tmpDIR = None then data is not stored, otherwise stored to
                given location; typically specified in app.conf
            '''
            self.tmpDIR = None
            if "WRITE_TO_TMP":
                self.tmpDIR = os.path.join(self.storePath,"tmp/")
                if not os.path.exists(self.tmpDIR):
                    os.makedirs(self.tmpDIR)

            self.scrape_start_date = date.today()
            self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
            self.scrapeTimeGap = 30
            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None