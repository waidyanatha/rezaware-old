#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "CryptoMarket"
__package__ = "etl"
__module__ = "assets"
__app__ = "wrangler"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages '''
    import os
    import sys
    import logging
    import traceback
    import configparser
    ''' function specific python packages '''
    import pandas as pd
    import json
    from datetime import datetime, date, timedelta
    from requests import Request, Session
    from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

    print("All %s-module %s-packages in function-%s imported successfully!"
          % (__module__,__package__,__name__))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__,__package__,__name__,e))


'''
    CLASS spefic to providing reusable functions for scraping ota data
'''

class CryptoMarkets():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="CryptoMarkets Class", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc
        
        self._connection = None
        self._prices = None

        global pkgConf
        global appConf
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

            ''' set app configparser '''
            appConf = configparser.ConfigParser()
            appConf.read(os.path.join(self.appDir, self.__conf_fname__))
            
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
            self.storePath = os.path.join(
                self.__app__,
                "data/",
                self.__module__,
                self.__package__,
            )
            logger.info("%s package files stored in %s",self.__package__,self.storePath)

            ''' import mongo work load utils to read and write data '''
            from utils.modules.etl.load import noSQLwls as nosql
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
            if "WRITE_TO_FILE":
    #             self.tmpDIR = os.path.join(self.rootDir,config.get('STORES','TMPDATA'))
    #             self.tmpDIR = os.path.join(self.dataDir,"tmp/")
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


    ''' Function
            name: update_crypto_metadata
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def update_crypto_metadata(self,url:str=None):
        
        _s_fn_id = "function <import_asset_metadata>"
        __db_name__ = "tip"
        __db_coll_name__ = "crypto.metadata"
        __uids__ = ['symbol','name']
        _collection = None

        try:
            if url is None:
                url = "https://api.coingecko.com/api/v3/coins/list"

            headers = {
                'accepts': 'application/json',
            }

            session = Session()
            session.headers.update(headers)

            parameters = {
                'include_platform':'false'
            }
            response = session.get(url, params=parameters)
            assets = json.loads(response.text)
#             print(assets)

#             market_cap_df = pd.DataFrame(coins)

            ''' initialize the connection using app.cfg propery values '''
            clsNoSQL.connect={}
            _collection = clsNoSQL.write_documents(__db_name__,__db_coll_name__,assets,__uids__)
            
        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _collection


    ''' Function
            name: update_crypto_metadata
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_daily_mc_data(self,data_owner:str=''):
        
        __s_fn_id__ = "function <get_daily_mc_data>"
        __as_type__ = "list"
        __price_source_db_name__ = "tip"
        __price_source_db_coll__ = 'datasource.catalog'
        __asset_meta_db_coll__ = 'datasource.catalog'
        __price_destin_db_name__ = "tip-marketcap"
        __price_destin_db_coll__ = ''
        __price_coll_prefix__ = ''
        __uids__ = ['id','symbol','name']
        _data_source_list = []
        _collection = None

        try:
            clsNoSQL.connect={'DBAUTHSOURCE':'tip-marketcap'}
#             clsNoSQL.dbName = __price_destin_db_name__
            if not __price_destin_db_name__ in clsNoSQL.connect.list_database_names():
                print("%s does not exist" % __price_destin_db_name__)

            clsNoSQL.connect={'DBAUTHSOURCE':'tip'}
            if data_owner is None or data_owner=='':
                ''' get marketcap from all sources '''
                _find = {'category':{"$regex":'marketcap'}}
            else:
                _find = {'category':{"$regex":'marketcap'},'owner':{"$regex" : data_owner}}

            _data_source_list = clsNoSQL.read_documents(
                as_type = __as_type__,
                db_name = __price_source_db_name__,
                db_coll = __asset_meta_db_coll__, 
                doc_find = _find
            )
            for _source in _data_source_list:
                _price_coll = '.'.join([_source['owner'],str(date.today())])
                _s_api = _source['api']['url']
                print(_price_coll, _s_api)
                _data = []
#                 headers=_source['api']['headers']
                headers = {k: v for k, v in _source['api']['headers'].items() if v}
                session = Session()
                session.headers.update(headers)
#                 parameters = {
#                     'localization':'false',
#                 }
                parameters = {k: v for k, v in _source['api']['parameters'].items() if v}
#                 response = session.get(_s_api, params=parameters)
                
                try:
                    response = session.get(_s_api, params=parameters)
#                     print(response.status_code)
                    if response.status_code != 200:
                        raise RuntimeError("Exit with %s" % (response.text))

                    ''' data found, write to collection '''
                    _data = json.loads(response.text)
                    json_object = json.dumps(_data, indent=4)
                    with open('test.json','w') as f:
                        f.write(json_object)
#                     break
#                     clsNoSQL.connect={'DBAUTHSOURCE':__price_destin_db_name__}
#                     _collection = clsNoSQL.write_documents(
#                         db_name=__price_destin_db_name__,
#                         db_coll=_price_coll,
#                         data=_data,
#                         uuid_list=__uids__)

                except Exception as err:
#                     print("No data for %s and date: " % (coin_id, str(date)))
                    print("[Error] ", err)
                    print(traceback.format_exc())
                    pass



        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return _collection

      
    