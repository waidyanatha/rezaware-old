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
    import functools
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
        
        self._data = None
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
            name: data @property and @setter functions
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):
        return self._data

    @data.setter
    def data(self,data=None):

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
    def mcextract(func):

        @functools.wraps(func)
        def marketcap_extractor(self,data_owner:str):

            __s_fn_id__ = "function wrapper <marketcap_extractor>"

            __mc_destin_db_name__ = "tip-marketcap"
            __mc_destin_db_coll__ = ''
            __uids__ = ['extract.source.name', # coingeko or coinmarketcap
                        'extract.source.id',   # source provided identifier
                        'asset.symbol',   # crypto symbol
                        'asset.name']     # crypto name

            try:
                logger.info("Begin processing %s data for writing to %s",
                            data_owner,__mc_destin_db_name__)
                _results = func(self,data_owner)

                _mc_dict_list = []
                _mc_coll_name = '.'.join([data_owner,str(date.today())])

                if data_owner == 'coinmarketcap':
                    _extract_dt = _results['status']['timestamp']
                    for _data in _results['data']:
                        _mc_dict_list.append(
                            {
                                "extract.source.id":_data['id'],
                                "extract.source.name":data_owner,
                                "extract.datetime":_extract_dt,
                                "asset.name":_data['name'],
                                "asset.symbol":_data['symbol'],
                                "asset.supply":int(_data['circulating_supply']),
                                "asset.price":float(_data['quote']['USD']['price']),
                                "marketcap.value":float(_data['quote']['USD']['market_cap']),
                                "marketcap.rank":int(_data['cmc_rank']),
                                "marketcap.updated":_data['quote']['USD']['last_updated'],
                            }
                        )
                elif data_owner == 'coingeko':
                    for _data in _results:
                        _mc_dict_list.append(
                            {
                                "extract.source.id":_data['id'],
                                "extract.source.name":data_owner,
                                "extract.datetime":_data['last_updated'],
                                "asset.name":_data['name'],
                                "asset.symbol":_data['symbol'],
                                "asset.supply":int(_data['circulating_supply']),
                                "asset.price":float(_data['current_price']),
                                "marketcap.value":float(_data['market_cap']),
                                "marketcap.rank":int(_data['market_cap_rank']),
                                "marketcap.updated":_data['last_updated'],
                            }
                        )
                else:
                    raise AttributeError("Unrecognized data owner %s" % data_owner)

                logger.info("Appended %d market-cap dicts",len(_mc_dict_list))
                logger.info("Ready to write %d documents to %s",
                            len(_mc_dict_list),__mc_destin_db_name__)
                clsNoSQL.connect={'DBAUTHSOURCE':__mc_destin_db_name__}

                if not __mc_destin_db_name__ in clsNoSQL.connect.list_database_names():
                    raise RuntimeError("%s does not exist",_mc_destin_db_name)

                self._data = clsNoSQL.write_documents(
                    db_name=__mc_destin_db_name__,
                    db_coll=_mc_coll_name,
                    data=_mc_dict_list,
                    uuid_list=__uids__)

                logger.info("Finished writing %s market-cap documents to %s",
                            data_owner,clsNoSQL.dbType)

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                print("[Error]"+__s_fn_id__, err)
                print(traceback.format_exc())

            return self._data, _mc_coll_name

        return marketcap_extractor

    @mcextract
    def get_daily_mc_data(self,data_owner:str):
        
        __s_fn_id__ = "function <get_daily_mc_data>"
        __as_type__ = "list"
        __asset_meta_db_name__ = "tip"
        __asset_meta_db_coll__ = 'datasource.catalog'
        _data_source_list = []
        _collection = None

        try:
            logger.info("Preparing to retrieve %s source metadata from %s database %s collection",
                       data_owner,__asset_meta_db_name__,__asset_meta_db_coll__)
            clsNoSQL.connect={'DBAUTHSOURCE':'tip'}
            _find = {'category':{"$regex":'marketcap'},'owner':{"$regex" : data_owner}}
            _data_source_list = clsNoSQL.read_documents(
                as_type = __as_type__,
                db_name = __asset_meta_db_name__,
                db_coll = __asset_meta_db_coll__, 
                doc_find = _find
            )
            logger.debug("Received %d %s metadata",
                       len(_data_source_list),__asset_meta_db_coll__)

            for _source in _data_source_list:
                _s_api = _source['api']['url']
                headers = {k: v for k, v in _source['api']['headers'].items() if v}
                session = Session()
                session.headers.update(headers)
                parameters = {k: v for k, v in _source['api']['parameters'].items() if v}
                
#                 try:
                response = session.get(_s_api, params=parameters)
                if response.status_code != 200:
                    raise RuntimeError("Exit with %s" % (response.text))

                ''' data found, write to collection '''
                self._data = json.loads(response.text)
                logger.info("Retrieved %d market-cap data with api:%s",
                           len(self._data),_s_api)

#                 except Exception as err:
# #                     print("[Error] ",__s_fn_id__,err)
#                     logger.error("%s %s",__s_fn_id__,err)
#                     print(traceback.format_exc())
#                     pass

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data

    ''' Function
            name: update_crypto_metadata
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def cold_store_daily_mc(
        self,
        from_db_name:str,
        from_db_coll:str,
        to_file_name:str,
        to_folder_path:str,
        **kwargs,   #
    ):

        import json
        from bson.json_util import dumps

        __s_fn_id__ = 'Function <save_data_file>'

        __as_type__ = "list"
        _data_source_list = []
        _collection = None

        try:

            clsRW.storeMode = "google-storage"
            if "STOREMODE" in kwargs.keys():
                clsRW.storeMode = kwargs["STOREMODE"]

            clsRW.storeRoot = "tip-daily-marketcap"   #"rezaware-wrangler-source-code"
            if "STOREROOT" in kwargs.keys():
                clsRW.storeRoot= kwargs["STOREROOT"]

            clsNoSQL.connect = {'DBAUTHSOURCE':from_db_name}
            _data = clsNoSQL.read_documents(
                as_type='DICT',
                db_name = from_db_name,
                db_coll=from_db_coll,
                doc_find={}
            )
            _json_data = json.loads(dumps(_data))

            write_data=clsRW.export_data(to_file_name,to_folder_path,_json_data)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return write_data

