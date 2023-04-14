#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "CryptoMarket"
__package__ = "etl"
__module__ = "assets"
__app__ = "wrangler"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"
__tmp_data_dir_ext__ = "tmp/"

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
        if desc is None or "".join(desc.split())=="":
            self.__desc__ = " ".join([self.__app__,self.__module__,self.__package__,self.__name__])
        else:
            self.__desc__ = desc
        
        self._data = None
#         self._tmpDir =None   # 
        self._dataDir=None   # asset data storage relative path
        self._tmpDir =None   # asset temporary data storate relative pather
        self._connection = None
        self._prices = None

        __s_fn_id__ = f"{self.__name__} function <__init__>"

        global pkgConf
#         global appConf
        global logger
#         global clsRW
        global clsFile
        global clsNoSQL
        global clsSDB

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,self.__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)
            
            ''' import sparkDBwls to write collections to db '''
            from utils.modules.etl.load import sparkDBwls
            clsSDB = sparkDBwls.SQLWorkLoads(desc=self.__desc__)
            ''' import mongo work load utils to read and write data '''
            from utils.modules.etl.load import sparkNoSQLwls as nosql
            clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)
            from utils.modules.etl.load import sparkFILEwls as spark
            clsFile = spark.FileWorkLoads(desc=self.__desc__)
            clsFile.storeMode=config.get("DATASTORE","MODE")
            clsFile.storeRoot=config.get("DATASTORE","ROOT")

            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
            
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
            logger.info("%s %s Class",__s_fn_id__)

            ''' set the package specific storage path '''
            ''' TODO change to use the utils/FileRW package '''
            self.storePath = pkgConf.get("CWDS","DATA")
    
            logger.info("%s package files stored in %s",self.__package__,self.storePath)

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))
            logger.info("Intantiated with desc: %s",__s_fn_id__,self.__desc__)

#             ''' set the tmp dir to store large data to share with other functions
#                 if self.tmpDIR = None then data is not stored, otherwise stored to
#                 given location; typically specified in app.conf
#             '''
#             self.tmpDIR = None
#             if "WRITE_TO_TMP":
#                 self.tmpDIR = os.path.join(self.storePath,"tmp/")
#                 if not os.path.exists(self.tmpDIR):
#                     os.makedirs(self.tmpDIR)

#             self.scrape_start_date = date.today()
#             self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
#             self.scrapeTimeGap = 30
            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <@data.setter>"

        try:
            if data is None:
                raise AttributeError("Invalid input parameter")
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

        __s_fn_id__ = f"{self.__name__} function <@property dataDir>"

        try:
            if self._dataDir is None:
                self._dataDir = os.path.join(self.__app__,'data',self.__module__,self.__package__)
                logger.warning("%s NoneType dataDIR set to default value %s",
                               __s_fn_id__,self._dataDir.upper())

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

        __s_fn_id__ = f"{self.__name__} function <@dataDir.setter>"

        try:
            if data_dir_path is None or "".join(data_dir_path.split())=="":
                raise AttributeError("Invalid data dir path")
            self._dataDir = data_dir_path
            logger.debug("%s dataDir relative path set to %s",
                         __s_fn_id__,self._dataDir.upper())
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dataDir

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

        __s_fn_id__ = f"{self.__name__} function <@property tmpDir>"

        try:
            if self._tmpDir is None:
                self._tmpDir = os.path.join(self.dataDir,__tmp_data_dir_ext__)
                logger.warning("%s NoneType tmpDIR set to default value %s",
                               __s_fn_id__,self._tmpDir.upper())
            
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

        __s_fn_id__ = f"{self.__name__} function <@tmpDir.setter>"

        try:
            if tmp_dir_path is None or "".join(tmp_dir_path.split())=="":
                raise AttributeError("Invalid tmp dir path")
            self._tmpDir = tmp_dir_path
            logger.debug("%s tmpDir relative path set to %s",
                         __s_fn_id__,self._tmpDir.upper())
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._tmpDir

    ''' Function --- UDATE ASSET METADATA ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def metadata_extractor(func):

        @functools.wraps(func)
        def extractor(self,data_owner:str, **kwargs):

            __s_fn_id__ = f"{self.__name__} function wrapper <metadata_extractor>"

            __def_destin_db_name__ = "tip-asset-metadata"
            __def_destin_coll_name__ = ''
            __uids__ = ['source', # coingeko or coinmarketcap
                        'symbol',   # crypto symbol
                        'name']     # crypto name

            try:
                _results = func(self,data_owner, **kwargs)

                if "DESTINDBNAME" not in kwargs.keys():
                    kwargs["DESTINDBNAME"]=__def_destin_db_name__
                    logger.warning("%s set kwargs DESTINDBNAME to %s",
                                   __s_fn_id__,kwargs["DESTINDBNAME"].upper())                    
#                 else:
#                     _destin_db = __destin_db_name__
                if "DESTINDBCOLL" not in kwargs.keys():
                    kwargs["DESTINDBCOLL"]='.'.join([data_owner,"asset","list"])
                    logger.warning("%s set kwargs DESTINDBCOLL to %s",
                                   __s_fn_id__,kwargs["DESTINDBCOLL"].upper())                    
#                 else:
#                     _destin_coll = '.'.join([data_owner,"asset","list"])

#                 logger.info("%s Begin processing %s data for writing to %s",
#                             __s_fn_id__,data_owner,kwargs["DESTINDBNAME"])

                _asset_dict_list = []
#                 _mc_coll_name = '.'.join([data_owner,"asset","list"])

                if data_owner == 'coinmarketcap':
                    _extract_dt = _results['status']['timestamp']
                    for _data in _results['data']:
                        _asset_dict_list.append(
                            {
                                "source":data_owner,
                                "name":_data['name'],
                                "symbol":_data['symbol'],
                                "lastupdated":_extract_dt,
                                "asset.id":_data['id'],
                                "asset.isactive":_data.get('is_active',1.0),
                                "asset.tokenaddress":_data.get('token_address',None),
                                "asset.platforms":_data.get('platform',None),
                            }
                        )

                elif data_owner == 'coingecko':
                    for _data in _results:
                        _asset_dict_list.append(
                            {
                                "source":data_owner,
                                "name":_data['name'],
                                "symbol":_data['symbol'],
                                "lastupdated":datetime.now(),
                                "asset.id":_data['id'],
                                "asset.isactive":_data.get('is_active',1.0),
                                "asset.tokenaddress":_data.get('token_address',None),
                                "asset.platforms":_data.get('platform',None),
                            }
                        )
                else:
                    raise AttributeError("Unrecognized data owner %s" % data_owner)

                if _asset_dict_list is None or len(_asset_dict_list)<=0:
                    raise RuntimeError("%s No data recovered from %s into asset_dict_list",
                                       __s_fn_id__,data_owner)
                logger.debug("%s Appended %d market-cap data dicts",__s_fn_id__,len(_asset_dict_list))
#                 logger.info("Ready to write %d documents to %s",
#                             len(_asset_dict_list),kwargs["DESTINDBNAME"])
                clsNoSQL.connect={'DBAUTHSOURCE':kwargs["DESTINDBNAME"]}
                if not kwargs["DESTINDBNAME"] in clsNoSQL.connect.list_database_names():
                    raise RuntimeError("%s aborting, db: %s does not exist, "+\
                                       "create db in %s and try again",
                                       __s_fn_id__,kwargs["DESTINDBNAME"],clsNoSQL.dbType)

                self._data = clsNoSQL.write_documents(
                    db_name=kwargs["DESTINDBNAME"],
                    db_coll=kwargs["DESTINDBCOLL"],
                    data=_asset_dict_list,
                    uuid_list=__uids__)
                if self._data is None or len(self._data)<=0:
                    raise RuntimeError("Write documents to db: %s in %s returned %s dtype with no data",
                                       kwargs["DESTINDBNAME"],clsNoSQL.dbType,type(self._data))
                logger.debug("%s Done writing %s market-cap documents to db: %s in %s",
                            __s_fn_id__,data_owner,kwargs["DESTINDBNAME"],clsNoSQL.dbType)

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data, kwargs["DESTINDBCOLL"]

        return extractor

    @metadata_extractor
    def update_asset_metadata(self,data_owner:str, **kwargs):
        
        __s_fn_id__ = f"{self.__name__} function <update_asset_metadata>"

        __def_as_type__ = "list"
        __def_api_db_name__ = "tip-data-sources"
        __def_api_coll_name__ = "marketcap.api"
        __def_api_categoty__ = "asset.metadata"
        _data_source_list = []
        _collection = None

        try:
            if "ASTYPE" not in kwargs.keys():
                kwargs["ASTYPE"]=__def_as_type__
                logger.warning("%s set kwargs ASTYPE to %s",
                               __s_fn_id__,kwargs["ASTYPE"].upper())
            if "APIDBNAME" not in kwargs.keys():
                kwargs["APIDBNAME"]=__def_api_db_name__
                logger.warning("%s set kwargs APIDBNAME to %s",
                               __s_fn_id__,kwargs["APIDBNAME"].upper())
#             else:
#                 _api_db_name = __api_db_name_name__
            if "APICOLLECT" not in kwargs.keys():
                kwargs["APICOLLECT"]=__def_api_coll_name__
                logger.warning("%s set kwargs APICOLLECT to %s",
                               __s_fn_id__,kwargs["APICOLLECT"].upper())
#             else:
#                 _api_collect = __api_collectection__
            if "APICATEGORY" not in kwargs.keys():
                kwargs["APICATEGORY"]=__def_api_categoty__
                logger.warning("%s set kwargs APICATEGORY to %s",
                               __s_fn_id__,kwargs["APICATEGORY"].upper())
#             else:
#                 _api_categoty = __api_categoty__

            logger.info("Preparing to retrieve %s asset metadata from %s database %s collection",
                       data_owner,kwargs["APIDBNAME"],kwargs["APICOLLECT"])
            clsNoSQL.connect={'DBAUTHSOURCE':kwargs["APIDBNAME"]}
            _find = {'category':{"$regex":kwargs["APICATEGORY"]},'owner':{"$regex" : data_owner}}
            _data_source_list = clsNoSQL.read_documents(
                as_type = kwargs["ASTYPE"],
                db_name = kwargs["APIDBNAME"],
                db_coll = kwargs["APICOLLECT"], 
                doc_find = _find
            )
            logger.debug("%s Received %d %s metadata",
                       __s_fn_id__,len(_data_source_list),kwargs["APICOLLECT"])

            for _source in _data_source_list:
                _s_api = _source['api']['url']
                headers = {k: v for k, v in _source['api']['headers'].items() if v}
                session = Session()
                session.headers.update(headers)
                parameters = {k: v for k, v in _source['api']['parameters'].items() if v}

                try:
                    response = session.get(_s_api, params=parameters)
                    if response.status_code != 200:
                        raise RuntimeError("Exit with %s" % (response.text))

    #                 with open("coin_list.json", "w") as outfile:
    #                     outfile.write(response.text)

                    ''' data found, write to collection '''
                    self._data = json.loads(response.text)
                    logger.debug("Retrieved %d coin metadata with api:\n%s",
                               len(self._data),_s_api)

                except Exception as resp_err:
                    logger.warning("%s executing api: %s error: %s",__s_fn_id__,_s_api,resp_err)
                    logger.debug(traceback.format_exc())

            if self._data is None or len(self._data)<=0:
                raise RuntimeError("Response returned %s empty list" % type(self._data))
            logger.debug("%s Returned response list with %s dictionaries",__s_fn_id__,len(self._data))
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' Function
            name: build_api_list
            parameters:

            procedure: Build the url with parameter insertion, headers, and parameter objects
            
            return dict

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def historic_extractor(func):

        @functools.wraps(func)
        def extractor(self,data_owner,from_date,to_date,**kwargs):

            from random import randint
            import random
            import string

            __s_fn_id__ = f"{self.__name__} function wrapper <historic_extractor>"

            __destin_db_name__ = "tip-historic-marketcap"
            __uids__ = ['source',   # coingeko or coinmarketcap
#                         'symbol',   # source provided identifier
                        'name',     # crypto name
                        'mcap.date',# market cap date
                       ]

            try:
                _proc_api_list = func(self,data_owner,from_date,to_date,**kwargs)

                if "DESTINDBNAME" in kwargs.keys():
                    _destin_db = kwargs["DESTINDBNAME"]
                else:
                    _destin_db = __destin_db_name__
                if "DESTINDBAUTH" in kwargs.keys():
                    _destin_db_auth = kwargs["DESTINDBAUTH"]
                else:
                    _destin_db_auth = __destin_db_name__
                if "DESTINDBCOLL" in kwargs.keys():
                    _destin_coll_prefix = kwargs["DESTINDBCOLL"]
                else:
#                     _destin_coll = ".".join([data_owner,str(from_date),str(to_date)])
                    _destin_coll_prefix = ".".join([data_owner,str(from_date),
                                                    "D"+str((to_date-from_date).days)])

                logger.debug("%s Begin processing %s data for writing to %s",
                            __s_fn_id__,data_owner,_destin_db)

                _hmc_dict_list = []
                _failed_assets = []

                clsNoSQL.connect={'DBAUTHSOURCE':_destin_db_auth}
                if not _destin_db in clsNoSQL.connect.list_database_names():
                    raise DatabaseError("%s does not exist",_destin_db)
    
#                 if data_owner == 'coinmarketcap':
#                     print("%s historic data is not free. API to be done")

#                 elif data_owner == 'coingecko':
                for _api in _proc_api_list:
                    time.sleep(randint(5,10))
                    session = Session()
                    session.headers.update(_api['headers'])
                    try:
                        response = session.get(_api['url'], params=_api['parameters'])
                        if response.status_code != 200:
                            _failed_assets.append(_api['symbol'])
                            raise ValueError("%s failed %s" % (_api['id'], response.text))

                        ''' data found, write to collection '''
                        _hmc_data = json.loads(response.text)
                        _coin_hmc_data = []
                        ''' COINGECKO - process response data '''
                        if data_owner.upper() == "COINGECKO":
                            ''' zip market_cap, price, and volume data '''
                            for _mcap, _price, _volume in zip(_hmc_data['market_caps'],
                                                              _hmc_data['prices'],
                                                              _hmc_data['total_volumes']):
                                if float(_mcap[1])<=0 or float(_price[1])<=0:
                                    raise ValueError("%s marketcap = %0.2f asset price = %0.2f"
                                                     %(_api['name'],_mcap[1],_price[1]))
                                ''' replace special char with _ to compy with pyspark imute '''
                                _api['name'] = _api['name'].replace("$","_")\
                                                .replace("%","_").replace("..","_")\
                                                .replace(".","_").replace(",","_")\
                                                .replace("#","_").replace("*","_")\
                                                .replace("!","1").replace("-","_")\
                                                .replace(" ","_").replace("  ","_")\
                                                .replace("(","").replace(")","")\
                                                .replace("[","").replace("]","")\
                                                .replace("__","_")
                                                
                                ''' construct the data dict '''
                                _coin_hmc_data.append(
                                    {
                                        "source":data_owner.lower(),
                                        "id" : _api['id'].lower(),
                                        "symbol" : _api['symbol'].lower(),
                                        "name" :_api['name'].lower(),
                                        "price.value":float(_price[1]),
                                        "price.date": datetime.fromtimestamp(_price[0]/1000),
                                        "price.unix": _price[0],
                                        "mcap.value": float(_mcap[1]),
                                        "mcap.date" : datetime.fromtimestamp(_mcap[0]/1000),
                                        "mcap.unix" : _mcap[0],
                                        "currency" : 'usd',
                                        "volume.size": float(_volume[1]),
                                        "volume.date":datetime.fromtimestamp(_volume[0]/1000),
                                        "volume.unix":_volume[0],
                                    }
                                )
                        else:
                            raise RuntimeError("%s Unavailable and TBD" % data_owner.upper())

                        if len(_coin_hmc_data)>0:
                            if _api['name'] == '' or not _api['name']:
                                # printing lowercase
                                letters = string.ascii_lowercase
                                _api['name']=''.join(random.choice(letters) for i in range(3))

                            _destin_coll = ".".join([_destin_coll_prefix,_api['name']])
                            ''' DEPRECATED bc special characters are replace in _api['name'] above'''
#                             _destin_coll = _destin_coll\
#                                                 .replace("$","_")\
#                                                 .replace("%","_")\
#                                                 .replace("..",".")\
# #                                                 .replace(".","_")\
# #                                                 .replace("..","_")\
#                                                 .replace("#","_")\
#                                                 .replace("*","_")\
#                                                 .replace(" ","_").replace("  ","_")\
#                                                 .replace("[","").replace("]","")\
#                                                 .replace(" (","").replace(")","")

                            _data = clsNoSQL.write_documents(
                                db_name=_destin_db,
                                db_coll=_destin_coll,
                                data=_coin_hmc_data,
                                uuid_list=__uids__)
                            _hmc_dict_list.append(_data)
                            ''' write completed assets to a temporary file '''
                            _comp_asset_fname = "proc_coins_"+_destin_coll_prefix+".txt"
                            _comp_fpath = os.path.join(self.tmpDIR,_comp_asset_fname)
                            with open(_comp_fpath, "a") as compfile:
                                compfile.write(_api['id']+"\n")

                    except Exception as err:
                        logger.warning("%s",err)
                        pass

#                 else:
#                     raise AttributeError("Unrecognized data owner %s" % data_owner)

#                 if len(_hmc_dict_list) > 0:
#                     _data = clsNoSQL.write_documents(
#                         db_name=_destin_db,
#                         db_coll=_destin_coll,
#                         data=_hmc_dict_list,
#                         uuid_list=__uids__)
                logger.info("Appended %d historic marketcap prices to %s collection in %s",
                            len(_hmc_dict_list),_destin_coll,clsNoSQL.dbType)
#                 else:
#                     logger.info("No data retrieved for %s with %d api list",
#                                 data_owner,(_proc_api_list))

#                 logger.info("Finished writing %s market-cap documents to %s",
#                             data_owner,clsNoSQL.dbType)

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return _hmc_dict_list, _failed_assets

        return extractor


    def api_builder(func):

        @functools.wraps(func)
        def builder(self,data_owner,from_date,to_date,**kwargs):

            __s_fn_id__ = f"{self.__name__} function <api_builder>"
            _built_api_list=[]

            try:
                _asset_list, _raw_api_docs = func(self,data_owner,from_date,to_date,**kwargs)
                
                if from_date > to_date:
                    raise ValueError("%s from_date must be <= to_date")

                if data_owner.upper() == "COINGECKO":
                    ''' for each coin get the historic data '''
                    unix_from_date = time.mktime(from_date.timetuple())
                    unix_to_date = time.mktime(to_date.timetuple())
                    logger.debug("%s processing %s from %s (unixtime=%d) to %s (unixtime=%d)",
                                 __s_fn_id__,data_owner.upper(),str(from_date),
                                 unix_from_date,str(to_date),unix_to_date)
                    for _asset in _asset_list:
                        _asset_id = _asset['asset']['id']

                        for _api_doc in _raw_api_docs:
                            _built_api_dict = {}
                            ''' inser id in placeholder'''
                            _s_regex = r"{id}"
                            urlRegex = re.compile(_s_regex, re.IGNORECASE)
                            _s_api = _api_doc['api']['url']
                            param = urlRegex.search(_s_api)
                            if param:
                                _s_api = re.sub(_s_regex, _asset_id, _s_api)
                                _built_api_dict['symbol']=_asset['symbol']
                                _built_api_dict['id']=_asset['asset']['id']
                                _built_api_dict['name']=_asset['name']
                                _built_api_dict['url']=_s_api
                            headers = {k: v for k, v in _api_doc['api']['headers'].items() if v}
                            headers['User-Agent']='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
                            _built_api_dict['headers']=headers
                            parameters = {k: v for k, v in _api_doc['api']['parameters'].items() if v}
#                             parameters['date']=datetime.strftime(from_date,"%d-%m-%Y")
                            parameters['from']=unix_from_date
                            parameters['to']=unix_to_date
                            _built_api_dict['parameters']=parameters
                            _built_api_list.append(_built_api_dict)

                elif data_owner.upper() == "COINMARKETCAP":
                    raise ValueError("Void process, hostoric marketcap data is not free "+ \
                                       "and must have a subscription => standard")
                else:
                    raise RuntimeError("Something was wrong")

                logger.info("Prepared api list with %d set of urls, headers, and parameters.",
                           len(_built_api_list))


            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return _built_api_list

        return builder


    @historic_extractor
    @api_builder
    def extract_historic_mcap(
        self,
        data_owner:str,   # data loading source name coingecko or cmc
        from_date:date,   # start date to extract prices
        to_date:date,     # end date to extract prices
        **kwargs
    ):
        """
        Description:
            Procedure authenticates the NoSQL databases and the collections for retrieving
            the API list and the respective asset list. It uses the utils/etl/load/sparkNoSQLwls 
            package read_documents function to retrieve the API list and the asset list.
            The procedure extends the api_builder and historic_extractor to retrieve the 
            historic market cap data from the respective data owner and listed assets.
        Attributes:
            data_owner (str)- data supplier (e.g. coingecko) 
            from_date (date)- start date to extract historic data
            to_date (date) -  end date to limit the historic data
            **kwargs
                APIDBNAME (str) - database name storing the API lists
                APIDBAUTH (str) - nosql database authentication
                APICOLLECT(str) - collection name containing the API list
                APICATEGORY(str)- historic price and mcap API category
                ASSETS (str) - subset of assets to focus on; else will use all assets
                ASSETDBNAME(str)- database containing the active and inactive asset ids
                ASSETCOLLECT(str)- collection name housing the assets
        Returns:
            _asset_list = list of active assets
            _api_list = data source api list to extract the data
        Exception:
            if **kwargs are not specified, then will use default values
                APIDBNAME is None -  __api_db_name__ = "tip-data-sources"
                APIDBAUTH is None -  _api_db_auth = __api_db_name__
                APICOLLECT is None-  __api_collect__ = 'marketcap.api'
                APICATEGORY is None- __api_categoty__ = 'historic.prices'
                ASSETDBNAME is None- __asset_db_name__ = "tip-asset-metadata"
                ASSETDBAUTH is None- _asset_db_auth = __asset_db_name__
                ASSETCOLLECT is None-__asset_collect__ = f"{data_owner}.asset.list"
            if either _asset_list or _api_list is empty; raise exception and abort
        """
        import time   # to convert datetime to unix timestamp int
        import re

        __s_fn_id__ = f"{self.__name__} function <extract_historic_mcap>"

        __as_type__ = "list"
        __api_db_name__ = "tip-data-sources" # if kwargs[APIDBNAME] is None
        __api_collect__ = 'marketcap.api'    # if kwargs[APICOLLECT] is None
        __api_categoty__ = 'historic.prices' # if kwargs[APICATEGORY] is None
        _api_list = []
        __asset_db_name__ = "tip-asset-metadata"       # if kwargs[ASSETDBNAME] is None
        __asset_collect__ = f"{data_owner}.asset.list" # if kwargs[ASSETCOLLECT] is None
        _asset_list = []
        _collection = None

        try:
#             if from_date > to_date:
#                 raise ValueError("%s from_date must be <= to_date")
            if "APIDBNAME" in kwargs.keys():
                _api_db_name = kwargs["APIDBNAME"]
            else:
                _api_db_name = __api_db_name__
            if "APIDBAUTH" in kwargs.keys():
                _api_db_auth = kwargs["APIDBAUTH"]
            else:
                _api_db_auth = __api_db_name__
            if "APICOLLECT" in kwargs.keys():
                _api_collect = kwargs["APICOLLECT"]
            else:
                _api_collect = __api_collect__
            if "APICATEGORY" in kwargs.keys():
                _api_categoty = kwargs["APICATEGORY"]
            else:
                _api_categoty = __api_categoty__

            if "ASSETS" in kwargs.keys():
                _asset_list = kwargs["ASSETS"]
            if "ASSETDBNAME" in kwargs.keys():
                _asset_db_name = kwargs["ASSETDBNAME"]
            else:
                _asset_db_name = __asset_db_name__
            if "ASSETDBAUTH" in kwargs.keys():
                _asset_db_auth = kwargs["ASSETDBAUTH"]
            else:
                _asset_db_auth = __asset_db_name__
            if "ASSETCOLLECT" in kwargs.keys():
                _asset_collect = kwargs["ASSETCOLLECT"]
            else:
                _asset_collect = __asset_collect__

            ''' get the list of active assets '''
            if len(_asset_list) == 0:
                clsNoSQL.connect={'DBAUTHSOURCE':_asset_db_auth}
                _find = {'source':{"$regex" : data_owner},
                         'asset.isactive':{"$gte":1.0},
                         'asset.type':{"$in":['altcoin','bitcoin']}
                        }
                _asset_list = clsNoSQL.read_documents(
                    as_type = __as_type__,
                    db_name = _asset_db_name,
                    db_coll = _asset_collect, 
                    doc_find = _find
                )

            if not len(_asset_list) > 0:
                raise ValueError("No data found %s in %s db and %s collection for %s"
                                 % (__s_fn_id__,str(_find),_asset_db_name,
                                    _asset_collect,data_owner))
            logger.debug("%s Read %d %s asset names from %s",
                       __s_fn_id__,len(_asset_list),data_owner,_asset_collect)

            ''' get the list of APIs '''
            clsNoSQL.connect={'DBAUTHSOURCE':_api_db_auth}
            _find = {'category':{"$regex":_api_categoty},'owner':{"$regex" : data_owner}}
            _api_list = clsNoSQL.read_documents(
                as_type = __as_type__,
                db_name = _api_db_name,
                db_coll = _api_collect,
                doc_find = _find
            )

            if not len(_api_list) > 0:
                raise ValueError("No API data in %s db and %s collection for %s"
                                 % (_api_db_name,_api_collect,data_owner))
            logger.debug("%s Read %d %s APIs with %s API cateogry from %s",
                       __s_fn_id__,len(_api_list),data_owner,_api_categoty,_api_collect)


        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _asset_list, _api_list


    ''' Function
            name: get_latest_marketcap
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def latest_extractor(func):

        @functools.wraps(func)
        def extractor(self,data_owner:str):

            __s_fn_id__ = f"{self.__name__} function wrapper <latest_extractor>"

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
                elif data_owner == 'coingecko':
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
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data, _mc_coll_name

        return extractor

    @latest_extractor
    def extract_latest_mcap(self,data_owner:str, **kwargs):
#     def get_daily_mc_data(self,data_owner:str, **kwargs):
        
        ''' TODO : use **kwargs to get DB connection parameters '''

        __s_fn_id__ = f"{self.__name__} function <extract_latest_mcap>"

        __as_type__ = "list"
        __asset_meta_db_name__ = "tip-data-sources"
        __asset_meta_db_coll__ = 'marketcap.api'
        __api_categoty__ = coins.metadata
        _data_source_list = []
        _collection = None

        try:
            if "APIDBNAME" in kwargs.keys():
                _api_db_name = kwargs["APIDBNAME"]
            else:
                _api_db_name = __api_db_name__
            if "APIDBAUTH" in kwargs.keys():
                _api_db_auth = kwargs["APIDBAUTH"]
            else:
                _api_db_auth = __api_db_name__
            if "APICOLLECT" in kwargs.keys():
                _api_collect = kwargs["APICOLLECT"]
            else:
                _api_collect = __api_collection__
            if "APICATEGORY" in kwargs.keys():
                _api_categoty = kwargs["APICATEGORY"]
            else:
                _api_categoty = __api_categoty__

            logger.info("Preparing to retrieve %s source metadata from %s database %s collection",
                       data_owner,__asset_meta_db_name__,__asset_meta_db_coll__)
            clsNoSQL.connect={'DBAUTHSOURCE':'tip'}
            _find = {'category':{"$regex":__api_categoty__},'owner':{"$regex" : data_owner}}
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
                
                response = session.get(_s_api, params=parameters)
                if response.status_code != 200:
                    raise RuntimeError("Exit with %s" % (response.text))

                ''' data found, write to collection '''
                self._data = json.loads(response.text)
                logger.info("Retrieved %d market-cap data with api:%s",
                           len(self._data),_s_api)


        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <cold_store_daily_mc>"

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
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return write_data

    ''' Function --- NOSQL-TO-DB ---
    
        TODO: replace pyspark dataframe and sql unfriendly characters
                chars: $ . &# ; ф
        author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def nosql_to_sql(
        self,
        source_db:str="",
        coll_list:list=[],
        destin_db:str="",
        table_name:str="",
        **kwargs
    ):
        """
        Description:
            with spark read all the mcap data from the mongodb colelctions for a given data_owner.
            Filter the data by date range. Construct a spark dataframe
            clean and replace the strings; e.g. $, ., &# etc
        Attributes:
            data_owner (str) - the name of the data source; e.g. coinmarketcap,
            from_date (date) - start date to filter the data by
            to_date (date) - end date to filter the data by
            **kwargs (doct) - for non-existent key/val pairs; will use default values
                            from the app.cfg file 
                "DBNAME" (str) - database name to retrieve data from
                "DBAUTHSOURCE" (str) - autherization database; else try with database name
                "HASINNAME" (str, optional) - matching text to filter collections
                "COLLLIST" (list, optional) - list of collections to consider
                        collection list preceeds over has-in-name 
        Returns:
                self._data (spark dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <nosql_to_sql>"

        __as_type__ = "pandas"
        _find = None
        _colls_list = []
        unique_row_filters = ['MAX','MIN','AVG','MODE','SUM']

        try:
            ''' validate and get collections to loop through each collection '''
            if "".join(source_db.split())=="":
                raise AttributeError("Invalid SOURCE DB NAME")
            clsNoSQL.dbName = source_db

#             if "DBAUTHSOURCE" in kwargs.keys():
#                 clsNoSQL.dbAuthSource = kwargs['DBAUTHSOURCE']
#             else:
#                 clsNoSQL.dbAuthSource = source_db
            if len(coll_list) > 0:
                clsNoSQL.collections = {"COLLLIST":coll_list}
            elif "COLLLIST" in kwargs.keys() and isinstance(kwargs['COLLLIST'],list):
                clsNoSQL.collections = {"COLLLIST":kwargs['COLLLIST']}
            elif "HASINNAME" in kwargs.keys() and isinstance(kwargs['HASINNAME'],str):
                clsNoSQL.collections = {"HASINNAME":kwargs['HASINNAME']}
            else:
                pass
            
            ''' valiate collection list '''
            if len(clsNoSQL.collections)<=0:
                raise ValueError("Unable to locate any collection in %s database"
                                %(clsNoSQL.dbName))
            print("retrieving collections ...")
            _colls_list = clsNoSQL.collections
            logger.debug("%s Found %d collection in %s %s",
                        __s_fn_id__,len(_colls_list),clsNoSQL.dbName,clsNoSQL.dbType)

            ''' assign the find filter '''
            if "FIND" in kwargs.keys() and isinstance(kwargs['FIND'],dict):
                _find = kwargs['FIND']

            ''' set the single asset pick flag '''
            if "UNIQUEROWBY" not in kwargs.keys() or \
                kwargs['UNIQUEROWBY'].upper() not in unique_row_filters:
                kwargs['UNIQUEROWBY']='MAX'
            
            self._data = pd.DataFrame()   # initialize return dataframe
            
            logger.debug("%s begin transfer data from %s %s to %s %s",
                         __s_fn_id__,clsNoSQL.dbType,source_db,clsSDB.dbType,destin_db)
            try:
                ''' loop through collection to get the read data '''
                for _coll in _colls_list:
                    _coll_df = pd.DataFrame()   # initialize the data frame
                    ''' read data from nosql collection as pandas dataframe '''
                    _coll_df = clsNoSQL.read_documents(
                        as_type=__as_type__,
                        db_name=None,      #clsNoSQL.dbName is already set above,
                        db_coll=[_coll],   #one collection at a time to reduce the load
                        doc_find=_find
                    )
                    ''' write to sql db if data exists '''
                    if _coll_df is None or _coll_df.shape[0]<=0:
                        raise RuntimeError("%s collection %s returned an empty dataframe",
                                           __s_fn_id__,_coll)
                    logger.debug("%s read %d documents from collection %s",
                                 __s_fn_id__,_coll_df.shape[0],_coll)

                    ''' rename columns to match the db table '''
                    if "COLUMNSMAP" in kwargs.keys() and isinstance(kwargs['COLUMNSMAP'],dict):
                        _coll_df.rename(columns=kwargs['COLUMNSMAP'],inplace=True)
#                             logger.debug("Column renamed in dataframe %s", str(kwargs['COLUMNSMAP']))
                    ''' remove duplicates '''
                    _coll_df.drop_duplicates(inplace=True)
                    logger.debug("%s After applying drop_duplicates there are %d rows in dataframe",
                                __s_fn_id__,_coll_df.shape[0])
                    ''' format date hh:mm:ss to 00:00:00; support grouping by date '''
                    for _date_val in ['price_date','mcap_date','volume_date']:
                        _coll_df[_date_val]=_coll_df[_date_val]\
                                                .dt.strftime('%Y-%m-%d 00:00:00')
                        _coll_df[_date_val]=pd.to_datetime(_coll_df[_date_val])

                    _coll_df.drop(['price.unix','mcap.unix','volume.unix'],axis=1,inplace=True)

                    ''' Set a single mcap value for each asset '''
                    if kwargs['UNIQUEROWBY']=='MAX':
                        _coll_mcap_df = _coll_df.loc[_coll_df\
                                            .reset_index()\
                                            .groupby(['asset_name','mcap_date'])\
                                            ['mcap_value'].idxmax()]
                        ''' TODO loop through column name tuples to replicate '''
                        _coll_price_df = _coll_df.loc[_coll_df\
                                            .reset_index()\
                                            .groupby(['asset_name','price_date'])\
                                            ['price_value'].idxmax()]
                        _mcap_price_df = pd.merge(_coll_mcap_df,_coll_price_df,
                                            how='inner', left_on='mcap_date', right_on='price_date',
                                                  suffixes=[None,'_1'])
                        _mcap_price_df.drop(['price_value','price_date'],axis=1,inplace=True)
                        _rename_col_dict = {
                            "price_value_1":"price_value",
                            "price_date_1":"price_date",
                        }
                        _mcap_price_df.rename(columns=_rename_col_dict,inplace=True)
                        _1_drop_cols = [x for x in _mcap_price_df.columns if x[-2:] in ['_1']]
                        _mcap_price_df.drop(_1_drop_cols,axis=1,inplace=True)

                        _coll_volume_df = _coll_df.loc[_coll_df\
                                            .reset_index()\
                                            .groupby(['asset_name','volume_date'])\
                                            ['volume_size'].idxmax()]                    
                        _coll_df = pd.merge(_mcap_price_df,_coll_volume_df,
                                            how='inner', left_on='mcap_date', right_on='volume_date',
                                                  suffixes=[None,'_1'])
                        _coll_df.drop(['volume_size','volume_date'],axis=1,inplace=True)
                        _rename_col_dict = {
                            "volume_size_1":"volume_size",
                            "volume_date_1":"volume_date",
                        }
                        _coll_df.rename(columns=_rename_col_dict,inplace=True)
                        _1_drop_cols = [x for x in _coll_df.columns if x[-2:] in ['_1']]
                        _coll_df.drop(_1_drop_cols,axis=1,inplace=True)
                    else:
                        raise AttributeError("Something went wrong with processing %s"
                                             % kwargs['UNIQUEROWBY'])
                    logger.debug("%s Retrieved %d rows with %s mcap_value in dataframe",
                            __s_fn_id__,_coll_df.shape[0],kwargs['UNIQUEROWBY'])

                    ''' augment dataframe with missing table columns '''
#                     _coll_df["asset_name"]=_coll_df["asset_symbol"]
                    _coll_df["created_proc"]="_".join([self.__app__,
                                                       self.__module__,
                                                       self.__package__,
#                                                        self.__name__,
                                                       __s_fn_id__])
                    ''' write dataframe to table with sparksqlwls'''
                    _saved_rec_count=clsSDB.insert_sdf_into_table(
                        save_sdf=_coll_df,
                        db_name =destin_db,
                        db_table=table_name,
                        session_args = kwargs
                    )
                    logger.debug("%s %d records inserted into table %s in database %s",
                                __s_fn_id__,_saved_rec_count,table_name,destin_db)
                    if _saved_rec_count > 0:
                        self._data = pd.concat([self._data,_coll_df])
                    else:
                        raise RuntimeError("None of %s collection %d rows were inserted %s table"
                                           %(_coll,_coll_df.shape[0],table_name))

            except Exception as coll_err:
                logger.error("Collection %s hadd errors: %s \n",_coll,coll_err)
                logger.debug("%s",traceback.format_exc())

            if self._data.shape[0] > 0:
                logger.info("Done transfering %d %s collection from %s database into "+\
                           "%s %s database table %s with %d rows", \
                           len(_colls_list),clsNoSQL.dbType,source_db, \
                           clsSDB.dbType,destin_db,table_name, self._data.shape[0]
                          )
                print('Done processing with %d rows' % self._data.shape[0])

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data
