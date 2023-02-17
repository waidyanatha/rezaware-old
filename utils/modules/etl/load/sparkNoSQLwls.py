#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "sparkNoSQLwls"
__module__ = "etl"
__package__ = "load"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import functools
    import configparser    
    import logging
    import traceback
    import re
    from pymongo import MongoClient
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import *
    import pandas as pd
    from bson.objectid import ObjectId

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS read and write data to a given location:
        1) local directory
        2) Amazon S3 bucket

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:

'''
class NoSQLWorkLoads():
    ''' Function
            name: __init__
            parameters:
                    @name (str)

            procedure: 

            return DataFrame

    '''
    def __init__(self,
                 desc:str="noSQL DB collection CRUD",
                 **kwargs,
                ):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        ''' --- NoSQL DB properties --- '''
        self._dbHostIP = None
        self._dbPort = None
        self._dbUser = None
        self._dbPswd = None
        self._dbAuthSource = None
        self._dbAuthMechanism = None
#         self._dbtls = None
#         self._dbtlsKeyFile = None
#         self._dbtlsCAFile = None
        
        self._dbType = None
        self._dbTypesList = [
            'mongodb',   # working and tested with community edition v4.4
            'cassandra', # TBD
            'hbase',   # TBD
            'neo4j',   # TBD
            'couchdb', # TBD
        ]
        self._dbName = None
        self._dbFormat = None
        self._collections = None
        self._connect = None
        self._documents = None

        self._asTypeList = [
            'STR',   # text string ""
            'LIST',  # list of values []
            'DICT',  # dictionary {}
            'ARRAY', # numpy array ()
            'SET',   # set of values ()
            'PANDAS', # pandas dataframe
            'SPARK',  # spark dataframe
        ]   # list of data types to convert content to
#         self._docTypeList = [
#             'CSV',   # comma separated value
#             'JSON',  # Javascript object notation
#             'TXT',   # text file
#         ]

        ''' --- SPARK properties --- '''
        self._sparkMaster =  None

        ''' initiate to load app.cfg data '''
        global logger
        global pkgConf
        global appConf
        
        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)
            from rezaware import Logger as logs

            ''' Set the wrangler root directory '''
            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
            self.dataDir = pkgConf.get("CWDS","DATA")

            appConf = configparser.ConfigParser()
            appConf.read(os.path.join(self.appDir, self.__conf_fname__))

            ''' initialize the logger '''
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class",self.__name__)
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))
            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return None

    ''' Functions - list of fget and fset @property and @*.setter functions
            dbHostIP, dbType, dbPort, dbDriver, dbName, 
            dbUser, dbPswd, dbAuthSource, dbAuthMechanism

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._documents

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def dbHostIP(self) -> str:

        __s_fn_id__ = "function @dbHostIP.property"

        try:
            if self._dbHostIP is None and appConf.get('NOSQLDB','DBHOSTIP'):
                self._dbHostIP = appConf.get('NOSQLDB','DBHOSTIP')
#             else:
#                 raise ConnectionError("Undefined hostip; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbHostIP

    @dbHostIP.setter
    def dbHostIP(self,db_host_ip:str = "127.0.0.1"):

        __s_fn_id__ = "function @dbHostIP.setter"

        try:
            if not (db_host_ip is None and db_host_ip==""):
                self._dbHostIP = db_host_ip

#             elif appConf.get('HOSTS','HOSTIP'):
#                 self._dbHostIP = appConf.get('HOSTS','HOSTIP')
            else:
                raise ConnectionError("Undefined hostip; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbHostIP

    ''' DB TYPE '''
    @property
    def dbType(self) -> str:
        
        __s_fn_id__ = "function @dbType.property"

        try:
            if self._dbType is None and appConf.get('NOSQLDB','DBTYPE'):
                self._dbType = appConf.get('NOSQLDB','DBTYPE')
#             else:
#                 raise ConnectionError("Cannot use %s dbType",self._dbType)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbType

    @dbType.setter
    def dbType(self,db_type:str):

        __s_fn_id__ = "function @dbType.setter"
        try:
            if db_type in self._dbTypesList:
                self._dbType = db_type
            elif appConf.get('NOSQLDB','DBTYPE'):
                self._dbType = appConf.get('NOSQLDB','DBTYPE')
            else:
                raise ConnectionError("Undefined dbType; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbType

    ''' DB PORT '''
    @property
    def dbPort(self) -> int:
        
        ___s_fn_id____ = "function @dbPort.property"

        try:
            if self._dbPort is None and appConf.get('NOSQLDB','DBPORT'):
                self._dbPort = appConf.get('NOSQLDB','DBPORT')
#             else:
#                 raise ConnectionError("Undefined dbPort; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",___s_fn_id____, err)
            print("[Error]"+___s_fn_id____, err)
            print(traceback.format_exc())

        return self._dbPort

    @dbPort.setter
    def dbPort(self,db_port=27017) -> int:

        ___s_fn_id____ = "function @dbPort.setter"

        try:
            if isinstance(db_port,int):
                self._dbPort = db_port
#             elif appConf.get('NOSQLDB','DBPORT'):
#                 self._dbPort = appConf.get('NOSQLDB','DBPORT')
            else:
                raise ConnectionError("dbPort must be a valid integer")

        except Exception as err:
            logger.error("%s %s \n",___s_fn_id____, err)
            print("[Error]"+___s_fn_id____, err)
            print(traceback.format_exc())

        return self._dbPort

    ''' DB FORMAT '''
    @property
    def dbFormat(self) -> str:

        __s_fn_id__ = "function @dbFormat.property"

        try:
            if self._dbFormat is None and appConf.get('NOSQLDB','DBFORMAT'):
                self._dbFormat = appConf.get('NOSQLDB','DBFORMAT')
#             else:
#                 raise ConnectionError("Undefined dbFormat; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())


        return self._dbFormat

    @dbFormat.setter
    def dbFormat(self,db_format:str) -> str:

        __s_fn_id__ = "function @dbFormat.setter"

        try:
            if not (db_driver is None and db_format==""):
                self._dbFormat = db_format
#             elif appConf.get('NOSQLDB','DBFORMAT'):
#                 self._dbFormat = appConf.get('NOSQLDB','DBFORMAT')
            else:
                raise ConnectionError("Undefined dbFormat; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbFormat

    ''' DB NAME '''
    @property
    def dbName(self) -> str:

        __s_fn_id__ = "function @dbName.property"

        try:
            if self._dbName is None and appConf.get('NOSQLDB','DBNAME'):
                self._dbName = appConf.get('NOSQLDB','DBNAME')
#             else:
#                 raise ConnectionError("Undefined dbName; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbName

    @dbName.setter
    def dbName(self,db_name:str) -> str:

        __s_fn_id__ = "function @dbName.setter"

        try:
            if not (db_name is None and db_name==""):
                self._dbName = db_name
#             elif appConf.get('NOSQLDB','DBNAME'):
#                 self._dbName = appConf.get('NOSQLDB','DBNAME')
            else:
                raise ConnectionError("Undefined dbName; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbName

    ''' DB USER '''
    @property
    def dbUser(self) -> str:

        __s_fn_id__ = "function @dbUser.setter"

        try:
            if self._dbUser is None and appConf.get('NOSQLDB','DBUSER'):
                self._dbUser = appConf.get('NOSQLDB','DBUSER')
#             else:
#                 raise ConnectionError("Undefined dbUser; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbUser

    @dbUser.setter
    def dbUser(self,db_user:str) -> str:

        __s_fn_id__ = "function @dbUser.setter"
        try:
            if not (db_user is None and db_user==""):
                self._dbUser = db_user
#             elif appConf.get('NOSQLDB','DBUSER'):
#                 self._dbUser = appConf.get('NOSQLDB','DBUSER')
            else:
                raise ConnectionError("Undefined dbUser; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbUser

    ''' DB PASSWORD '''
    @property
    def dbPswd(self) -> str:

        __s_fn_id__ = "function @dbPswd.setter"

        try:
            if self._dbPswd is None and appConf.get('NOSQLDB','DBPSWD'):
                self._dbPswd = appConf.get('NOSQLDB','DBPSWD')
#             else:
#                 raise ConnectionError("Undefined dbPswd; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbPswd

    @dbPswd.setter
    def dbPswd(self,db_pswd:str) -> str:

        __s_fn_id__ = "function @dbPswd.setter"
        try:
            if not (db_pswd is None and db_pswd==""):
                self._dbPswd = db_pswd
#             elif appConf.get('NOSQLDB','DBPSWD'):
#                 self._dbPswd = appConf.get('NOSQLDB','DBPSWD')
            else:
                raise ConnectionError("Undefined dbPswd; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbPswd

    ''' DB AUTHSOURCE '''
    @property
    def dbAuthSource(self) -> str:

        __s_fn_id__ = "function @dbAuthSource.setter"

        try:
            if self._dbAuthSource is None and appConf.get('NOSQLDB','DBAUTHSOURCE'):
                self._dbAuthSource = appConf.get('NOSQLDB','DBAUTHSOURCE')
#             else:
#                 raise ConnectionError("Undefined dbAuthSource; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbAuthSource

    @dbAuthSource.setter
    def dbAuthSource(self,db_auth_source:str) -> str:

        __s_fn_id__ = "function @dbAuthSource.setter"
        try:
            if not (db_auth_source is None and db_auth_source==""):
                self._dbAuthSource = db_auth_source
#             elif appConf.get('NOSQLDB','DBAUTHSOURCE'):
#                 self._dbAuthSource = appConf.get('NOSQLDB','DBAUTHSOURCE')
            else:
                raise ConnectionError("Undefined dbAuthSource; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbAuthSource

    ''' DB MECHANISM '''
    @property
    def dbAuthMechanism(self) -> str:

        __s_fn_id__ = "function @dbAuthMechanism.setter"

        try:
            if self._dbAuthMechanism is None and appConf.get('NOSQLDB','DBAUTHMECHANISM'):
                self._dbAuthMechanism = appConf.get('NOSQLDB','DBAUTHMECHANISM')
#             else:
#                 raise ConnectionError("Undefined dbAuthMechanism; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbAuthMechanism

    @dbAuthMechanism.setter
    def dbAuthMechanism(self,db_auth_mechanism:str) -> str:

        __s_fn_id__ = "function @dbAuthMechanism.setter"
        try:
            if not (db_auth_mechanism is None and db_auth_mechanism==""):
                self._dbAuthMechanism = db_auth_mechanism
#             elif appConf.get('NOSQLDB','DBAUTHMECHANISM'):
#                 self._dbAuthMechanism = appConf.get('NOSQLDB','DBAUTHMECHANISM')
            else:
                raise ConnectionError("Undefined dbAuthMechanism; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbAuthMechanism

    ''' SPARK MASTER '''
    @property
    def sparkMaster(self) -> str:

        __s_fn_id__ = "function @sparkMaster.setter"

        try:
            if self._sparkMaster is None and appConf.get('SPARK','MASTER'):
                self._sparkMaster = appConf.get('SPARK','MASTER')

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._sparkMaster

    @sparkMaster.setter
    def sparkMaster(self,spark_master:str) -> str:

        __s_fn_id__ = "function @sparkMaster.setter"
        try:
            if not (spark_master is None and spark_master==""):
                self._sparkMaster = spark_master
#             elif appConf.get('NOSQLDB','DBAUTHMECHANISM'):
#                 self._dbAuthMechanism = appConf.get('NOSQLDB','DBAUTHMECHANISM')
            else:
                raise ConnectionError("Undefined sparkMaster; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._sparkMaster

    
    ''' Function - connect

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._connect

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def connect(self):

        try:
            if self._connect is None and \
                self.dbHostIP and \
                self.dbAuthSource and \
                self.dbUser and \
                self.dbPswd and \
                self.dbAuthSource and \
                self.dbAuthMechanism:
                if self.dbType.lower() == 'mongodb':
#                     self._connect = MongoClient(
#                         _db_host_ip,
#                         username=_db_user,
#                         password=_db_pswd,
#                         authSource=_db_auth,
#                         authMechanism=_db_mech
                    self._connect = MongoClient(
                        self.dbHostIP,
                        username=self.dbUser,
                        password=self.dbPswd,
                        authSource=self.dbAuthSource,
                        authMechanism=self.dbAuthMechanism
                    )
                    logger.warning("Non-type connection %s set using existing properties"
                                   ,self._connect)
                elif self.dbType.lower() == 'cassandra':
                    raise RuntimError("cassandra is to be included in a future release")
                else:
                    raise ValueError("Undefined dbType. It must be one of %s" % self._dbTypeList)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._connect

    @connect.setter
    def connect(self,connect_properties:dict={}):

        _db_host_ip=None
        _db_user=None
        _db_pswd=None
        _db_auth=None
        _db_mech=None
        __s_fn_id__ = "function @connect.setter"

        try:
            ''' check if properties in args or config file are defined '''
            if not (len(connect_properties) > 0 or "NOSQLDB" in appConf.sections()):
                raise TypeError("Input args in %s and NOSQLDB section in %s undefined"
                                % (__s_fn_id__,self.__conf_fname__))
            ''' check and set DBHOSTIP from args or app config '''
            if "DBHOSTIP" in connect_properties.keys():
                _db_host_ip = connect_properties['DBHOSTIP']
            elif appConf.get('NOSQLDB','DBHOSTIP'):
                _db_host_ip = appConf.get('NOSQLDB','DBHOSTIP')
            else:
                raise ValueError("Undefined DBHOSTIP in function args and app config file. aborting")

            ''' check and set DBTYPE '''
            if "DBTYPE" in connect_properties.keys():
                self.dbType = connect_properties['DBTYPE']
            elif appConf.get('NOSQLDB','DBTYPE'):
                self.dbType = appConf.get('NOSQLDB','DBTYPE')
            else:
                raise ValueError("Undefined DBTYPE in function args and app config file. aborting")

            ''' check and set DBUSER from args or app config '''
            if "DBUSER" in connect_properties.keys():
                _db_user = connect_properties['DBUSER']
            elif appConf.get('NOSQLDB','DBUSER'):
                _db_user = appConf.get('NOSQLDB','DBUSER')
            else:
                raise ValueError("Undefined DBUSER in function args and app config file. aborting")

            ''' check and set DBPSWD from args or app config '''
            if "DBPSWD" in connect_properties.keys():
                _db_pswd = connect_properties['DBPSWD']
            elif appConf.get('NOSQLDB','DBPSWD'):
                _db_pswd = appConf.get('NOSQLDB','DBPSWD')
            else:
                raise ValueError("Undefined DBPSWD in function args and app config file. aborting")

            ''' check and set DBAUTHSOURCE from args or app config '''
            if "DBAUTHSOURCE" in connect_properties.keys():
                _db_auth = connect_properties['DBAUTHSOURCE']
            elif not self.dbName is None:
                _db_auth = self.dbName
                logger.warning("Unspecified DBAUTHSOURCE try with authSource = dbName")
            elif appConf.get('NOSQLDB','DBAUTHSOURCE'):
                _db_auth = appConf.get('NOSQLDB','DBAUTHSOURCE')
                logger.warning("Trying db auth source with %s value",self.__conf_fname__)
            else:
                raise ValueError("Undefined DBAUTHSOURCE in function args and app config file. aborting")

            ''' check and set DBAUTHMECHANISM from args or app config '''
            if "DBAUTHMECHANISM" in connect_properties.keys():
                _db_mech = connect_properties['DBAUTHMECHANISM']
            elif appConf.get('NOSQLDB','DBAUTHMECHANISM'):
                _db_mech = appConf.get('NOSQLDB','DBAUTHMECHANISM')
            else:
                raise ValueError("Undefined DBAUTHMECHANISM in function args and app config file. aborting")

            ''' initialize noSQLdbconnect '''
            if self.dbType.lower() == 'mongodb':
                self._connect = MongoClient(
                    _db_host_ip,
                    username=_db_user,
                    password=_db_pswd,
                    authSource=_db_auth,
                    authMechanism=_db_mech
                )
                logger.debug(self._connect)
            elif self.dbType.lower() == 'cassandra':
                raise RuntimError("cassandra is to be included in a future release")
            else:
                raise ValueError("Undefined dbType. It must be one of %s" % self._dbTypeList)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._connect

    ''' Function - collection

            parameters:

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def collections(self) -> list:

        ___s_fn_id____ = "function <collections.setter>"

        try:
            if self._collections is None and self.dbName and self.dbAuthSource:
                if self.dbType.lower() == 'mongodb':
                    db = self.connect[self.dbName]
                    self._collections = db.list_collection_names()
                elif self.dbType.lower() == 'cassendra':
                    print('TBD')
                else:
                    raise AttributeError('Something was wrong')
                    
        except Exception as err:
            logger.error("%s %s \n",___s_fn_id____, err)
            print("[Error]"+___s_fn_id____, err)
            print(traceback.format_exc())

        return self._collections

    @collections.setter
    def collections(self, collection_properties:dict={}) -> list:
        
        ___s_fn_id____ = "function <collections.setter>"
        _coll_list=[]

        try:
            ''' set the dbName if specified '''
            if "DBNAME" in collection_properties.keys():
                self.dbName = collection_properties['DBNAME']
#             if self.dbName is None:
#                 raise AttributeError("Database name must be specified")
            ''' set the dbType if specified '''
            if "DBTYPE" in collection_properties.keys():
                self.dbType = collection_properties['DBTYPE'].lower()
            if "DBAUTHSOURCE" in collection_properties.keys():
                self.dbAuthSource = collection_properties['DBAUTHSOURCE']
                self.connect = {'DBAUTHSOURCE':collection_properties['DBAUTHSOURCE']}
            else:
                self.dbAuthSource = self.dbName
                self.connect = {'DBAUTHSOURCE':self.dbName}
            
            if self.dbType.lower() == 'mongodb':
                db = self.connect[self.dbName]
                _coll_list = db.list_collection_names()
            ''' select collections with specified regex '''
            if "COLLLIST" in collection_properties.keys() and len(_coll_list)>0:
                self._collections = list(filter(lambda _coll: 
                                                _coll in collection_properties['COLLLIST'],
                                                _coll_list
                                               ))
            elif "HASINNAME" in collection_properties.keys() and len(_coll_list)>0:
                r = re.compile(f".*{collection_properties['HASINNAME']}*")
                self._collections = list(filter(r.match, _coll_list))
            else:
                self._collections = _coll_list

        except Exception as err:
            logger.error("%s %s \n",___s_fn_id____, err)
            print("[Error]"+___s_fn_id____, err)
            print(traceback.format_exc())

        return self._collections


    ''' Function - data

            parameters:
                
            return (dtype) any data type: str, dict, list, dataframe, array, and so on

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def documents(self):
        return self._documents

    @documents.setter
    def documents(self, docMeta:dict):
    
        __s_fn_id__ = "function @data.setter"

        _asType = None  # mandatory - data convertion type from store dict
        _dbName = None  # database name to query collections
        _dbColl = None   # mandatory - file path from store dict
        _docFind = None   # either - file name from store dict
#         _docType = None   # or - file type from store dict

        try:
            if ("ASTYPE" in [x.upper() for x in docMeta.keys()]) \
                and (docMeta['ASTYPE'].upper() in self._asTypeList):
                _asType = docMeta['ASTYPE']
            else:
                ''' set asTpe to a dict by daefulat '''
                _asType = "DICT"

            if not ("DBNAME" in [x.upper() for x in docMeta.keys()]):
                raise ValueError("Missing DBNAME and must be specified")
            _dbName = docMeta['DBNAME']

            ''' if not specified will return data from all collections '''
            if "COLLECTION" in [x.upper() for x in docMeta.keys()]:
                _dbColl = docMeta['COLLECTION']

            ''' if not specified will return data for all documents '''
            if "FIND" in [x.upper() for x in docMeta.keys()]\
                and isinstance(docMeta['FIND'],dict):
                _docFind = docMeta['FIND']
#             elif ("DOCTYPE" in [x.upper() for x in docMeta.keys()]) \
#                 and (docMeta['DOCNAME'].upper() in self._docTypeList):
#                 _docType = docMeta['DOCTYPE']
#             else:
#                 raise ValueError("Either a DOCNAME or DOCTYPE must be specified")

            self._documents = self.read_documents(
                as_type=_asType,
                db_name=_dbName,
                db_coll=_dbColl,
                doc_find=_docFind,
#                 doc_type=_fType,
            )
            logger.debug("%s execute find {%s} from %s",__s_fn_id__,str(_docFind), _dbColl)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._documents


    ''' Function - read_doc

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._documents

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def converter(func):

        @functools.wraps(func)
        def wrapper_converter(self,
                 as_type:str,   # mandatory - define the data type to return
                 db_name:str,
                 db_coll:str,      # mandatory - relative path, w.r.t. self.storeRoot
                 doc_find:dict={},   # optional - name of the file to read
#                  doc_type:str=None    # optional - read all the files of same type
                  **kwargs,
                ):

            _the_docs = func(self,as_type,db_name,db_coll,doc_find, **kwargs)

            if as_type.upper() == 'DICT':
                self._documents = list(_the_docs)
            elif as_type.upper() == 'STR':
                self._documents=' '.join(list(_the_docs))
            elif as_type.upper() == 'PANDAS':
#                 tmp_df = pd.DataFrame()
#                 for _docs in _the_docs:
#                     tmp_df = pd.concat([tmp_df,pd.DataFrame(_docs)])
# #                     logger.debug("tmp_df type %s",type(tmp_df))
#                 self._documents=tmp_df
                self._documents=pd.DataFrame(_the_docs)
                if "_id" in self._documents.columns:
                    self._documents['_id'] = self._documents['_id'].astype('str')
            elif as_type.upper() == 'SPARK':
                self._documents=_the_docs
#                 print("pandas",type(self._documents))
            else:
                ''' dtype unspecified return as dictionary '''
                self._documents=list(_the_docs)

            return self._documents

        return wrapper_converter

    @converter
    def read_documents(
        self,
        as_type:str="",
        db_name:str="",
        db_coll:list=[],
        doc_find:dict={},
        **kwargs):

        __s_fn_id__ = "function <read_documents>"
        _appName = " ".join([
            self.__app__,
            self.__name__,
            self.__package__,
            self.__module__
        ])   # spark app name

        doc_list = None
        doc_dics = None
        _docs_sdf = None

        try:
            if db_name:
                self.dbName = db_name
            if len(db_coll)>0:
                self.collections={"COLLLIST":db_coll}
            if doc_find is None:
                doc_find = {}

            logger.debug("Prepared to read documents from "+\
                         "database %s, collection %s with %s find condition"\
                         ,self.dbName,self.collections,doc_find)

            if self.dbType.lower() == 'mongodb':
                ''' get data from MongoDB collection '''
                db = self.connect[self.dbName]
                _coll_list = db.list_collection_names()
                logger.debug("%s database has %d collections",self.dbName,len(_coll_list))
                if self.collections:
#                     logger.debug("Filtering collections by %s",str(self.collections))
                    self._collections = list(filter(lambda _coll: 
                                                    _coll in self.collections, 
                                                    _coll_list
                                                   ))
                else:
                    logger.debug("No filters appied collections are %s",str(self.collections))
                    self._collections = _coll_list
                logger.debug("Filtered set of collection %s", str(self.collections))

                ''' read data from all the collections '''
                if as_type.upper() == "SPARK":
                    ''' read with spark '''
#                     spark = SparkSession.builder.appName(_appName).getOrCreate()
#                     empty_rdd = spark.sparkContext.emptyRDD()
#                     _docs_sdf = spark.createDataFrame(data=empty_rdd,schema=StructType([]))
#                     _docs_sdf = spark.emptyDataFrame()
#                     _inp_uri = f"{self.dbType}://"+\
#                                 f"{self.dbUser}:"+\
#                                 f"{self.dbPswd}@"+\
#                                 f"{self.dbHostIP}/"+\
#                                 f"{self.dbName}."
                    for _coll_idx,_coll in enumerate(self.collections):
                        try:
                            _inp_uri = f"{self.dbType}://"+\
                                        f"{self.dbUser}:"+\
                                        f"{self.dbPswd}@"+\
                                        f"{self.dbHostIP}/"+\
                                        f"{self.dbName}."+\
                                        f"{_coll}"+ \
                                        f"?authSource={self.dbAuthSource}"
#                             _inp_uri = _inp_uri + f"{_coll}" + f"?authSource={self.dbAuthSource}"
                            logger.debug("input uri: %s",_inp_uri)
                            
                            # Create Spark session
                            spark = SparkSession.builder \
                                .appName(_appName) \
                                .master(self.sparkMaster) \
                                .config("spark.mongodb.input.uri", _inp_uri) \
                                .getOrCreate()

                            sdf = spark.read.format(self.dbFormat)\
                                .option( "uri", _inp_uri)\
                                .load()

                            if _coll_idx == 0:
                                _docs_sdf = sdf.alias('_docs_sdf')
                            else:
                                _old_docs_sdf = _docs_sdf.alias('_docs_sdf')
                                _docs_sdf = _old_docs_sdf.unionByName(sdf)
                                logger.debug("%s",str(_docs_sdf.head(10)))
                            logger.debug("Union sdf size %d",_docs_sdf.count())

                        except Exception as err:
                            logger.warning("collection: %s in database: %s had errors: %s \n",
                                           _coll, self.dbName, err)
                            logger.error(traceback.format_exc())
                            pass

                    if (not _docs_sdf is None) and (_docs_sdf.count() > 0):
                        doc_list=_docs_sdf

                    logger.info("Loaded %d documents from %d collections",
                                doc_list.count(),len(self.collections))
                else:
                    ''' read with pymongo '''
                    doc_list=[]
                    for _coll in self.collections:
                        try:
                            logger.debug("Find %s in %s collection",doc_find,_coll)
                            _coll_cur = db[_coll].find(doc_find)
                            if len(list(_coll_cur.clone())) <=0:
                                raise ValueError("No data")
                            doc_list.extend(list(_coll_cur.clone()))

                        except Exception as err:
                            logger.warning("collection: %s in database: %s had errors: %s \n",
                                           _coll, self.dbName, err)
                            pass

                    logger.info("Loaded %d documents from %d collections",
                                len(doc_list),len(self.collections))

            elif self.dbType.lower() == 'cassandra':
                ''' get data from Cassandra collection '''
                raise RuntimeError("cassandra read is tbd")
            else:
                raise ValueError("Something was wrong")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return doc_list

    ''' Function - write collection

            parameters:

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def createDocs(func):

        @functools.wraps(func)
        def create_documents(
            self,
            db_name:str,   # optional - name of the file to read
            db_coll:str,   # mandatory - relative path, w.r.t. self.storeRoot
            data,   # data to be stored
            uuid_list,
        ):
            
            if isinstance(data,list):
                ''' list to collection '''
                self._documents = data
            elif isinstance(data,dict):
                self._documents = [data]
            elif isinstance(self.documents,pd.DataFrame):
                ''' dataframe to collection '''
                self._documents = [self.documents.to_dict()]
            else:
                raise TypeError("Unrecognized data type %s must be either of\n%s"
                                % (type(self.documents),str(self._asTypeList)))

            _collection=func(self,
                             db_name,
                             db_coll,
                             data,
                             uuid_list
                            )

#             if self.dbType.lower == 'mongodb':
#                 _insert_ids = _collection.insert_many(self.documents)
                
#             elif self.dbType.lower() == 'cassandra':
#                 ''' get data from cassandra collection '''
#                 raise RuntimeError("cassandra write is tbd")
#             else:
#                 raise ValueError("Something was wrong")

            return self._documents 
        return create_documents

    @createDocs
    def write_documents(
        self,
        db_name:str,   # optional - name of the file to read
        db_coll:str,   # mandatory - relative path, w.r.t. self.storeRoot
        data=None,   # data to be stored
        uuid_list:list=[],   # unique identifier name to check if document exists
    ):

        __s_fn_id__ = "function <write_data>"
        _collection = None, 
        _objIds = None

        try:
            logger.debug("Writing document to %s",self.dbType)
            db = self.connect[db_name]
            
            ''' check if collection exists; else create one '''
            if self.dbType.lower() == 'mongodb':
                ''' get data from MongoDB collection '''
                if not db_coll in db.list_collection_names():
                    _collection = db[db_coll]
                    logger.info("Created a new collection %s",_collection)
                    ''' insert all the documents '''
                    results = db[db_coll].insert_many(self.documents)
                    logger.info("Inserted %d documents",db[db_coll].count_documents({}))
                else:
                    _insert_count = 0
                    _modify_count = 0
                    for data_dict in self.documents:
#                         ''' add ObjectIds if not in data '''
#                         if '_id' not in data_dict.keys() \
#                             or data_dict['_id'] is None \
#                             or data_dict['_id']=="":
#                             new_id = ObjectId()
#                             while not db[db_coll].find({"_id": {"$eq": ObjectId(new_id)}}):
#                                 new_id = ObjectId()
#                             data_dict['_id']=ObjectId(new_id)

                        ''' find and update on the key list '''
                        if not (len(uuid_list) > 0):
#                             _filter_dict = {}
#                             for _key in uuid_list:
#                                 _filter_dict[_key]=data_dict[_key]
                            uuid_list = list(data_dict.keys())
#                         else:
#                             uuid_list = data_dict.keys()

#                         print(uuid_list)
                        _filter_dict = {}
                        for _uuid in uuid_list:
                            _filter_dict[_uuid]=data_dict[_uuid]
#                         print(_filter_dict)
                        _filtered_cur = db[db_coll].find(_filter_dict)
#                         for x in _filtered_cur.clone():
#                             print(x)
                        ''' if no match then insert with new ObjectID '''
                        if len(list(_filtered_cur.clone())) <=0:
                            ''' add ObjectIds if not in data '''
                            doc = db[db_coll].insert_one(data_dict)
                            _insert_count += 1
#                             print('insert count:',doc.inserted_count)
#                             logger.info("Modified %d documents",doc.inserted_count)
#                             if '_id' not in data_dict.keys() \
#                                 or data_dict['_id'] is None \
#                                 or data_dict['_id']=="":
# #                                 _filtered_cur = data_dict
#                                 new_id = ObjectId()
#                                 while not db[db_coll].find({"_id": {"$eq": ObjectId(new_id)}}):
#                                     new_id = ObjectId()
#                                 data_dict['_id']=ObjectId(new_id)
# #                                 _filtered_cur['_id']=ObjectId(new_id)
#                             ''' insert the new data '''
#                             _filtered_cur.append(data_dict)
#                             print('inserted',doc)
                        else:
                            ''' update all matching documents '''
                            for _cur in _filtered_cur:

                                ''' update with existing ObjectId '''
#                                 doc = db[db_coll].update_one({'_id':_cur['_id']},{"$set": data_dict})
                                doc = db[db_coll].update_one(
                                    filter={
                                        '_id' : _cur['_id'],
                                    },
                                    update={
    #                                             '$setOnInsert': data_dict,
                                        '$set': data_dict,
                                    },
                                    upsert=True,
                                )
                                _modify_count += 1
                    print('Total %d documents, successful insert count = %d & modify count = %d'
                          %(len(self.documents),_insert_count, _modify_count))
                    logger.info("Total %d documents, successful insert count = %d & modify count = %d",
                                len(self.documents),_insert_count, _modify_count)

            elif self.dbType.lower() == 'cassandra':
                ''' get data from cassandra collection '''
                raise RuntimeError("cassandra write is tbd")
            else:
                raise ValueError("Something was wrong")
            


        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return db[db_coll]


    ''' convert to dict '''
    def convert_2_dict_mongodb(obj):
        result = {}
        for key, val in obj.items():
            if not isinstance(val, dict):
                result[key] = val
                continue

            for sub_key, sub_val in val.items():
                new_key = '{}.{}'.format(key, sub_key)
                result[new_key] = sub_val
                if not isinstance(sub_val, dict):
                    continue

                result.update(convert_2_dict_mongodb(result))
                if new_key in result:
                    del result[new_key]

        return result