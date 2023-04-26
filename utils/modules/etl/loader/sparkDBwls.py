#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "sparkDBwls"
__module__ = "etl"
__package__ = "loader"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import configparser    
    import logging
    import functools
    import traceback

    import findspark
    findspark.init()
    from pyspark.sql import functions as F
#     from pyspark.sql.functions import lit, current_timestamp,col,isnan, when, count, countDistinct
    from pyspark.ml.feature import Imputer
    from pyspark.sql import DataFrame
    from typing import List, Iterable, Dict, Tuple
    from psycopg2 import connect, DatabaseError
    from psycopg2.extras import execute_values

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    Class reads and writes data from and to RDBMS using apache spark sql functions
        Currently working with postgresql databases.

    Resources:
        * Notebooks
            * Upsert function evaluation use utils/notebooks/etl/load/sparkDBwls.ipynb
        * Installation guide
            * https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/
        * Acknowledgement
            * Many thanks to Santhanu's medium article and code snipets for generating the
                upsert_sdf_to_db function https://tinyurl.com/pyspark-batch-upsert
'''
class SQLWorkLoads():

    def __init__(self, desc : str="spark workloads",   # identifier for the instances
                 sparkPath:str=None,        # directory path to spark insallation
                 **kwargs:dict,   # can contain hostIP and database connection settings
                ):
        """
        Description:
            Initializes the SQLWorkLoads: class property attributes, app configurations, 
                logger function, data store directory paths, and global classes
        Attributes:
            desc (str) to change the instance description for identification
        Returns:
            None
        """

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        ''' Initialize the DB parameters '''
        self._dbTypeList=['postgresql','mysql']
        self._dbType = None
        self._dbDriver = None
        self._dbHostIP = None
        self._dbPort = None
        self._dbName = None
        self._dbSchema = None
        self._partitions = None
        self._dbUser = None
        self._dbPswd = None
        self._dbConnURL = None

        ''' Initialize spark session parameters '''
        self._homeDir = None
        self._binDir = None
        self._config = None
        self._jarDir = None
        self._appName = None
        self._master = None
        self._rwFormat = None
        self._session = None
        self._saveMode = None

        ''' Initialize property var to hold the data '''
        self._data = None
        
        self._aggList = [
            'sum',
            'avg',
            'stdv',
        ]

        __s_fn_id__ = f"{self.__name__} function <__init__>"

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

            ''' Set the utils root directory '''
            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
            self.dataDir = pkgConf.get("CWDS","DATA")

            appConf = configparser.ConfigParser()
            appConf.read(os.path.join(self.appDir, self.__conf_fname__))

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
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None

    ''' Function --- SPARK DB PROPERTIES ---
            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    ''' --- DB TYPE --- '''
    @property
    def dbType(self) -> str:
        """
        Description: 
            session @property and @setter functions
            @property - if None try __conf_file__; else throw exception
            @setter - if None or Empty throw exception; else set it
        Attributes: 
        Return self._* (* is the property attribute name)
        """

        __s_fn_id__ = f"{self.__name__} function <@property dbType>"

        try:
            if self._dbType is None and appConf.has_option('DATABASE','DBTYPE'):
                self._dbType = appConf.get('DATABASE','DBTYPE')
                logger.debug("%s class property dbType set to: %s",__s_fn_id__,self._dbType)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbType

    @dbType.setter
    def dbType(self,db_type:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbType.setter>"

        try:
            if db_type not in self._dbTypeList:
                raise ConnectionError("Invalid database TYPE %s" % db_type)

            self._dbType = db_type
            logger.debug("%s dbType is: %s",__s_fn_id__,self._dbType)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbType

    ''' --- DRIVER --- '''
    @property
    def dbDriver(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbDriver>"

        try:
            if self._dbDriver is None and appConf.has_option('DATABASE','DBDRIVER'):
                self._dbDriver = appConf.get('DATABASE','DBDRIVER')
                logger.debug("%s class property dbDriver set to: %s",__s_fn_id__,self._dbDriver)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbDriver

    @dbDriver.setter
    def dbDriver(self,db_driver:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbDriver.setter>"

        try:
            if db_driver is None or "".join(db_driver.strip()) == "":
                raise ConnectionError("Invalid database DRIVER %s" % db_driver)

            self._dbDriver = db_driver
            logger.debug("@setter Database dbDriver set to: %s",self._dbDriver)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbDriver

    ''' --- IP --- '''
    @property
    def dbHostIP(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbHostIP>"

        try:
            if self._dbHostIP is None and appConf.has_option('DATABASE','DBHOSTIP'):
                self._dbHostIP = appConf.get('DATABASE','DBHOSTIP')
                logger.debug("%s class property dbHostIP set to: %s",__s_fn_id__,self._dbHostIP)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbHostIP

    @dbHostIP.setter
    def dbHostIP(self,db_host_ip:str='127.0.0.1') -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbHostIP.setter >"

        try:
            if db_host_ip is None or "".join(db_host_ip.strip()) == "":
                raise ConnectionError("Invalid database host IP %s" % db_host_ip)

            self._dbHostIP = db_host_ip
            logger.debug("@setter Database dbHostIP set to: %s",self._dbHostIP)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbHostIP

    ''' --- PORT --- '''
    @property
    def dbPort(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbPort>"

        try:
            if self._dbPort is None and appConf.has_option('DATABASE','DBPORT'):
                self._dbPort = appConf.get('DATABASE','DBPORT')
                logger.debug("%s class property Port set to: %s",__s_fn_id__,self._dbPort)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return str(self._dbPort)

    @dbPort.setter
    def dbPort(self,db_port:int=5432) -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbPort.setter dbPort>"

        try:
            if db_port is None or not isinstance(db_type,int):
                raise ConnectionError("Invalid database port integer %s" % str(db_port))

            self._dbPort = str(db_port)
            logger.debug("@setter Database Port set to: %s",self._dbPort)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPort

    ''' --- NAME --- '''
    @property
    def dbName(self):

        __s_fn_id__ = f"{self.__name__} function <@property dbName>"

        try:
            if self._dbName is None and appConf.has_option('DATABASE','DBNAME'):
                self._dbName = appConf.get('DATABASE','DBNAME')
                logger.debug("@property Database dbName set to: %s",self._dbName)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbName

    @dbName.setter
    def dbName(self,db_name:str=''):

        __s_fn_id__ = f"{self.__name__} function <@dbName.setter>"

        try:
            if db_name is None or "".join(db_name.strip()) == "":
                raise ConnectionError("Invalid database NAME %s" % db_name)

            self._dbName = db_name
            logger.debug("%s class property dbName set to: %s",__s_fn_id__,self._dbName)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbName

    ''' --- SCHEMA --- '''
    @property
    def dbSchema(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbSchema>"

        try:
            if self._dbSchema is None and appConf.has_option('DATABASE','DBSCHEMA'):
                self._dbSchema = appConf.get('DATABASE','DBSCHEMA')
                logger.debug("%s class property dbSchema set to: %s",__s_fn_id__,self._dbSchema)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbSchema

    @dbSchema.setter
    def dbSchema(self,db_schema:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbSchema.setter>"

        try:
            if db_schema is None or "".join(db_schema.strip()) == "":
                raise ConnectionError("Invalid database SCHEMA %s" % db_schema)

            self._dbSchema = db_schema
            logger.debug("@setter Database dbSchema set to: %s",self._dbSchema)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbSchema

    ''' --- USER --- '''
    @property
    def dbUser(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbUser>"

        try:
            if self._dbUser is None and appConf.has_option('DATABASE','DBUSER'):
                self._dbUser = appConf.get('DATABASE','DBUSER')
                logger.debug("%s class property dbUser set to: %s",__s_fn_id__,self._dbUser)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbUser

    @dbUser.setter
    def dbUser(self,db_user:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbPswd.setter>"

        try:
            if db_user is None or "".join(db_user.strip()) == "":
                raise ConnectionError("Invalid database USER %s" % db_user)

            self._dbUser = db_user
            logger.debug("@setter Database dbUser set to: %s",self._dbUser)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbUser

    ''' --- PASSWORD --- '''
    @property
    def dbPswd(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbPswd>"

        try:
            if self._dbPswd is None and appConf.has_option('DATABASE','DBPSWD'):
                self._dbPswd = appConf.get('DATABASE','DBPSWD')
                logger.debug("%s class property dbPswd set to: %s",__s_fn_id__,self._dbPswd)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPswd

    @dbPswd.setter
    def dbPswd(self,db_driver:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@session.setter dbPswd>"

        try:
            if db_driver is None or "".join(db_driver.strip()) == "":
                raise ConnectionError("Invalid database PASSWORD %s" % db_driver)

            self._dbPswd = db_driver
            logger.debug("@setter Database dbPswd set to: %s",self._dbPswd)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPswd

    ''' Function
            name: reset_type to the original data type
            parameters:

            procedure: 
            return self._dbConnURL

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def dbConnURL(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbConnURL>"

        try:
            if self._dbConnURL is None and \
                not self.dbType is None and \
                not self.dbHostIP is None and \
                not self.dbPort is None and \
                not self.dbName is None:
                self._dbConnURL = "jdbc:"+self.dbType+\
                                    "://"+self.dbHostIP+":"+\
                                    self.dbPort+"/"+self.dbName
            logger.debug("%s class property dbConnURL set to: %s",__s_fn_id__,self._dbConnURL)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug("%s",traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbConnURL

    @dbConnURL.setter
    def dbConnURL(self,**kwargs) -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbConnURL.setter>"

        try:
            ''' --- DATABASE PROPERTY **KWARGS --- '''
            if "DBTYPE" in kwargs.keys():
                self.dbType = kwargs['DBTYPE']
            if "DBDRIVER" in kwargs.keys():
                self.dbDriver = kwargs['DBDRIVER']
            if "DBHOSTIP" in kwargs.keys():
                self.dbHostIP = kwargs['DBHOSTIP']
            if "DBPORT" in kwargs.keys():
                self.dbPort = kwargs['DBPORT']
            if "DBNAME" in kwargs.keys():
                self.dbName = kwargs['DBNAME']
            if "DBSCHEMA" in kwargs.keys():
                self.dbSchema = kwargs['DBSCHEMA']
            if "DBUSER" in kwargs.keys():
                self.dbUser = kwargs['DBUSER']
            if "DBPSWD" in kwargs.keys():
                self.dbPswd = kwargs['DBPSWD']

            self._dbConnURL = "jdbc:"+self.dbType+"://"+self.dbHostIP+":"+self.dbPort+"/"+self.dbName
            logger.debug("@dbConnURL.setter Database dbConnURL set to: %s",self._dbConnURL)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug("%s",traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbConnURL

    ''' Function --- SPARK SESSION PROPERTIES ---
            name: session @property and @setter functions
            parameters:

            procedure: 
                @property - if None try __conf_file__; else throw exception
                @setter - if None or Empty throw exception; else set it
            return self._* (* is the property attribute name)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    ''' --- HOMEDIR --- '''
    ''' TODO - check if evn var $SPARK_HOME and $JAVA_HOME is set '''
    @property
    def homeDir(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property homeDir>"

        try:
            if self._homeDir is None and appConf.has_option('SPARK','HOMEDIR'):
                self._homeDir = appConf.get('SPARK','HOMEDIR')
                logger.debug("%s class property homeDir set to: %s",__s_fn_id__,self._homeDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._homeDir

    @homeDir.setter
    def homeDir(self,home_dir:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@homeDir.setter>"

        try:
            if home_dir is None or "".join(home_dir.strip()) == "":
                raise ConnectionError("Invalid spark HOMEDIR %s" % home_dir)

            self._homeDir = home_dir
            logger.debug("@setter Spark homeDir set to: %s",self._homeDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._homeDir

    ''' --- BINDIR --- '''
    @property
    def binDir(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property binDir>"

        try:
            if self._binDir is None and appConf.has_option('SPARK','BINDIR'):
                self._binDir = appConf.get('SPARK','BINDIR')
                logger.debug("%s class property binDir set to: %s",__s_fn_id__,self._binDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._binDir

    @binDir.setter
    def binDir(self,bin_dir:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@binDir.setter>"

        try:
            if bin_dir is None or "".join(bin_dir.strip()) == "":
                raise ConnectionError("Invalid spark BINDIR %s" % bin_dir)

            self._binDir = bin_dir
            logger.debug("@setter Spark binDir set to: %s",self._binDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._binDir

    ''' --- APPNAME --- '''
    @property
    def appName(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property appName>"

        try:
            if self._appName is None or "".join(self._appName.split())=="":
                self._appName = " ".join([self.__app__,
                                          self.__module__,
                                          self.__package__,
                                          self.__name__])
                logger.debug("%s class property appName set to: %s",__s_fn_id__,self._appName)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._appName

    @appName.setter
    def appName(self,app_name:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@appName.setter>"

        try:
            if app_name is None or "".join(app_name.strip()) == "":
                raise ConnectionError("Invalid spark APPNAME %s" % app_name)

            self._appName = app_name
            logger.debug("@setter Spark appName set to: %s",self._appName)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._appName

    ''' --- CONFIG --- '''
    @property
    def config(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property config>"

        try:
            if self._config is None and appConf.has_option('SPARK','CONFIG'):
                self._config = appConf.get('SPARK','CONFIG')
                logger.debug("%s class property Spark config set to: %s",__s_fn_id__,self._config)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._config

    @config.setter
    def config(self,config:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@config.setter>"

        try:
            if config is None or "".join(config.strip()) == "":
                raise ConnectionError("Invalid spark CONFIG %s" % config)

            self._config = config
            logger.debug("@setter Spark config set to: %s",self._config)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._config

    ''' --- JARDIR --- '''
    @property
    def jarDir(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property jarDir>"

        try:
            if self._jarDir is None and appConf.has_option('SPARK','JARDIR'):
                self._jarDir = appConf.get('SPARK','JARDIR')
                logger.debug("%s class property Spark jarDir set to: %s",__s_fn_id__,self._jarDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._jarDir

    @jarDir.setter
    def jarDir(self,jar_dir:str='') -> str:

        __s_fn_id__ = f"{self.__name__} function <@jarDir.setter>"

        try:
            if jar_dir is None or "".join(jar_dir.strip()) == "":
                raise ConnectionError("Invalid spark JARDIR %s" % jar_dir)

            self._jarDir = jar_dir
            logger.debug("@setter Spark jarDir set to: %s",self._jarDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._jarDir

    ''' --- MASTER --- '''
    @property
    def master(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property master>"

        try:
            if self._master is None and appConf.has_option('SPARK','MASTER'):
                self._master = appConf.get('SPARK','MASTER')
                logger.debug("%s class property Spark master set to: %s",__s_fn_id__,self._master)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._master

    @master.setter
    def master(self,master:str='local[1]') -> str:

        __s_fn_id__ = f"{self.__name__} function <@master.setter>"

        try:
            if master is None or "".join(master.strip()) == "":
                self._master = "local[1]"
                logger.warning("SparkSession master set to default %s",self._master)

            self._master = master
            logger.debug("@setter Spark master set to: %s",self._master)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._master

    ''' --- RWFORMAT --- '''
    @property
    def rwFormat(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property rwFormat>"

        try:
            if self._rwFormat is None and appConf.has_option('SPARK','FORMAT'):
                self._rwFormat = appConf.get('SPARK','FORMAT')
                logger.debug("@property Spark rwFormat set to: %s",self._rwFormat)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._rwFormat

    @rwFormat.setter
    def rwFormat(self,rw_format:str='jdbc') -> str:

        __s_fn_id__ = f"{self.__name__} function <@rwFormat.setter>"

        try:
            if rw_format.lower() not in ['jdbc']:
                raise ConnectionError("Invalid spark RWFORMAT %s" % rw_format)

            self._rwFormat = rw_format
            logger.debug("@setter Spark rwFormat set to: %s",self._rwFormat)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._rwFormat


    ''' --- SAVEMODE --- '''
    @property
    def saveMode(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property saveMode>"

        try:
            if self._saveMode is None and appConf.has_option('SPARK','SAVEMODE'):
                self._saveMode = appConf.get('SPARK','SAVEMODE')
                logger.debug("@property Spark saveMode set to: %s",self._saveMode)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._saveMode

    @saveMode.setter
    def saveMode(self,save_mode:str='Append') -> str:

        __s_fn_id__ = f"{self.__name__} function <@saveMode.setter>"

        try:
            if save_mode not in ['Append','Overwrite']:
                raise ConnectionError("Invalid spark SAVEMODE %s" % save_mode)

            self._saveMode = save_mode
            logger.debug("@setter Spark saveMode set to: %s",self._saveMode)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._saveMode


    ''' Function --- SPARK SESSION ---
    
            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def session(self):

        __s_fn_id__ = f"{self.__name__} function <@property session>"

        try:
            if self._session is None and \
                self.homeDir is not None and \
                self.appName is not None and \
                self.config is not None and \
                self.jarDir is not None and \
                self.master is not None:
                findspark.init(self.homeDir)
                from pyspark.sql import SparkSession
                logger.debug("%s importing %s library from spark dir: %s"
                         % (__s_fn_id__,SparkSession.__name__, self.homeDir))

                self._session = SparkSession \
                                .builder \
                                .master(self.master) \
                                .appName(self.appName) \
                                .config(self.config, self.jarDir) \
                                .getOrCreate()
                logger.debug("Nonetype spark session set with homeDir: %s appName: %s "+\
                             "conf: %s jarDir: %s master: %s"
                             ,self.homeDir,self.appName,self.config,self.jarDir,self.master)
                
#             logger.info("Starting a Spark Session: %s",self._session)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug("%s",traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._session

    @session.setter
    def session(self,session_args:dict={}):

        __s_fn_id__ = f"{self.__name__} function <@session.setter>"

        try:
            ''' set the spark home directory '''
            if "HOMEDIR" in session_args.keys():
                self.homeDir = session_args['HOMEDIR']
            findspark.init(self.homeDir)
            from pyspark.sql import SparkSession
            logger.debug("Importing %s library from spark dir: %s"
                         % (SparkSession.__name__, self.homeDir))
            if "CONFIG" in session_args.keys():
                self.config = session_args['CONFIG']
            ''' set master cluster setup local[x], yarn or mesos '''
            if "MASTER" in session_args.keys():
                self.master = session_args['MASTER']    
            if "APPNAME" in session_args.keys():
                self.appName = session_args['APPNAME']  
            ''' set the db_type specific jar '''
            if "JARDIR" in session_args.keys():
                self.jarDir = session_args['JARDIR']

            if self._session:
                self._session.stop
            self._session = SparkSession \
                                .builder \
                                .master(self.master) \
                                .appName(self.appName) \
                                .config(self.config, self.jarDir) \
                                .getOrCreate()
                
            logger.info("Starting a Spark Session: %s for %s"
                        ,self._session, self.dbType)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._session


    ''' Function --- DATA SETTER & GETTER ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):

        __s_fn_id__ = f"{self.__name__} function <@property data>"

        try:
            if not isinstance(self._data,DataFrame):
                self._data = self.session.createDataFrame(self._data)
            if self._data.count() <= 0:
                raise ValueError("No records found in data") 
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):

        __s_fn_id__ = f"{self.__name__} function <@data.setter>"

        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")
            if not isinstance(data,DataFrame):
                self._data = self.session.createDataFrame(data)
            else:
                self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' Function
            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    ''' --- PARTITIONS --- '''
    @property
    def partitions(self):
        """
        Description:
            partition options @property and @setter functions
        Attributes:
        Returns: self._partitions (int)
        """

        __s_fn_id__ = f"{self.__name__} function <@property partitions>"

        try:
            if self._partitions is None and appConf.has_option('SPARK','PARTITIONS'):
                self._partitions = appConf.get('SPARK','PARTITIONS')
                logger.debug("@property Spark PARTITIONS set to: %s",self._partitions)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._partitions

    @partitions.setter
    def partitions(self,num_partitions:int=2):

        __s_fn_id__ = f"{self.__name__} function <@partitions.setter>"

        try:
            if num_partitions <= 0:
                raise ConnectionError("Invalid  %d spark NUMBER of PARTIONS" % num_partitions)

            self._partitions = num_partitions
            logger.debug("@setter Spark PARTIONS set to: %s",self._partitions)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._partitions


    ''' Function

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def read_data_from_table(
        self,
        select:str="",
        db_table:str="",
        db_column:str="",
        lower_bound=None,
        upper_bound=None,
        **options) -> DataFrame:
        """
        Description:
            There is the option of speficing a valid SQL select statement with clauses to
            retrieve the data from one or more tables. The option is to provide table name
        Attributes:
            select (str)
            db_table (str)
        db_column:str="",
        lower_bound=None,
        upper_bound=None,
        **options) -> DataFrame:

        Returns:
        """

        load_sdf = None   # initiatlize return var
        __s_fn_id__ = f"{self.__name__} function <read_data_from_table>"

        try:
            ''' set the partitions '''
            if "PARTITIONS" in options.keys():
                self.partitions = options['PARTITIONS']
            if "FORMAT" in options.keys():
                self.rwFormat = options['FORMAT']
            if "url" not in options.keys():
                options['url'] = self.dbConnURL
            if "numPartitions" not in options.keys():
                options['numPartitions'] = self.partitions
            if "user" not in options.keys():
                options['user'] = self.dbUser
            if "password" not in options.keys():
                options['password'] = self.dbPswd
            if "driver" not in options.keys():
                options['driver'] = self.dbDriver

#             print("Wait a moment, retrieving data ...")
            ''' use query else use partition column'''
            if select is not None and "".join(select.split())!="":
                options['query'] = select
            elif db_table is not None and "".join(db_table.split())!="":
                if db_table.find(self.dbSchema+".") == -1:
                    db_table = self.dbSchema+"."+db_table
                option["dbtable"]=db_table
            else:
                raise AttributeError("Invalid set of input variables necesssary "+\
                                     "to determine the read operation")
            
            load_sdf = self.session.read\
                .format(self.rwFormat)\
                .options(**options)\
                .load()
            
            if load_sdf:
                logger.debug("loaded %d rows into pyspark dataframe" % load_sdf.count())
            else:
                logger.debug("Something went wrong")

            self._data = load_sdf

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    ''' Function --- INSERT INTO TABLE ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def insert_sdf_into_table(
        self,
        save_sdf,
        db_name : str=None,
        db_table: str=None,
        **kwargs):
        
        __s_fn_id__ = f"{self.__name__} function <insert_sdf_into_table>"
        _num_records_saved = 0

        try:
            self.data = save_sdf
            if self.data.count() <= 0:
                raise AttributeError("No data to insert into database table %s"% db_table)
            logger.debug("%s %s dtype data convereted to %s pysaprk dataframe with %d rows %d columns",
                         __s_fn_id__,type(save_sdf),type(self._data),
                         self._data.count(),len(self._data.columns))
            self.dbName=db_name
            if len(kwargs) > 0:
                self.session = kwargs
            else:
                self.session = {}

            ''' TODO validate table exists '''
            
            ''' if created audit columns don't exist add them '''
            listColumns=self._data.columns
            if "created_dt" not in listColumns:
                self._data = self._data.withColumn("created_dt", F.current_timestamp())
            if "created_by" not in listColumns:
                self._data = self._data.withColumn("created_by", F.lit(self.dbUser))
            if "created_proc" not in listColumns:
                self._data = self._data.withColumn("created_proc", F.lit("Unknown"))
            
            ''' TODO: add code to accept options() to manage schema specific
                authentication and access to tables '''

#             if "saveMode" in kwargs.keys():
# #                 self.sparkSaveMode = kwargs['saveMode']
#                 self.sparkSaveMode = kwargs['SAVEMODE']
                
#             logger.info("%s Wait a moment while we insert data int %s",__s_fn_id__,db_table)
            
            self._data.select(self.data.columns).\
                    write.format(self.rwFormat).\
                    mode(self.saveMode).\
                options(
                    url=self.dbConnURL,    # 'jdbc:postgresql://10.11.34.33:5432/Datascience', 
                    dbtable=self.dbSchema+"."+db_table,       # '_issuefix_bkdata.customerbookings',
                    user=self.dbUser,     # 'postgres',
                    password=self.dbPswd, # 'postgres',
                    driver=self.dbDriver).save(self.saveMode.lower())

            _num_records_saved = self._data.count()
            logger.debug("%s Saved %d  rows into table %s in database %s complete!",
                        __s_fn_id__,_num_records_saved,
                         ".".join([self.dbSchema,db_table]), self._dbName)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _num_records_saved

    ''' - Function --- UPDATE TABLE ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def upsert_with_dbtype(func):
        """
        Description:
            wrapper function to upsert dataframe based on self.dbType
        Attribute
            func inherits upsert_sdf_to_table
        returns upsert_dbtype_wrapper
        """
        
        @functools.wraps(func)
        def upsert_wrapper(self,save_sdf,db_table,unique_keys,**options):
            """
            Description:
                Applies the database type specific @static functions to upsert dataframe
            Attributes:
                same as upsert_sdf_to_table input attributes
            Returns:
                processed record count
            """
                
            __s_fn_id__ = f"{self.__name__} function <upsert_wrapper>"
            _num_records_saved = 0
            total_recs_loaded = None   # initiatlize return attr

            try:
                _data, _omitted_cols, _batch_size = func(
                    self,save_sdf,db_table,unique_keys,**options
                )

                if self.dbType == 'postgresql':
                    upsert_query = SQLWorkLoads.build_pgsql_query(
                        cols=_data.schema.names,
                        table_name=self.dbSchema+"."+db_table,
                        unique_key=unique_keys,
                        cols_not_for_update=_omitted_cols,
                    )
#                     print(upsert_query)
                    _db_cred = {
                        "host": self.dbHostIP,
                        "database": self.dbName,
                        "user": self.dbUser,
                        "password": self.dbPswd,
                        "port": self.dbPort,
                    }
                    upsert_stats = _data.coalesce(self.partitions).rdd.mapPartitions(
                        lambda dataframe_partition: SQLWorkLoads.batch_and_upsert(
                            dataframe_partition=dataframe_partition,
                            sql=upsert_query,
                            database_credentials=_db_cred,
                            batch_size=_batch_size,
                        )
                    )
                    total_recs_loaded = 0
                    for counter in upsert_stats.collect():
                        total_recs_loaded += counter

                else:
                    raise RuntimeError("TBD %s dbType upsert; only works for postgresql", self.dbType)

                logger.info("%s Saved %d  rows into table %s in database %s complete!",
                            __s_fn_id__,self.data.count(),
                            self.dbSchema+"."+db_table, self.dbName)
            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return total_recs_loaded

        return upsert_wrapper

    @upsert_with_dbtype
    def upsert_sdf_to_table(
        self,
        save_sdf,       # any dtype data set (will be converted to Dataframe)
        db_table :str,  # name of table to be updated; excluding the schema name
        unique_keys:List[str], # list of columns to use in the where statement 
#         uspert_sql:str="",  # sql update statement
        **options) -> int:
        """
        Description:
            First option is to use the function to generate an update statement, for you,
            using the data columns and the table given; then apply the dynamically generated
            statement string to update the columns.
            Other option is to provid and an update sql statement that can be directly applied;
            e.g., UPDATE <tablename> SET 
            The options and and drivers will be set accordingly.
        Attributes:
            save_sdf (any) dtype that can be converted to a pyspark dataframe
            db_table (str) table name to update rows; excluding schema name
            uspert_sql (str) sql update statement to apply directly
        Returns"
            self._data (DataFrame) 
            cols_not_for_update_ (List) of column names to omit from update
            batch_size_ (int) number of rows to process in each batch
        """
        
        __s_fn_id__ = f"{self.__name__} function <upsert_sdf_to_table>"
        
        cols_not_for_update_=None
        batch_size_ = 1000

        try:
            print("Validating upsert attributes and parameters ...")
            self.data = save_sdf
            if self.data.count() <= 0:
                raise ValueError("No data to update in table")
            if len(options) > 0:
                self.session = options
            else:
                self.session = {}

            ''' TODO validate table exists '''
            
            ''' if created audit columns don't exist add them '''
            listColumns=self.data.columns
            if "modified_dt" not in listColumns:
                self._data = self._data.withColumn("modified_dt", F.current_timestamp())
            else:
                self._data = self._data.withColumn('modified_dt',
                                                   F.when(F.col('modified_dt').isNull(),
                                                          F.current_timestamp()))
            if "modified_by" not in listColumns:
                self._data = self._data.withColumn("modified_by", F.lit(self.dbUser))
            else:
                self._data = self._data.withColumn('modified_by',
                                                   F.when(F.col('modified_by').isNull(),
                                                          F.lit(self.dbUser)))
            _s_mod_proc = "_".join([self.__app__,self.__module__,self.__package__,
                                    self.__name__,__s_fn_id__])
            if "modified_proc" not in listColumns:
                self._data = self._data.withColumn("modified_proc", F.lit(_s_mod_proc))
            else:
                self._data = self._data.withColumn('modified_proc',
                                                   F.when(F.col('modified_proc').isNull(),
                                                          F.lit(_s_mod_proc)))

            ''' TODO: add code to accept options() to manage schema specific
                authentication and access to tables '''

            ''' set all option parameters '''
            if "DBTYPE" in options.keys():
                self.dbType = options['DBTYPE']
            if "PARTITIONS" in options.keys():
                self.partitions = options['PARTITIONS']
            if "OMITCOLS" in options.keys():
                cols_not_for_update_ = options['OMITCOLS']
            if "BATCHSIZE" in options.keys():
                batch_size_ = options['BATCHSIZE']
            if "numPartitions" not in options.keys():
                options['numPartitions'] = self.partitions
            if "user" not in options.keys():
                options['user'] = self.dbUser
            if "password" not in options.keys():
                options['password'] = self.dbPswd
#             if "driver" not in options.keys():
#                 options['driver'] = self.dbDriver

            logger.debug("%s Validation complete with %d rows to process, "+\
                         "%d columns to omit in update, and "+\
                         "applying %d batch size", 
                         __s_fn_id__,self._data.count(), len(cols_not_for_update_), batch_size_)
            print("Wait a moment, writing data to %s %s database ..." 
                  %(self.dbType, self.dbName))
            ''' use query else use partition column '''

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data, cols_not_for_update_, batch_size_


    ''' - Functions --- SPARK POSTGRES BATCH UPSERT ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @staticmethod
    def get_pgsql_conn(
        host: str,     # host name of database instance
        database: str, # name of the database to connect to.
        user: str,     # user name with permissions to access schema and tables.
        password: str, # password for authenticating the user name.
        port: str):    # postgres port to connect; e.g. 5432.
        """
        Description:
            makes us of psycopg2 connection to a postgres database.
        Attributes:
            host (str) host name of database instance.
            database (str) name of the database to connect to.
            user (str) user name with permissions to access schema and tables.
            password (str) password for authenticating the user name.
            port (str) postgres port to connect; e.g. 5432.
        Return (connect) database connection.
        """
        __s_fn_id__ = f"{SQLWorkLoads.__name__} @staticmethod <upsert_sdf_to_table>"

        try:
            conn = connect(
                host=host, database=database,
                user=user, password=password,
                port=port
            )

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return conn

    @staticmethod
    def batch_and_upsert(dataframe_partition: Iterable,
                         sql: str,
                         database_credentials: dict,
                         batch_size: int = 1000):
        """
        Description:
            Batch the input dataframe_partition as per batch_size and upsert to postgres
            using psycopg2 execute values.
        Attributes:
            dataframe_partition (DataFrame/Iterable) Pyspark DataFrame partition or any iterable.
            sql (str): query to insert/upsert the spark dataframe partition to postgres.
            database_credentials (Dict) postgres database credentials.
            Example: database_credentials = {
                    host: <host>,
                    database: <database>,
                    user: <user>,
                    password: <password>,
                    port: <port>
                }
            batch_size (int) size of batch per round trip to database.
        Return (int) with total records processed.
        """

        __s_fn_id__ = f"{SQLWorkLoads.__name__} @staticmethod <batch_and_upsert>"

        conn, cur = None, None
        counter = 0
        batch = []

        for record in dataframe_partition:

            counter += 1
            batch.append(list(record))

            if not conn:
                conn = SQLWorkLoads.get_pgsql_conn(**database_credentials)
                cur = conn.cursor()

            if counter % batch_size == 0:
                execute_values(
                    cur=cur, sql=sql,
                    argslist=batch,
                    page_size=batch_size
                )
                conn.commit()
                batch = []

        if batch:
            execute_values(
                cur=cur, sql=sql,
                argslist=batch,
                page_size=batch_size
            )
            conn.commit()

        if cur:
            cur.close()
        if conn:
            conn.close()

        yield counter

    @staticmethod
    def build_pgsql_query(
        cols: List[str],
        table_name: str,
        unique_key: List[str],
        cols_not_for_update: List[str] = None
    ) -> str:
        """
        Description:
            Builds postgres upsert query using input arguments.
            Example : build_pgsql_query(
                ['col1', 'col2', 'col3', 'col4'],
                "my_table",
                ['col1'],
                ['col2']
            ) ->
            INSERT INTO my_table (col1, col2, col3, col4) VALUES %s  
            ON CONFLICT (col1) DO UPDATE SET (col3, col4) = (EXCLUDED.col3, EXCLUDED.col4) ;
        Attributes:
            cols (List) the postgres table columns required in the insert part of the query.
            table_name (str) the postgres table name; must contain the schema name as a prefix.
            unique_key (List) unique_key of the postgres table for checking unique constraint
                violations.
            cols_not_for_update (List) columns in cols which are not required in the update
                part of upsert query.
        Returns (str) Upsert query as per input arguments.
        """

        __s_fn_id__ = f"{SQLWorkLoads.__name__} @staticmethod <build_pgsql_query>"

        insert_query=""
        on_conflict_clause=""

        try:
            cols_str = ', '.join(cols)

            insert_query = """ INSERT INTO %s (%s) VALUES %%s """ % (
                table_name, cols_str
            )

            if cols_not_for_update is not None:
                cols_not_for_update.extend(unique_key)
            else:
                cols_not_for_update = [col for col in unique_key]

            unique_key_str = ', '.join(unique_key)

            ''' construct update columns removing the ommited columns '''
            update_cols = [col for col in cols if col not in cols_not_for_update]
            update_cols_str = ', '.join(update_cols)
            ''' add prefix EXCLUDED to the cols to be updated '''
            update_cols_with_excluded_markers = [f'EXCLUDED.{col}' for col in update_cols]
            update_cols_with_excluded_markers_str = ', '.join(update_cols_with_excluded_markers)
            ''' constrict the on conflict insertging the keys, cols, and excludes '''
            on_conflict_clause = """ ON CONFLICT (%s) DO UPDATE SET (%s) = (%s) ;""" % (
                unique_key_str,
                update_cols_str,
                update_cols_with_excluded_markers_str
            )

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return insert_query + on_conflict_clause
