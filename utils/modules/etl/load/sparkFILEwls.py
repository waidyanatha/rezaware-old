#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "FileWorkLoads"
__module__ = "etl"
__package__ = "load"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import configparser    
    import logging
    import traceback
    import functools
    import findspark
    findspark.init()
#     from pyspark.sql.functions import split, col,substring,regexp_replace, lit, current_timestamp
    from pyspark.sql.functions import lit, current_timestamp
#     from pyspark import SparkContext, SparkConf
    from pyspark.sql import DataFrame
    import boto3   # handling AWS S3 
    from google.cloud import storage   # handles GCS reads and writes
#     import pandas as pd
#     import numpy as np

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS create, update, and migrate databases using sql scripts
        1) 

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:
        https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/
'''
class FileWorkLoads():
    ''' Function
            name: __init__
            parameters:
                    @name (str)
                    @enrich (dict)
            procedure: 
            return None
            
            author: <nuwan.waidyanatha@rezgateway.com>

    '''
    def __init__(self, desc : str="spark workloads",   # identifier for the instances
                 **kwargs:dict,   # can contain hostIP and database connection settings
                ):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

#         self._dType = None
#         self._dTypeList = [
#             'RDD',     # spark resilient distributed dataset
#             'SDF',     # spark DataFrame
#             'PANDAS',  # pandas dataframe
#             'ARRAY',   # numpy array
#             'DICT',    # data dictionary
#         ]

        ''' Initialize property var to hold the data '''
        self._data = None
        self._storeMode = None
        self._storeModeList = [
            'local-fs',     # local hard drive on personal computer
            'aws-s3-bucket', # cloud amazon AWS S3 Bucket storage
            'google-storage',   # google cloud storage buckets
        ]
        self._storeRoot = None   # holds the data root path or bucket name
        self._storeData = None
        self._asType = None
        self._asTypeList = [
            'str',   # text string ""
            'list',  # list of values []
            'dict',  # dictionary {}
            'array', # numpy array ()
            'set',   # set of values ()
            'pandas', # pandas dataframe
            'spark',  # spark dataframe
        ]   # list of data types to convert content to
        self._rwFormatTypes = [
            'csv',   # comma separated value
            'json',  # Javascript object notation
            'text',  # text file
        ]

        ''' Initialize spark session parameters '''
        self._homeDir = None   # spark $SPARK_HOME dir path property required for sessions
        self._binDir = None    # spark $SPARK_BIN dir path property required for sessions
        self._config = None    # spark .conf option property
        self._jarDir = None    # spark JAR files dir path property
        self._appName = None   # spark appName property with a valid string
        self._master = None    # spark local[*], meso, or yarn property 
        self._rwFormat = None  # spark read/write formats (jdbc, csv,json, text) property
        self._session = None   # spark session is set based on the storeMode property
        self._context = None   # spark context is set to support Hadoop & authentication
        self._saveMode = None  # spark write append/overwrite save mode property

        __s_fn_id__ = "__init__"

        ''' initiate to load app.cfg data '''
        global logger
        global pkgConf
        global appConf

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
        
        ''' get tmp storage location '''
        self.tmpDIR = None
        if "WRITE_TO_FILE" in kwargs.keys():
            self.tmpDIR = os.path.join(self.dataDir,"tmp/")
            if not os.path.exists(self.tmpDIR):
                os.makedirs(self.tmpDIR)

        try:
            logger.info("Connection complete! ready to load data.")
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None


    ''' Function -- DATA --
            TODO: 
            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):
        """ @propert data function

            supports a class decorator @property that is used for getting the
            instance specific datafame. The data must be a pyspark dataframe
            and if it is not one, the function will try to convert the to a 
            pyspark dataframe.

            return self._data (pyspark dataframe)
        """

        __s_fn_id__ = "function <@property data>"

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
        """ @data.setter function

            supports the class propert for setting the instance specific data. 
            The data must not be None-Type and must be a pyspark dataframe.

            return self._data (pyspark dataframe)
        """

        __s_fn_id__ = "function <@data.setter>"

        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")
            self._data = data
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    ''' Function - @property mode and @mode.setter

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._storeMode

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def storeMode(self) -> str:

        __s_fn_id__= "function <@property.storeMode>"
        try:
#                 self._storeMode.lower() not in self._storeModeList and \
            if self._storeMode is None and appConf.has_option("DATASTORE","MODE"):
                self._storeMode = appConf.get("DATASTORE","MODE")
                logger.warning("Reseting non-type storeMode to default %s from %s",
                              self._storeMode, self.__conf_fname__)

#             if not self._storeMode.lower() in self._storeModeList:
#                 raise ValueError("Parameter storeMode is not and must be set")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._storeMode

    @storeMode.setter
    def storeMode(self, store_mode:str) -> str:

        __s_fn_id__ = "function @mode.setter"
        try:
            if not store_mode.lower() in self._storeModeList:
                raise ValueError("Parameter storeMode is not and must be set")
#             if not store_mode.lower() in self._storeModeList and \
#                 appConf.has_option["DATASTORE","MODE"]:
#                 self._storeMode = appConf.get["DATASTORE","MODE"]
#                 logger.warning("%s is invalid MODE and reseting to default % mode from %s",
#                               store_mode,self._storeMode, self.__conf_fname__)
#             else:
            self._storeMode = store_mode.lower()
#                 raise ValueError("Invalid mode = %s. Must be in %s" % (store_mode,self._storeModeList))

            if self._storeMode == 'google-storage' and os.environ.get('GCLOUD_PROJECT') is None:
                if appConf.has_option("GOOGLE","PROJECTID"):
                    os.environ["GCLOUD_PROJECT"] = appConf.get("GOOGLE","PROJECTID")
                    logger.info("GCLOUD_PROJECT os.environment set to %s"
                                % os.environ.get('GCLOUD_PROJECT'))
                else:
                    raise RuntimeError("PROJECTID of the google project, required to"+\
                                       "set the environment variable is undefined in %s"
                                       % (self.__conf_fname__))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._storeMode


    ''' Function - @property store_root and @store_root.setter

            parameters:
                store_root - local file system root directory or (e.g. wrangler/data/ota/scraper)
                            S3 bucket name (e.g. rezaware-wrangler-source-code)
            procedure: Check it the directory exists and then set the store_root property
            return (str) self._storeRoot

            author: <nuwan.waidyanatha@rezgateway.com>

    '''
    @property
    def storeRoot(self) -> str:

        __s_fn_id__ = "function @property storeRoot"

        try:
            if self._storeRoot is None and appConf.has_option("DATASTORE","ROOT"):
                self._storeRoot = appConf.get("DATASTORE","ROOT")
                logger.warning("Non-type storeRoot set to default %s from %s"
                               ,self._storeRoot,self.__conf_fname__)
#             else:
#                 raise ValueError("Invalid Non-Type %s. Set as a property of define in %s"
#                                  % (self._storeRoot,self.__conf_fname__))


        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._storeRoot

    @storeRoot.setter
    def storeRoot(self, store_root:str="") -> str:

        __s_fn_id__ = "function @storeRoot.setter"

        try:
            if store_root is None and "".join(store_root.split())=="":
                raise AttributeError("Invalid store_root parameter %s" % store_root)
#                 appConf.has_option["DATASTORE","ROOT"]:
#                 self._storeRoot = appConf.get["DATASTORE","ROOT"]
#                 logger.warning("%s is invalid ROOT and reseting to default %",
#                               store_root,self._storeRoot)
#             else:
#                 self._storeRoot = store_root

            ''' validate storeRoot '''
            if self.storeMode == "aws-s3-bucket":
                ''' check if bucket exists '''
                logger.debug("%s %s",__s_fn_id__,self.storeMode)
                s3_resource = boto3.resource('s3')
                s3_bucket = s3_resource.Bucket(name=store_root)
                count = len([obj for obj in s3_bucket.objects.all()])
                logger.debug("%s bucket with %d objects exists",
                             self.storeMode,count)
                if count <=0:
                    raise ValueError("Invalid S3 Bucket = %s.\nAccessible Buckets are %s"
                                     % (str(_bucket.name),
                                        str([x for x in s3_resource.buckets.all()])))

            elif self.storeMode == "local-fs":
                ''' check if folder path exists '''
                if not os.path.exists(store_root):
#                 logger.debug("%s %s",__s_fn_id__,self.storeMode)
#                 if not os.path.exists(store_root):
                    raise ValueError("Invalid local folder path = %s does not exists." 
                                     % (store_root))

            elif self.storeMode == "google-storage":
                ''' check if bucket exists '''
                logger.debug("%s %s",__s_fn_id__,self.storeMode)
#                 client = storage.Client()
#                 if not client.bucket(store_root).exists():
#                     raise ValueError("Invalid GCS bucket %s, does not exist." % (store_root))

            else:
                raise ValueError("storeRoot %s does not exist for storeMode %s" 
                                 % (store_root,self.storeMode))

            self._storeRoot = store_root

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._storeRoot

    @property
    def asType(self) -> str:
        """
        Description: 
            The asType property sets the dtype for returning the data. See self._asTypeList
            for valid types. If the asType is not set then it will default to SPARK dataframe
        
        Returns:
            self._asType (str) if unspecified default is SPARK
        """
        __s_fn_id__ = "function @property asType"

        try:
            if self._asType is None:
                self._asType = 'spark'
                logger.warning("Non-type asType value, set to default value SPARK")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._asType

    @asType.setter
    def asType(self, as_type:str="") -> str:
        """
        Description: 
            The asType property is set to the value defined in the as_type input parameter.
            If the value is not in self._asTypeList, the function will throw an exception.
        
        Returns:
            self._asType (str)
        """
        __s_fn_id__ = "function @asType.setter"

        try:
            if as_type.lower() not in self._asTypeList:
                raise AttributeError("Invaid attribute as_type; must be %s" % str(self._asTypeList))
            self._asType = as_type.lower()
            logger.debug("The asType property set to %s",self._asType)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._asType


    ''' Function --- SPARK SESSION PROPERTIES ---
            name: session @property and @setter functions
            parameters:

            procedure: 
                @property - if None try __conf_file__; else throw exception
                @setter - if None or Empty throw exception; else set it
            return self._* (* is the property attribute name)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    ''' HOMEDIR '''
    ''' TODO - check if evn var $SPARK_HOME and $JAVA_HOME is set '''
    @property
    def homeDir(self) -> str:

        __s_fn_id__ = "function <@property homeDir>"

        try:
            if self._homeDir is None and appConf.has_option('SPARK','HOMEDIR'):
                self._homeDir = appConf.get('SPARK','HOMEDIR')
                logger.debug("@property Spark homeDir set to: %s",self._homeDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._homeDir

    @homeDir.setter
    def homeDir(self,home_dir:str='') -> str:

        __s_fn_id__ = "function <@homeDir.setter>"

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

    ''' BINDIR '''
    @property
    def binDir(self) -> str:

        __s_fn_id__ = "function <@property binDir>"

        try:
            if self._binDir is None and appConf.has_option('SPARK','BINDIR'):
                self._binDir = appConf.get('SPARK','BINDIR')
                logger.debug("@property Spark binDir set to: %s",self._binDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._binDir

    @binDir.setter
    def binDir(self,bin_dir:str='') -> str:

        __s_fn_id__ = "function <@binDir.setter>"

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

    ''' APPNAME '''
    @property
    def appName(self) -> str:

        __s_fn_id__ = "function <@property appName>"

        try:
            if self._appName is None or "".join(self._appName.split())=="":
                self._appName = " ".join([self.__app__,
                                          self.__module__,
                                          self.__package__,
                                          self.__name__])
                logger.debug("@property Spark appName set to: %s",self._appName)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._appName

    @appName.setter
    def appName(self,app_name:str='') -> str:

        __s_fn_id__ = "function <@appName.setter>"

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

    ''' CONFIG '''
    @property
    def config(self) -> str:

        __s_fn_id__ = "function <@property config>"

        try:
            if self._config is None and appConf.has_option('SPARK','CONFIG'):
                self._config = appConf.get('SPARK','CONFIG')
                logger.debug("@property Spark config set to: %s",self._config)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._config

    @config.setter
    def config(self,config:str='') -> str:

        __s_fn_id__ = "function <@config.setter>"

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

    ''' JARDIR '''
    @property
    def jarDir(self) -> str:

        __s_fn_id__ = "function <@property jarDir>"

        try:
            if self._jarDir is None and appConf.has_option('SPARK','JARDIR'):
                self._jarDir = appConf.get('SPARK','JARDIR')
                logger.debug("@property Spark jarDir set to: %s",self._jarDir)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._jarDir

    @jarDir.setter
    def jarDir(self,jar_dir:str='') -> str:

        __s_fn_id__ = "function <@jarDir.setter>"

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

    ''' MASTER '''
    @property
    def master(self) -> str:

        __s_fn_id__ = "function <@property master>"

        try:
            if self._master is None and appConf.has_option('SPARK','MASTER'):
                self._master = appConf.get('SPARK','MASTER')
                logger.debug("@property Spark master set to: %s",self._master)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._master

    @master.setter
    def master(self,master:str='local[1]') -> str:

        __s_fn_id__ = "function <@master.setter>"

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

    ''' RWFORMAT '''
    @property
    def rwFormat(self) -> str:

        __s_fn_id__ = "function <@property rwFormat>"

        try:
            if self._rwFormat is None and appConf.has_option('SPARK','FORMAT'):
                self._rwFormat = appConf.get('SPARK','FORMAT')
                logger.debug("Non-type Spark rwFormat set to default %s from %s"
                             ,self._rwFormat,self.__conf_fname__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._rwFormat

    @rwFormat.setter
    def rwFormat(self,rw_format:str='csv') -> str:

        __s_fn_id__ = "function <@saveMode.setter>"

        try:
            if rw_format.lower() not in self._rwFormatTypes:
                raise AttributeError("Invalid spark read/write FORMAT %s must %s" 
                                     % (rw_format,str(self._rwFormatTypes)))

            self._rwFormat = rw_format
            logger.debug("@setter Spark rwFormat set to: %s",self._rwFormat)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._rwFormat


    ''' SAVEMODE '''
    @property
    def saveMode(self) -> str:

        __s_fn_id__ = "function <@property saveMode>"

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
    def saveMode(self,save_mode:str='Overwrite') -> str:

        __s_fn_id__ = "function <@saveMode.setter>"

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
            name: session @property and @setter functions
            parameters:

            procedure: 
            return self._session

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def session(self):

        __s_fn_id__ = "function <@property session>"

        try:
            if self._session is None and \
                not self.homeDir is None and \
                not self.appName is None and \
                not self.config is None and \
                not self.jarDir is None and \
                not self.master is None:
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
                logger.debug("Non-type spark session set with homeDir: %s appName: %s "+\
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

        __s_fn_id__ = "function <@session.setter session>"

        try:
            ''' 
                set the spark home directory '''
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

#             if self.storeMode == 'local-fs':
            ''' create session to read from local file system '''
            if self._session:
                self._session.stop
            self._session = SparkSession \
                                .builder \
                                .master(self.master) \
                                .appName(self.appName) \
                                .config(self.config, self.jarDir) \
                                .getOrCreate()
#             elif self.storeMode == 'aws-s3-bucket':
#                 pass
#             elif self.storeMode == 'google-storage':
#                 pass
#             else:
#                 raise RuntimeError("Unrecognized storeMode %s to start a spark session" 
#                                    % self.storeMode)

            logger.info("Starting a Spark Session: %s for %s"
                        ,self._session, self.storeMode)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._session


    ''' Function --- SPARK CONTEXT ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def context(self):
        """
        Description:
            Sets the spark contect to work with Hadoop necessary for AWS-S3, GCS, etc
            If the context is None, @property will set the default values
        Attributes:
        Returns:
            self._context (sparkConf object)
        """

        __s_fn_id__ = "function <@property context>"
        _access_key=None
        _secret_key=None

        try:
            if self._context is None:
                conf = self.session.sparkContext._jsc.hadoopConfiguration()
                if self.storeMode == 'aws-s3-bucket':
                    _access_key, _secret_key = credentials.aws()
                    conf.set("fs.s3a.access.key", _access_key)
                    conf.set("fs.s3a.secret.key", _secret_key)
                    conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

                elif self.storeMode == 'google-storage':
                    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                    conf.set("fs.AbstractFileSystem.gs.impl",
                             "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                    # This is required if you are using service account and set true, 
                    conf.set('fs.gs.auth.service.account.enable', 'true')
                    ##conf.set('google.cloud.auth.service.account.json.keyfile', "/path/to/keyfile")
                    # Following are required if you are using oAuth
                    ##conf.set('fs.gs.auth.client.id', 'YOUR_OAUTH_CLIENT_ID')
                    ##conf.set('fs.gs.auth.client.secret', 'OAUTH_SECRET')
                else:
                    pass

                self._context = conf
                logger.info("Non-type context configurered for Spark %s authentication with %s"
                            ,self.storeMode,self._context)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug("%s",traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._context

    @context.setter
    def context(self,context_args:dict={}):

        __s_fn_id__ = "function <@session.setter context>"
        _access_key=None
        _secret_key=None

        try:
            conf = self.session.sparkContext._jsc.hadoopConfiguration()
            ''' -- AWS S3 --- '''
            if self.storeMode == 'aws-s3-bucket':
                _access_key, _secret_key = credentials.aws()
                if "ACCESSKEY" in context_args.keys():
                    _access_key = context_args['ACCESSKEY']
                if "SECRETKEY" in context_args.keys():
                    _secret_key = context_args['SECRETKEY']

                conf.set("fs.s3a.access.key", _access_key)
                conf.set("fs.s3a.secret.key", _secret_key)
                conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

            elif self.storeMode == 'google-storage':
                conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                conf.set("fs.AbstractFileSystem.gs.impl",
                         "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                # This is required if you are using service account and set true, 
                conf.set('fs.gs.auth.service.account.enable', 'true')
                ##conf.set('google.cloud.auth.service.account.json.keyfile', "/path/to/keyfile")
                # Following are required if you are using oAuth
                ##conf.set('fs.gs.auth.client.id', 'YOUR_OAUTH_CLIENT_ID')
                ##conf.set('fs.gs.auth.client.secret', 'OAUTH_SECRET')

            else:
                pass

            self._context = conf
            logger.info("Setting a new spark contex config: %s for %s"
                        ,self._context, self.storeMode)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._context


    ''' Function --- IMPORT_FILES ---

        TODO : google-storage still not working
        author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def converter(func):
        """
        Description:
            The retrieved data is converted to the dtype specificed in the as_type
            attribute. The self._data is returned in the prescribed format.
        Arguments:
            func inherits the import_files function
        Returns:
            self._data (pyspark dataframe)        
        """
        @functools.wraps(func)
        def wrapper_converter(
            self,
            as_type,
            folder_path,
            file_name,
            file_type,
            **options,
        ):
            """
                @functools.wraps receives the data, if not null and possible to convert
                will convert to the requested dtype.
            """
            __s_fn_id__ = "Function <wrapper_converter>"

            try:
#                 format_, as_type_ = func(self,as_type,folder_path,file_name,file_type,**options)
                data_ = func(self,as_type,folder_path,file_name,file_type,**options)

                if data_ is None:
                    raise AttributeError("Cannot initiate %s with Non-type dataset" % __s_fn_id__)

                ''' convert to dtype '''
                if self.asType == 'pandas':
                    self._data = data_.toPandas()
                    logger.debug("Converted %d rows to pandas dataframe",self._data.shape[0])
                elif self.asType == 'dict':
                    self._data = data_.toPandas().T.to_dict('list')
                    logger.debug("Converted %d rows to dictionary",len(self._data))
                elif self.asType == 'list':
                    logger.warning("Returning data as pyspark dataframe. "+\
                                   "Unsupported convertion, to be implemented in the future")
                elif self.asType == 'str':
                    logger.warning("Returning data as pyspark dataframe. "+\
                                   "Unsupported convertion, to be implemented in the future")
                else:
                    self._data = data_
                    logger.info("Retuning %d rows pyspark dataframe, no convertion required."
                                ,self._data.count())

            except Exception as err:
                logger.error("%s %s",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data
            return self._data
        return wrapper_converter

    def importer(func):
        """
        Description:
            Wrapper function will apply the storeMode specific import protocols
            to read the data as a pyspark dataframe
        Arguments:
            func inherits the import_files function
        Returns:
            self._data (pyspark dataframe)        
        """
        @functools.wraps(func)
        def wrapper_importer(
            self,
            as_type,
            folder_path,
            file_name,
            file_type,
            **options,
        ):
            """
                @functools.wraps to read the data into a pyspark dataframe using the
                established self_rwFormat and the self.session
            """
            __s_fn_id__ = "Function <wrapper_importer>"

            try:
                format_, as_type_ = func(self,as_type,folder_path,file_name,file_type,**options)
                if format_ is None or as_type_ is None:
                    raise AttributeError("The format_ or as_type_ can not be a Non-Types")

                if self.storeMode == 'local-fs':
                    ''' read content from local file system '''
                    if file_name:
                        file_path = str(os.path.join(self.storeRoot,folder_path,file_name))
                    else:
                        file_path = str(os.path.join(self.storeRoot,folder_path))

                elif self.storeMode == 'aws-s3-bucket':
                    ''' read content from s3 bucket '''
                    if file_name:
                        file_path = str(os.path.join(self.storeRoot,folder_path,file_name))
                    else:
                        file_path = str(os.path.join(self.storeRoot,folder_path))
                    file_path = "s3a://"+file_path
                    self.context = {}

                elif self.storeMode == 'google-storage':
                    ''' read content from google cloud storage '''
                    if file_name:
                        file_path = str(os.path.join(self.storeRoot,folder_path,file_name))
#                         client = storage.Client()
#                         bucket = client.bucket(self.storeRoot)
#                         blob = bucket.blob(file_path)
#     #                     blob.upload_from_filename(_tmp_file_path)
#                         with blob.open("r") as f:
#                             file_content = f.read()
                    elif file_type:
                        ''' multiple files of same file type '''
                        file_path = str(os.path.join(self.storeRoot,folder_path))
                    file_path = "gs://"+file_path
                else:
                    raise typeError("Invalid storage mode %s" % self.storeMode)

                sdf = self.session.read\
                                .format(self.rwFormat)\
                                .options(**options)\
                                .load(file_path)

                if sdf.count() > 0:
                    self._data = sdf
                    logger.info("Read %d rows of data into dataframe",self._data.count())

            except Exception as err:
                logger.error("%s %s",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data
        return wrapper_importer


    @converter
    @importer
    def read_files_to_dtype(
        self,
        as_type:str="",      # optional - define the data type to return
        folder_path:str="",  # optional - relative path, w.r.t. self.storeRoot
        file_name:str=None,  # optional - name of the file to read
        file_type:str=None,  # optional - read all the files of same type
        **options,
    ):
        """
        Description:
            When the direct file path or path to a folder is given, the function
            will read the files and convert them to a dtype specified as_type.
            If a file type is given, instead of the direct file path with name, 
            the function will read all the files in the folder of the requested file
            type. If both a file_name and file_type are specified, then only data
            from the file_name is imported.
            If a folder is not specified, then the function will import data from the
            root folder (e.g. AWS S3 bucket)
        Arguments:
            as_type (str) the dtype to return data as; e.g. dataframe, dict, list
                see self._asTypeList. If unspecified will return the default pyspark
                dataframe
            folder_path (str) a path to the folder relative to the storeRoot; if
                unspecified, will use the self._storeRoot as the folder
            file_name (str) specific a particular file of type
            file_type (str) speficies a file type to read a collection of files; 
                see self._rwFormatTypes for supported file types
            kwargs
        returns:
            _read_mode (str) indicating whether a file name or file type read process
        """

        __s_fn_id__ = "Function <read_files_to_dtype>"
        _read_mode = None
        _read_format = None

        try:
            self.asType = as_type  # validate and set the property
            logger.info("Reading data from %s at root %s",self.storeMode,self.storeRoot)
            if  file_name is not None and \
                "".join(file_name.split())!="":
                ''' check if supported file type and set the reFormat propert '''
                _fname, _fext = os.path.splitext(file_name)  
                self.rwFormat = _fext.replace(".", "").lower()
                logger.info("Reading format %s data from file %s from folder %s",
                           self._rwFormat,_fname,folder_path)
            elif file_type.replace(".", "").lower() in self._rwFormatTypes:
                self.rwFormat = file_type.lower()
                logger.info("Reading all files with read format %s from folder %s",
                           self._rwFormat,folder_path)
            else:
                raise AttributeError("Either a file_name to read a single file "+\
                                     "or a file_type to read all files of type "+\
                                     "must be specified.")

        except Exception as err:
            logger.error("%s %s",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._rwFormat, self._asType
        


    ''' Function --- READ_CSV_TO_SDF ---
            TODO:
            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @classmethod
    def read_csv_to_sdf(
        self,
        files_path:str="", 
        **kwargs):
        """ 
            When the direct file path or path to a folder is given, the function
            will read the files and convert them to a pyspark dataframe. If a folder
            path is given, instead of the direct file path with name, the function
            will read all the CSV files in that folder
            
            Arguments:
                filesPath (str) a path relative to the storeRoot

            return self._data (pyspark dataframe)
        """

        __s_fn_id__ = "Class <SparkWorkLoads> Function <read_folder_csv_to_sdf>"

        _csv_to_sdf = self.session.sparkContext.emptyRDD()     # initialize the return var
#         _tmp_df = self.session.sparkContext.emptyRDD()
        _start_dt = None
        _end_dt = None
        _sdf_cols = []
        _l_cols = []
        _traceback = None

        try:
            ''' check if the folder and files exists '''
            if not filesPath or "".join(files_path.split())=="":
                raise ValueError("Invalid file or folder path %s" % filesPath)
            if "IS_FOLDER" in kwargs.keys() and kwargs['IS_FOLDER']:
                filelist = os.listdir(filesPath)
                if not (len(filelist) > 0):
                    raise ValueError("No data files found in director: %s" % (filesPath))

            ''' set options '''
            _recLookup="true"
            if "RECURSIVELOOKUP" in kwargs.keys() and \
                kwargs['RECURSIVELOOKUP'].lower() in ['true','false']:
                _recLookup = kwargs['RECURSIVELOOKUP'].lower()
            _header="true"
            if "HEADER" in kwargs.keys() and kwargs['HEADER'].lower() in ['true','false']:
                _header = kwargs['HEADER'].lower()
            _inferSchema = "true"
            if "INFERSCHEMA" in kwargs.keys() and kwargs['INFERSCHEMA'].lower() in ['true','false']:
                _inferSchema = kwargs['INFERSCHEMA'].lower()
            _delimeter = ','
            if "DELIMETER" in kwargs.keys():
                _delimeter = kwargs['DELIMETER']
            ''' extract data from **kwargs if exists '''
            if 'SCHEMA' in kwargs.keys():
                _sdf_cols = kwargs['SCHEMA']
            if 'FROMDATETIME' in kwargs.keys():
                _from_dt = kwargs['FROMDATETIME']
            if 'TODATETIME' in kwargs.keys():
                _to_dt = kwargs['TODATETIME']

            _csv_to_sdf = self.session.read\
                            .options( \
                                     recursiveFileLookup=_recLookup, \
                                     header=_header, \
                                     inferSchema=_inferSchema, \
                                     delimiter=_delimeter \
                                    ) \
                            .csv(filesPath)

#            _csv_to_sdf.select(split(_csv_to_sdf.room_rate, '[US$]',2).alias('rate_curr')).show()
            if 'TOPANDAS' in kwargs.keys() and kwargs['TOPANDAS']:
                _csv_to_sdf = _csv_to_sdf.toPandas()
                logger.debug("Converted pyspark dataframe to pandas dataframe with %d rows"
                             % _csv_to_sdf.shape[0])

        except Exception as err:
            logger.error("%s %s",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _csv_to_sdf
        
    ''' Function
            name: read_csv_to_sdf
            parameters:
                    filesPath (str)
                    @enrich (dict)
            procedure: 
            return DataFrame

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def save_sdf_to_csv(self, sdf, filesPath=None, **kwargs):
        
        _csv_file_path = None

        __s_fn_id__ = "function <read_folder_csv_to_sdf>"
        logger.info("Executing %s in %s",__s_fn_id__, __name__)

        try:
            if isinstance(sdf,pd.DataFrame):
                sdf = self.session.createDataFrame(sdf) 
            ''' data exists? '''
            if sdf.count() <= 0:
                raise ValueError("No data for input dataframe to save")
            logger.info("Received % rows to save to file", sdf.count())
            ''' determine where to save '''
            if filesPath:
                _csv_file_path = filesPath
                logger.info("File ready to save to %s", _csv_file_path)
            else:
                fname = __package__+"_"+"save_sdf_to.csv"
                _csv_file_path = os.path.join(self.tmpDIR, fname)
                logger.info("No file path defined, saving to default %s", _csv_file_path)

            ''' save sdf to csv '''
#            sdf.write.option("header",True)\
#                    .option("delimiter",",")\
#                    .csv(_csv_file_path)
            sdf.write.mode(self.saveMode)\
                    .option("header",True)\
                    .format(self.rwFormat)\
                    .save(_csv_file_path)

            logger.info("%d rows of data written to %s",sdf.count(), _csv_file_path)

        except Exception as err:
            logger.error("%s %s",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _csv_file_path


class credentials():
    """
    Description:
        The class supports retrieving authentication credentials for establishing the
        necessary cloud storage connections.
    """

    ''' Function --- GET_APP_CONF ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @staticmethod
    def get_app_conf():
        """
        Description:
            Uses the FileWorkLoads current working directory (cwd) and config file name
        Attributes:
        Returns: 
            appCFG (configparser object)
        """
        __s_fn_id__ = "function <get_app_conf>"

        try:
#         self.cwd=os.path.dirname(__file__)
            clsSpark = FileWorkLoads(desc=__s_fn_id__)
            appCFG = configparser.ConfigParser()
            appCFG.read(os.path.join(clsSpark.appDir,clsSpark.__conf_fname__))

        except Exception as err:
            logger.error("%s %s",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return appCFG

    ''' Function --- GET_APP_CONF ---
    
        TODO: retrieve credentials from the name profile. Now it is reading
              the default profile.

        authors: <nuwan.waidyanatha@rezgateway.com>
    '''
    @staticmethod
    def aws(**kwargs):
        """ 
        Description:
            returns the aws credentials from the aws CLI established credentials
            file stored in the local folder defined in the app config file (__conf_fname__)
            to instatiate the configparser to read the key value pairs. 
        Attributes:
        Returns: 
            aws_access_key_id (str) 
            aws_secret_access_key (str)
        """
        __s_fn_id__ = "function <aws> credentials"
        aws_access_key_id=None
        aws_secret_access_key=None

        try:
            _appCFG = credentials.get_app_conf()
            ''' --- profile --- '''
            if "CREDPROFILE" in kwargs.keys():
                _profile = kwargs['CREDPROFILE']
            elif _appCFG.has_option('AWSAUTH','CREDPROFILE'):
                _profile = _appCFG.get('AWSAUTH','CREDPROFILE')
            else:
                _profile = 'DEFAULT'

            ''' --- file --- '''
            if "CREDFILEPATH" in kwargs.keys():
                _fpath = kwargs['CREDFILEPATH']
            elif _appCFG.has_option('AWSAUTH','CREDFILEPATH'):
                _fpath = _appCFG.get('AWSAUTH','CREDFILEPATH')
            else:
                _fpath = '~/.aws/credentials'
                
            with open(os.path.expanduser(_fpath)) as f:
                for line in f:
                    #print(line.strip().split(' = '))
                    try:
                        key, val = line.strip().split(' = ')
                        if key == 'aws_access_key_id':
                            aws_access_key_id = val
                        elif key == 'aws_secret_access_key':
                            aws_secret_access_key = val
                    except ValueError:
                        pass

        except Exception as err:
            logger.error("%s %s",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return aws_access_key_id, aws_secret_access_key

#     ''' Function
#             name: reset_type to the original data type
#             parameters:

#             procedure: 
#             return self._data

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def reset_dtype(self,data):
        
#         ___s_fn_id____ = "function <reset_dtype>"
#         reset_data = None

#         try:
#             if self.dType == 'RDD':
#                 reset_data=self.data
#             elif self.dType == 'PANDAS':
#                 reset_data=self.data.toPandas()
#             elif self.dType == 'DICT':
#                 print('Method to be done')
#             elif self.dType == 'ARRAY':
#                 print('Method to be done')
#             else:
#                 raise RuntimeError("Something went wrong?")

#         except Exception as err:
#             logger.error("%s %s \n",___s_fn_id____, err)
#             print("[Error]"+___s_fn_id____, err)
#             print(traceback.format_exc())

#         return reset_data


#     ''' Function
#             name: dType @property and @setter functions
#             parameters:

#             procedure: 
#             return self._dType

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     @property
#     def dType(self):
#         return self._dType

#     @dType.setter
#     def dType(self,data_type:str):

#         ___s_fn_id____ = "function <@dType.setter>"

#         try:
#             if data_type is None and not data_type in self._dTypeList:
#                 raise AttributeError('Invalid data_type or is set to empty string')
#             self._dType = data_type

#         except Exception as err:
#             logger.error("%s %s \n",___s_fn_id____, err)
#             print("[Error]"+___s_fn_id____, err)
#             print(traceback.format_exc())

#         return self._dType


#     ''' Function
#             name: get_data_from_table
#             parameters:
#                     @name (str)
#                     @enrich (dict)
#             procedure: 
#             return DataFrame

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def read_data_from_table(self, db_table:str, **kwargs):

#         load_sdf = None   # initiatlize return var
#         __s_fn_id__ = "function <read_data_from_table>"

#         try:
#             ''' validate table '''
            
#             ''' TODO: add code to accept options() to manage schema specific
#                 authentication and access to tables '''

#             print("Wait a moment, retrieving data ...")
#             ''' jdbc:postgresql://<host>:<port>/<database> '''
            
#             # driver='org.postgresql.Driver').\
#             load_sdf = self.session.read.format("jdbc").\
#                 options(
#                     url=self.dbConnURL,    # 'jdbc:postgresql://10.11.34.33:5432/Datascience', 
#                     dbtable=self.dbSchema+"."+db_table,      # '_issuefix_bkdata.customerbookings',
#                     user=self.dbUser,     # 'postgres',
#                     password=self.dbPswd, # 'postgres',
#                     driver=self.dbDriver).load()
#             logger.debug("loaded %d rows into pyspark dataframe" % load_sdf.count())

#             ''' drop duplicates '''
#             if "DROP_DUPLICATES" in kwargs.keys() and kwargs['DROP_DUPLICATES']:
#                 load_sdf = load_sdf.distinct()

#             ''' convert to pandas dataframe '''
#             if 'TO_PANDAS' in kwargs.keys() and kwargs['TO_PANDAS']:
#                 load_sdf = load_sdf.toPandas()
#                 logger.debug("Converted pyspark dataframe to pandas dataframe with %d rows"
#                              % load_sdf.shape[0])

#             print("Loading complete!")

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return load_sdf

#     ''' Function
#             name: insert_sdf_into_table
#             parameters:
#                     @name (str)
#                     @enrich (dict)
#             procedure: 
#             return DataFrame

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def insert_sdf_into_table(self, save_sdf, db_table:str, **kwargs):
        
#         __s_fn_id__ = "function <insert_sdf_into_table>"
#         _num_records_saved = 0
        
#         try:
#             if not save_sdf is None:
#                 self.data = save_sdf
#             if self.data.count() <= 0:
#                 raise ValueError("No data to insert into database table %s"% db_table)
#             if len(kwargs) > 0:
#                 self.session = kwargs
#             else:
#                 self.session = {}
# #             ''' convert pandas to spark dataframe '''
# #             if isinstance(save_sdf,pd.DataFrame):
# # #                 save_sdf = self.session.createDataFrame(save_sdf) 
# #                 save_sdf = self.session.createDataFrame(save_sdf) 
# #             ''' validate sdf have data '''
# #             if save_sdf.count() <= 0:
# #                 raise ValueError("Invalid spark dataframe with %d records" % (save_sdf.count())) 
#             ''' TODO validate table exists '''
            
#             ''' if created audit columns don't exist add them '''
#             listColumns=self.data.columns
#             if "created_dt" not in listColumns:
#                 self.data = self.data.withColumn("created_dt", current_timestamp())
#             if "created_by" not in listColumns:
#                 self.data = self.data.withColumn("created_by", lit(self.dbUser))
#             if "created_proc" not in listColumns:
#                 self.data = self.data.withColumn("created_proc", lit("Unknown"))
            
#             ''' TODO: add code to accept options() to manage schema specific
#                 authentication and access to tables '''

# #             if "saveMode" in kwargs.keys():
# # #                 self.sparkSaveMode = kwargs['saveMode']
# #                 self.sparkSaveMode = kwargs['SAVEMODE']
                
#             logger.info("Wait a moment while we insert data int %s", db_table)
#             ''' jdbc:postgresql://<host>:<port>/<database> '''
            
#             # driver='org.postgresql.Driver').\
#             self.data.select(self.data.columns).\
#                     write.format(self.rwFormat).\
#                     mode(self.saveMode).\
#                 options(
#                     url=self.dbConnURL,    # 'jdbc:postgresql://10.11.34.33:5432/Datascience', 
#                     dbtable=self.dbSchema+"."+db_table,       # '_issuefix_bkdata.customerbookings',
#                     user=self.dbUser,     # 'postgres',
#                     password=self.dbPswd, # 'postgres',
#                     driver=self.dbDriver).save(self.saveMode.lower())
# #                     driver=self.dbDriver).save("append")
# #            load_sdf.printSchema()

#             logger.info("Saved %d  rows into table %s in database %s complete!"
#                         ,self.data.count(), self.dbSchema+"."+db_table, self.dbName)
#             _num_records_saved = self.data.count()

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return _num_records_saved

#     ''' Function
#             name: read_s3obj_to_sdf
#             parameters:
#                 bucketName (str) - s3 bucket name
#                 objPath (str) - s3 key that points to the objecy
#             procedure: 
#             return DataFrame

#             author: <nuwan.waidyanatha@rezgateway.com>

#     '''
#     def read_s3csv_to_sdf(self,bucketName:str,keyFPath: str, **kwargs):

#         import boto3
        
#         _csv_to_sdf = self.session.sparkContext.emptyRDD()     # initialize the return var
# #         _tmp_df = self.session.sparkContext.emptyRDD()
#         _start_dt = None
#         _end_dt = None
#         _sdf_cols = []
#         _l_cols = []
#         _traceback = None
        
#         __s_fn_id__ = "function <read_s3csv_to_sdf>"
#         logger.info("Executing %s in %s",__s_fn_id__, __name__)

#         try:

#             if not 'AWSAUTH' in pkgConf.sections():
#                 raise ValueError('Unable to find AWSAUTH keys and values to continue')
            
#             AWS_ACCESS_KEY_ID = pkgConf.get('AWSAUTH','ACCESSKEY')
#             AWS_SECRET_ACCESS_KEY = pkgConf.get('AWSAUTH','SECURITYKEY')
#             AWS_REGION_NAME = pkgConf.get('AWSAUTH','REGION')

#             s3 = boto3.resource(
#                 's3',
#                 aws_access_key_id=AWS_ACCESS_KEY_ID,
#                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#                 region_name=AWS_REGION_NAME,
#             )
# #             response = s3.get_object(Bucket=bucketName, Key=str(key))
# #             print(self.session.__dict__)
# #             self.session.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
# #             self.session.sparkContext\
# #                     .hadoopConfiguration.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
# #             self.session.sparkContext\
# #                   .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

# #             os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
            
# #             conf = SparkConf().set('spark.executor.extraJavaOptions', \
# #                                    '-Dcom.amazonaws.services.s3.enableV4=true')\
# #                             .set('spark.driver.extraJavaOptions', \
# #                                  '-Dcom.amazonaws.services.s3.enableV4=true')\
# #                             .setAppName('pyspark_aws')\
# #                             .setMaster('local[*]')
            
# #             sc=SparkContext(conf=conf)
# # #             sc=self.session.sparkContext(conf=conf)
# #             sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
            
# #             hadoopConf = sc._jsc.hadoopConfiguration()
# #             hadoopConf.set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
# #             hadoopConf.set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)
# #             hadoopConf.set('fs.s3a.endpoint', AWS_REGION_NAME)
# #             hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            
# #             spark=SparkSession(sc)

# #             Bucket=bucketName,
# #             Key=keyFPath

# #             s3 = boto3.resource('s3')
#             bucket = s3.Bucket(str(bucketName))
#             obj = bucket.objects.filter(Prefix=str(keyFPath))
# #             response = s3.get_object(Bucket=bucketName, Key=str(keyFPath))
# #             _s3_obj = "s3a://"+bucketName+"/"+objPath
# #             _csv_to_sdf=spark.read.csv(
# #             _csv_to_sdf=self.session.read.csv(
#             _csv=self.session.read.csv(
#                 obj,
# #                 _s3_obj,
#                 header=True,
#                 inferSchema=True)
# #             _csv_to_sdf = self.session.read.csv(_s3_obj)

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             print("[Error]"+__s_fn_id__, err)
#             print(traceback.format_exc())

#         return _csv_to_sdf