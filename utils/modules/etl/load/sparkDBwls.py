#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "sparkdbwls"
__module__ = "etl"
__package__ = "load"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import findspark
    import configparser    
    import logging
    import traceback
    findspark.init()
#     from pyspark.sql.functions import split, col,substring,regexp_replace, lit, current_timestamp
    from pyspark.sql.functions import lit, current_timestamp
#     from pyspark import SparkContext, SparkConf
    from pyspark.sql import DataFrame
#     import pandas as pd
#     import numpy as np

    print("All packages in %s %s %s %s imported successfully!"
          % (__app__,__module__,__package__,__name__))

except Exception as e:
    print("Some in packages in {0} didn't load\n{1}".format(__package__,e))

'''
    CLASS create, update, and migrate databases using sql scripts
        1) 

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:
        https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/
'''
class SQLWorkLoads():
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
                 sparkPath:str=None,        # directory path to spark insallation
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

        ''' Initialize the DB parameters '''
        self._dbType = None
        self._dbDriver = None
        self._dbHostIP = None
        self._dbPort = None
        self._dbDriver = None
        self._dbName = None
        self._dbSchema = None
        self._dbUser = None
        self._dbPswd = None
        self._dbConnURL = None

        ''' Initialize DB connection parameters '''
#         self._sparkDIR = None
#         self._sparkJAR = None

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
#             findspark.init(self.sparkDIR)
#             from pyspark.sql import SparkSession

#             ''' the Spark session should be instantiated as follows '''
#             if not "DATA_STORE" in kwargs.keys():
#                 kwargs['DATA_STORE']="LOCAL"
#             if kwargs['DATA_STORE']=="AWS-S3":
#                 print("setting up spark session for AWS S3")
#                 os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

#                 conf = SparkConf().set('spark.executor.extraJavaOptions', \
#                                        '-Dcom.amazonaws.services.s3.enableV4=true')\
#                                 .set('spark.driver.extraJavaOptions', \
#                                      '-Dcom.amazonaws.services.s3.enableV4=true')\
#                                 .setAppName(self.__app__)\
#                                 .setMaster('local[*]')

#                 sc=SparkContext(conf=conf)
#                 print(sc)
#                 sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

#                 hadoopConf = sc._jsc.hadoopConfiguration()
#                 hadoopConf.set('fs.s3a.access.key', pkgConf.get('AWSAUTH','ACCESSKEY'))
#                 hadoopConf.set('fs.s3a.secret.key', pkgConf.get('AWSAUTH','SECURITYKEY'))
#                 hadoopConf.set('fs.s3a.endpoint', pkgConf.get('AWSAUTH','REGION'))
#                 hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

#                 self.session=SparkSession(sc)
                
#             elif kwargs['DATA_STORE']=="LOCAL":
#                 print("setting up spark session for local files")
#             self.session = SparkSession \
#                                 .builder \
#                                 .appName(self.__app__) \
#                                 .config("spark.jars", self.sparkJAR) \
#                                 .getOrCreate()
#             else:
#                 raise ValueError("Invalid DATA_STORE value defined to set the spark session")

            logger.info("Connection complete! ready to load data.")
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
            name: session @property and @setter functions
            parameters:

            procedure: 
                @property - if None try __conf_file__; else throw exception
                @setter - if None or Empty throw exception; else set it
            return self._* (* is the property attribute name)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    ''' --- TYPE --- '''
    @property
    def dbType(self):

        __s_fn_id__ = "function <@property dbType>"

        try:
            if self._dbType is None and appConf.has_option('DATABASE','DBTYPE'):
                self._dbType = appConf.get('DATABASE','DBTYPE')
                logger.debug("@property Database dbType set to: %s",self._dbType)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbType

    @dbType.setter
    def dbType(self,db_type:str=''):

        __s_fn_id__ = "function <@dbType.setter>"

        try:
            if db_type is None or "".join(db_type.strip()) == "":
                raise ConnectionError("Invalid database TYPE %s" % db_type)

            self._dbType = db_type
            logger.debug("@setter Database dbType set to: %s",self._dbType)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbType

    ''' --- DRIVER --- '''
    @property
    def dbDriver(self):

        __s_fn_id__ = "function <@property dbDriver>"

        try:
            if self._dbDriver is None and appConf.has_option('DATABASE','DBDRIVER'):
                self._dbDriver = appConf.get('DATABASE','DBDRIVER')
                logger.debug("@property Database dbDriver set to: %s",self._dbDriver)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbDriver

    @dbDriver.setter
    def dbDriver(self,db_driver:str=''):

        __s_fn_id__ = "function <@dbDriver.setter>"

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
    def dbHostIP(self):

        __s_fn_id__ = "function <@property dbHostIP>"

        try:
            if self._dbHostIP is None and appConf.has_option('DATABASE','DBHOSTIP'):
                self._dbHostIP = appConf.get('DATABASE','DBHOSTIP')
                logger.debug("@property Database dbHostIP set to: %s",self._dbHostIP)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbHostIP

    @dbHostIP.setter
    def dbHostIP(self,db_host_ip:str='127.0.0.1'):

        __s_fn_id__ = "function <@dbHostIP.setter >"

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
    def dbPort(self):

        __s_fn_id__ = "function <@property dbPort>"

        try:
            if self._dbPort is None and appConf.has_option('DATABASE','DBPORT'):
                self._dbPort = appConf.get('DATABASE','DBPORT')
                logger.debug("@property Database Port set to: %s",self._dbPort)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPort

    @dbPort.setter
    def dbPort(self,db_port:int=5432):

        __s_fn_id__ = "function <@dbPort.setter dbPort>"

        try:
            if db_port is None or not isinstance(db_type,int):
                raise ConnectionError("Invalid database port integer %s" % str(db_port))

            self._dbPort = str(db_port)
            logger.debug("@setter Database Port set to: %s",self._dbPort)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self.__dbPort

    ''' --- NAME --- '''
    @property
    def dbName(self):

        __s_fn_id__ = "function <@property dbName>"

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

        __s_fn_id__ = "function <@dbName.setter>"

        try:
            if db_name is None or "".join(db_name.strip()) == "":
                raise ConnectionError("Invalid database NAME %s" % db_name)

            self._dbName = db_name
            logger.debug("@setter Database dbName set to: %s",self._dbName)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbName

    ''' --- SCHEMA --- '''
    @property
    def dbSchema(self):

        __s_fn_id__ = "function <@property dbSchema>"

        try:
            if self._dbSchema is None and appConf.has_option('DATABASE','DBSCHEMA'):
                self._dbSchema = appConf.get('DATABASE','DBSCHEMA')
                logger.debug("@property Database dbSchema set to: %s",self._dbSchema)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbSchema

    @dbSchema.setter
    def dbSchema(self,db_schema:str=''):

        __s_fn_id__ = "function <@dbSchema.setter>"

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
    def dbUser(self):

        __s_fn_id__ = "function <@property dbUser>"

        try:
            if self._dbUser is None and appConf.has_option('DATABASE','DBUSER'):
                self._dbUser = appConf.get('DATABASE','DBUSER')
                logger.debug("@property Database dbUser set to: %s",self._dbUser)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbUser

    @dbUser.setter
    def dbUser(self,db_user:str=''):

        __s_fn_id__ = "function <@dbPswd.setter>"

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
    def dbPswd(self):

        __s_fn_id__ = "function <@property dbPswd>"

        try:
            if self._dbPswd is None and appConf.has_option('DATABASE','DBPSWD'):
                self._dbPswd = appConf.get('DATABASE','DBPSWD')
                logger.debug("@property Database dbPswd set to: %s",self._dbPswd)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPswd

    @dbPswd.setter
    def dbPswd(self,db_driver:str=''):

        __s_fn_id__ = "function <@session.setter dbPswd>"

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
    def dbConnURL(self):

        __s_fn_id__ = "function <@property dbConnURL>"

        try:
            if self._dbConnURL is None and \
                not self.dbType is None and \
                not self.dbHostIP is None and \
                not self.dbPort is None and \
                not self.dbName is None:
                self._dbConnURL = "jdbc:"+self.dbType+\
                                    "://"+self.dbHostIP+":"+\
                                    self.dbPort+"/"+self.dbName
            logger.debug("@property Database dbConnURL set to: %s",self._dbConnURL)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug("%s",traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbConnURL

    @dbConnURL.setter
    def dbConnURL(self,**kwargs):

        __s_fn_id__ = "function <@dbConnURL.setter dbConnURL>"

        try:
            ''' --- DATABASE PROPERTY **KWARGS --- '''
            if "DBTYPE" in kwargs.keys():
                self.db_type = kwargs['DBTYPE']
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
    ''' HOMEDIR '''
    ''' TODO - check if evn var $SPARK_HOME and $JAVA_HOME is set '''
    @property
    def homeDir(self):

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
    def homeDir(self,home_dir:str=''):

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
    def binDir(self):

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
    def binDir(self,bin_dir:str=''):

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
    def appName(self):

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
    def appName(self,app_name:str=''):

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
    def config(self):

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
    def config(self,config:str=''):

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
    def jarDir(self):

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
    def jarDir(self,jar_dir:str=''):

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
    def master(self):

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
    def master(self,master:str='local[1]'):

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
    def rwFormat(self):

        __s_fn_id__ = "function <@property rwFormat>"

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
    def rwFormat(self,rw_format:str='jdbc'):

        __s_fn_id__ = "function <@saveMode.setter>"

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


    ''' SAVEMODE '''
    @property
    def saveMode(self):

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
    def saveMode(self,save_mode:str='Append'):

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
                
            logger.info("Starting a Spark Session: %s",self._session)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug("%s",traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._session

    @session.setter
#     def session(self,**kwargs):
    def session(self,session_args:dict={}):

        __s_fn_id__ = "function <@session.setter session>"

        try:
            ''' 
                set the spark home directory '''
            if "HOMEDIR" in session_args.keys():
                self.homeDir = session_args['HOMEDIR']
#             elif appConf.has_option('SPARK','HOMEDIR'):
#                 _spark_home_dir = appConf.get('SPARK','HOMEDIR')
#             else:
#                 raise AttributeError("Spark home directory is required to proceed "+ \
#                                 "It must be specified in app.cfg or "+ \
#                                 "passed as a **session_args key value pair")
            
            findspark.init(self.homeDir)
            from pyspark.sql import SparkSession
            logger.debug("Importing %s library from spark dir: %s"
                         % (SparkSession.__name__, self.homeDir))

            if "CONFIG" in session_args.keys():
                self.config = session_args['CONFIG']
#             elif appConf.has_option('SPARK','CONFIG'):
#                 _conf_opt = appConf.get('SPARK','CONFIG')
#             else:
#                 _conf_opt = "spark.jars"

            ''' set master cluster setup local[x], yarn or mesos '''
            if "MASTER" in session_args.keys():
                self.master = session_args['MASTER']
#             elif appConf.has_option('SPARK','MASTER'):
#                 _master = appConf.get('SPARK','MASTER')
#             else:
#                 _master = "local[1]"     

            if "APPNAME" in session_args.keys():
                self.appName = session_args['APPNAME']
#             elif appConf.has_option('SPARK','APPNAME'):
#                 _app_name = appConf.get('SPARK','APPNAME')
#             else:
#                 _app_name = self.__app__     

            ''' set the db_type specific jar '''
            if "JARDIR" in session_args.keys():
                self.jarDir = session_args['JARDIR']
#             elif appConf.has_option('SPARK','JARDIR'):
#                 _jar_dir = appConf.get('SPARK','JARDIR')
#             else:
#                 _jar_dir = None
#                 raise ConnectionError("Spark requires a valid jar file to use with %s" % self.db_type)
#             self.sparkJAR = appConf.get('SPARK','JARDIR')
#             logger.info("Defining Spark Jar dir: %s" % (self.sparkJAR))

#             if _jar_dir is None:
#                 self._session = SparkSession \
#                                     .builder \
#                                     .master(_master) \
#                                     .appName(_app_name) \
#                                     .getOrCreate()
#             else:
            self._session = SparkSession \
                                .builder \
                                .master(self.master) \
                                .appName(self.appName) \
                                .config(self.config, self.jarDir) \
                                .getOrCreate()
                
            logger.info("Starting a Spark Session: %s" % (self._session))

#             ''' TODO Deprate after all functions are working '''
#             self.session=self._session

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._session


    ''' Function
            name: data @property and @setter functions
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):

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

        __s_fn_id__ = "function <@data.setter>"

#         self.session.conf.set("spark.sql.execution.arrow.enabled","true")

        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")
            self._data = data

#             if not isinstance(data,DataFrame):
#                 self._data = self.session.createDataFrame(data)
# #                 self.dType = 'OTHER'
#             else:
#                 self.dType = 'SDF'
#             elif isinstance(data, pd.DataFrame) and not data.empty:
#                 ''' pandas dataframe convert to pyspark DataFrame '''
#                 self.dType = 'PANDAS'
#                 self._data = self.session.createDataFrame(data)
#             elif isinstance(data, dict) and len(data) > 0:
#                 ''' dict convert to spark dataframe '''
#                 self.dType = 'DICT'
#                 print('Method To Be Defined')
#             elif isinstance(data, np.ndarray) and data.size > 0:
#                 ''' ndarray convert to spark dataframe '''
#                 self.dType = 'ARRAY'
#                 print('Method To Be Defined')
#             else:
#                 raise AttributeError('Invalid data set of dtype %s and must be none empty of type %s'
#                                      % (type(data),str(self._dTypeList)))
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

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


    ''' Function
            name: get_data_from_table
            parameters:
                    @name (str)
                    @enrich (dict)
            procedure: 
            return DataFrame

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def read_data_from_table(self, db_table:str, **kwargs):

        load_sdf = None   # initiatlize return var
        __s_fn_id__ = "function <read_data_from_table>"

        try:
            ''' validate table '''
            
            ''' TODO: add code to accept options() to manage schema specific
                authentication and access to tables '''

            print("Wait a moment, retrieving data ...")
            ''' jdbc:postgresql://<host>:<port>/<database> '''
            
            # driver='org.postgresql.Driver').\
            load_sdf = self.session.read.format("jdbc").\
                options(
                    url=self.dbConnURL,    # 'jdbc:postgresql://10.11.34.33:5432/Datascience', 
                    dbtable=self.dbSchema+"."+db_table,      # '_issuefix_bkdata.customerbookings',
                    user=self.dbUser,     # 'postgres',
                    password=self.dbPswd, # 'postgres',
                    driver=self.dbDriver).load()
            logger.debug("loaded %d rows into pyspark dataframe" % load_sdf.count())

            ''' drop duplicates '''
            if "DROP_DUPLICATES" in kwargs.keys() and kwargs['DROP_DUPLICATES']:
                load_sdf = load_sdf.distinct()

            ''' convert to pandas dataframe '''
            if 'TO_PANDAS' in kwargs.keys() and kwargs['TO_PANDAS']:
                load_sdf = load_sdf.toPandas()
                logger.debug("Converted pyspark dataframe to pandas dataframe with %d rows"
                             % load_sdf.shape[0])

            print("Loading complete!")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return load_sdf

    ''' Function
            name: insert_sdf_into_table
            parameters:
                    @name (str)
                    @enrich (dict)
            procedure: 
            return DataFrame

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def insert_sdf_into_table(self, save_sdf, db_table:str, **kwargs):
        
        __s_fn_id__ = "function <insert_sdf_into_table>"
        _num_records_saved = 0
        
        try:
            if not save_sdf is None:
                self.data = save_sdf
            if self.data.count() <= 0:
                raise ValueError("No data to insert into database table %s"% db_table)
            if len(kwargs) > 0:
                self.session = kwargs
            else:
                self.session = {}
#             ''' convert pandas to spark dataframe '''
#             if isinstance(save_sdf,pd.DataFrame):
# #                 save_sdf = self.session.createDataFrame(save_sdf) 
#                 save_sdf = self.session.createDataFrame(save_sdf) 
#             ''' validate sdf have data '''
#             if save_sdf.count() <= 0:
#                 raise ValueError("Invalid spark dataframe with %d records" % (save_sdf.count())) 
            ''' TODO validate table exists '''
            
            ''' if created audit columns don't exist add them '''
            listColumns=self.data.columns
            if "created_dt" not in listColumns:
                self.data = self.data.withColumn("created_dt", current_timestamp())
            if "created_by" not in listColumns:
                self.data = self.data.withColumn("created_by", lit(self.dbUser))
            if "created_proc" not in listColumns:
                self.data = self.data.withColumn("created_proc", lit("Unknown"))
            
            ''' TODO: add code to accept options() to manage schema specific
                authentication and access to tables '''

#             if "saveMode" in kwargs.keys():
# #                 self.sparkSaveMode = kwargs['saveMode']
#                 self.sparkSaveMode = kwargs['SAVEMODE']
                
            logger.info("Wait a moment while we insert data int %s", db_table)
            ''' jdbc:postgresql://<host>:<port>/<database> '''
            
            # driver='org.postgresql.Driver').\
            self.data.select(self.data.columns).\
                    write.format(self.rwFormat).\
                    mode(self.saveMode).\
                options(
                    url=self.dbConnURL,    # 'jdbc:postgresql://10.11.34.33:5432/Datascience', 
                    dbtable=self.dbSchema+"."+db_table,       # '_issuefix_bkdata.customerbookings',
                    user=self.dbUser,     # 'postgres',
                    password=self.dbPswd, # 'postgres',
                    driver=self.dbDriver).save(self.saveMode.lower())
#                     driver=self.dbDriver).save("append")
#            load_sdf.printSchema()

            logger.info("Saved %d  rows into table %s in database %s complete!"
                        ,self.data.count(), self.dbSchema+"."+db_table, self.dbName)
            _num_records_saved = self.data.count()

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _num_records_saved

    ''' Function
            name: read_csv_to_sdf
            parameters:
                    filesPath (str)
                    @enrich (dict)
            procedure: 
            return DataFrame

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def read_csv_to_sdf(self,filesPath: str, **kwargs):

        _csv_to_sdf = self.session.sparkContext.emptyRDD()     # initialize the return var
#         _tmp_df = self.session.sparkContext.emptyRDD()
        _start_dt = None
        _end_dt = None
        _sdf_cols = []
        _l_cols = []
        _traceback = None

        try:
            ''' check if the folder and files exists '''
            if not filesPath:
                raise ValueError("Invalid folder path %s" % filesPath)
            if "IS_FOLDER" in kwargs.keys() and kwargs['IS_FOLDER']:
                filelist = os.listdir(filesPath)
                if not (len(filelist) > 0):
                    raise ValueError("No data files found in director: %s" % (filesPath))

            ''' set inferschema '''
            _csv_inferSchema = True
            if "INFERSCHEMA" in kwargs.keys():
                _csv_inferSchema = kwargs['INFERSCHEMA']
            ''' extract data from **kwargs if exists '''
            if 'schema' in kwargs.keys():
                _sdf_cols = kwargs['schema']
            if 'start_datetime' in kwargs.keys():
                _start_dt = kwargs['start_datetime']
            if 'end_datetime' in kwargs.keys():
                _start_dt = kwargs['end_datetime']

            _csv_to_sdf = self.session.read.options( \
                                                          header='True', \
                                                          inferSchema=_csv_inferSchema, \
                                                          delimiter=',') \
                                            .csv(filesPath)

#            _csv_to_sdf.select(split(_csv_to_sdf.room_rate, '[US$]',2).alias('rate_curr')).show()
            if 'TO_PANDAS' in kwargs.keys() and kwargs['TO_PANDAS']:
                _csv_to_sdf = _csv_to_sdf.toPandas()
                logger.debug("Converted pyspark dataframe to pandas dataframe with %d rows"
                             % _csv_to_sdf.shape[0])

        except Exception as err:
            __s_fn_id__ = "Class <SparkWorkLoads> Function <read_folder_csv_to_sdf>"
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            _traceback = traceback.format_exc()
            print(traceback.format_exc())

        return _csv_to_sdf, _traceback
        
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
            sdf.write.mode("overwrite")\
                    .option("header",True)\
                    .format("csv")\
                    .save(_csv_file_path)

            logger.info("%d rows of data written to %s",sdf.count(), _csv_file_path)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return _csv_file_path

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