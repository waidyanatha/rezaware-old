#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "sparkWorkLoads"
__package__ = "SparkWorkLoads"
# __root_dir__ = "/home/nuwan/workspace/rezgate/wrangler/"
__utils_dir__ = '/home/nuwan/workspace/rezgate/utils/'
#__data_dir__ = 'data/hospitality/bookings/scraper/'
__conf_fname__ = 'app.cfg'
__logs_dir__ = 'logs/'
__log_fname__ = 'app.log'

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import findspark
    findspark.init()
    from pyspark.sql.functions import split, col,substring,regexp_replace, lit, current_timestamp
#    import pyspark
    import pandas as pd
#    from pandas.api.types import is_datetime64_any_dtype as is_datetime
#    import calendar
    import configparser    
    import logging
    import traceback


    print("All packages in SparkWorkLoads loaded successfully!")

except Exception as e:
    print("Some packages didn't load\n{}".format(e))

'''
    CLASS create, update, and migrate databases using sql scripts
        1) 

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:
        https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/
'''
class SparkWorkLoads():
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
        self.__desc__ = desc

        ''' initiate to load app.cfg data '''
        global confApp
        global confUtil
        confApp = configparser.ConfigParser()
        confUtil = configparser.ConfigParser()

#         ''' Set the wrangler root directory '''
#         self.rootDir = __root_dir__
#         if "ROOT_DIR" in kwargs.keys():
#             self.rootDir = kwargs['ROOT_DIR']
#         if self.rootDir[-1] != "/":
#             self.rootDir +="/"
        ''' Set the utils root directory '''
        self.utilsDir = __utils_dir__
        if "UTILS_DIR" in kwargs.keys():
            self.utilsDir = kwargs['UTILS_DIR']
        ''' load the utils config env vars '''
#         self.appConfigPath = os.path.join(self.rootDir, __conf_fname__)
#         confApp.read(self.appConfigPath)
#         self.utilsDir = os.path.join(self.rootDir, __utils_dir__)
        self.utilConfFPath = os.path.join(self.utilsDir, __conf_fname__)
        confUtil.read(self.utilConfFPath)

        ''' get the file and path for the logger '''
        self.logDir = os.path.join(self.utilsDir,confUtil.get('LOGGING','LOGPATH'))
        if not os.path.exists(self.logDir):
            os.makedirs(self.logDir)
        self.logFPath = os.path.join(self.logDir,confUtil.get('LOGGING','LOGFILE'))
        ''' innitialize the logger '''
        global logger
        logger = logging.getLogger(__package__)
        logger.setLevel(logging.DEBUG)
        if (logger.hasHandlers()):
            logger.handlers.clear()
        # create file handler which logs even debug messages
        fh = logging.FileHandler(self.logFPath, confUtil.get('LOGGING','LOGMODE'))
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        ''' set a new logger section '''
        logger.info('########################################################')
        logger.info(__name__)
        logger.info('Utils Path = %s', self.utilsDir)
        
        ''' get tmp storage location '''
        self.tmpDIR = None
        self.tmpDIR = os.path.join(self.utilsDir,confUtil.get('STORES','TMPDATA'))
        if not os.path.exists(self.tmpDIR):
            os.makedirs(self.tmpDIR)


        ''' Initialize the DB connection parameters '''
        self.db_port = None
        self.db_driver = None
        self.db_name = None
        self.db_schema = None
        self.db_user = None
        self.db_pswd = None

        ''' Initialize spark connection parameters '''
        self.spark_dir = None
        self.spark_jar = None
        self.spark_url = None
        self.spark_session = None

        ''' Spark function parameters '''
        self.spark_save_mode = "Append"
        try:
            ''' --- DATABASE ---
                set the host IP '''
            self.host_ip = None
            if "hostIP" in kwargs.keys():
                self.host_ip = kwargs['hostIP']
            elif confUtil.get('HOSTS','HOSTIP'):
                self.host_ip = confUtil.get('HOSTS','HOSTIP')
            else:
                raise ConnectionError("Undefined host IP. Set the host_ip in app.cfg")

            ''' set the database type '''
            self.db_type = None
            if "dbType" in kwargs.keys():
                self.db_type = kwargs['dbType']
            elif confUtil.get('DATABASE','DBTYPE'):
                self.db_type = confUtil.get('DATABASE','DBTYPE')
            else:
                raise ConnectionError("Undefined database type. Set the db_type in app.cfg")

            ''' set the database port '''
            if "dbPort" in kwargs.keys():
                self.db_port = kwargs['dbPort']
            elif confUtil.get('DATABASE','DBPORT'):
                self.db_port = confUtil.get('DATABASE','DBPORT')
            else:
                raise ConnectionError("Undefined database port. Set the db_port in app.cfg")

            ''' set the database driver '''
            if "dbDriver" in kwargs.keys():
                self.db_driver = kwargs['dbDriver']
            elif confUtil.get('DATABASE','DBDRIVER'):
                self.db_driver = confUtil.get('DATABASE','DBDRIVER')
            else:
                raise ConnectionError("Undefined database password. Set the db_driver in app.cfg")

            ''' set the database name '''
            if "dbName" in kwargs.keys():
                self.db_name = kwargs['dbName']
            elif confUtil.get('DATABASE','DBNAME'):
                self.db_name = confUtil.get('DATABASE','DBNAME')
            else:
                raise ConnectionError("Undefined database name. Set the db_name in app.cfg")

            ''' set the database schema '''
            if "dbSchema" in kwargs.keys():
                self.db_schema = kwargs['dbSchema']
            elif confUtil.get('DATABASE','DBSCHEMA'):
                self.db_schema = confUtil.get('DATABASE','DBSCHEMA')
            else:
                raise ConnectionError("Undefined database schema. Set the db_schema in app.cfg")

            ''' set the database username '''
            if "dbUser" in kwargs.keys():
                self.db_user = kwargs['dbUser']
            elif confUtil.get('DATABASE','DBUSER'):
                self.db_user = confUtil.get('DATABASE','DBUSER')
            else:
                raise ConnectionError("Undefined database username. Set the db_user in app.cfg")

            ''' set the database password '''
            if "dbPswd" in kwargs.keys():
                self.db_pswd = kwargs['DBPSWD']
            elif confUtil.get('DATABASE','DBPORT'):
                self.db_pswd = confUtil.get('DATABASE','DBPSWD')
            else:
                raise ConnectionError("Undefined database password. Set the db_pswd in app.cfg")


            ''' --- SPARK ---
                set the spark home directory '''
            if not (sparkPath or confUtil.get('SPARK','SPARKHOMEDIR')):
                raise ValueError("Spark directory required to proceed. \
                                Must be specified in app_config.py or \
                                spark_path %s must be valid" % sparkPath)
            if sparkPath:
                ''' override config.spark_install_director '''
                ''' TODO validate spark_dir '''
                self.spark_dir = sparkPath
            else:
                self.spark_dir = confUtil.get('SPARK','SPARKHOMEDIR')
            
            findspark.init(self.spark_dir)
            from pyspark.sql import SparkSession
            logger.info("Importing %s library from spark dir: %s" % (SparkSession.__name__, self.spark_dir))

            ''' set the db_type specific jar '''
            if not confUtil.get('SPARK','SPARKJARDIR'):
                raise ConnectionError("Spark requires a valid jar file to use with %s" % self.db_type)
            self.spark_jar = confUtil.get('SPARK','SPARKJARDIR')
            logger.info("Defining Spark Jar dir: %s" % (self.spark_jar))

            ''' the Spark session should be instantiated as follows '''
            self.spark_session = SparkSession \
                    .builder \
                    .appName("rezaware wrangler") \
                    .config("spark.jars", self.spark_jar) \
            .getOrCreate()
            logger.info("Starting a Spark Session: %s" % (self.spark_session))

            ''' build the url for db connection '''
            self.spark_url = "jdbc:"+self.db_type+"://"+self.host_ip+":"+self.db_port+"/"+self.db_name
            logger.info("Defined spark database connection url: %s" % (self.spark_url))

            logger.info("Connection complete! ready to load data.")
            print("Initialing %s class for %s with instance %s"
                  % (self.__package__, self.__name__, self.__desc__))
            print("Logging %s info, warnings, and error to %s" % (self.__package__, self.logFPath))

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <__init__>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None

#     ''' Function
#             name: get_spark_session
#             parameters:
#                     @name (str)
#                     @enrich (dict)
#             procedure:

#             return DataFrame
#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def DEPRECATED_get_spark_session(self, **kwargs):

#         try:
#             ''' the Spark session should be instantiated as follows '''
#             self.spark_session = SparkSession \
#                                     .builder \
#                                     .appName("rezaware wrangler") \
#                                     .config("spark.jars", self.spark_jar) \
#                                     .getOrCreate()

#         except Exception as err:
#             _s_fn_id = "Class <SparkWorkLoads> Function <get_spark_session>"
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self.spark_session

    ''' Function
            name: get_data_from_table
            parameters:
                    @name (str)
                    @enrich (dict)
            procedure: 
            return DataFrame

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_data_from_table(self, dbTable:str, **kwargs):
        
        load_sdf = None   # initiatlize return var

        try:
            ''' validate table '''
            
            ''' TODO: add code to accept options() to manage schema specific
                authentication and access to tables '''

            print("Wait a moment, retrieving data ...")
            ''' jdbc:postgresql://<host>:<port>/<database> '''
            
            # driver='org.postgresql.Driver').\
            load_sdf = self.spark_session.read.format("jdbc").\
                options(
                    url=self.spark_url,    # 'jdbc:postgresql://10.11.34.33:5432/Datascience', 
                    dbtable=self.db_schema+"."+dbTable,      # '_issuefix_bkdata.customerbookings',
                    user=self.db_user,     # 'postgres',
                    password=self.db_pswd, # 'postgres',
                    driver=self.db_driver).load()
            logger.debug("loaded %d rows into pyspark dataframe" % load_sdf.count())

            if 'TO_PANDAS' in kwargs.keys() and kwargs['TO_PANDAS']:
                load_sdf = load_sdf.toPandas()
                logger.debug("Converted pyspark dataframe to pandas dataframe with %d rows"
                             % load_sdf.shape[0])

            print("Loading complete!")

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <get_data_from_table>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

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
    def insert_sdf_into_table(self, save_sdf, dbTable:str, **kwargs):
        
        _num_records_saved = 0
        
        try:
            ''' convert pandas to spark dataframe '''
            if isinstance(save_sdf,pd.DataFrame):
                save_sdf = self.spark_session.createDataFrame(save_sdf) 
            ''' validate sdf have data '''
            if save_sdf.count() <= 0:
                raise ValueError("Invalid spark dataframe with %d records" % (save_sdf.count())) 
            ''' validate table '''
            
            ''' if created audit columns don't exist add them '''
            listColumns=save_sdf.columns
            if "created_dt" not in listColumns:
                save_sdf = save_sdf.withColumn("created_dt", current_timestamp())
            if "created_by" not in listColumns:
                save_sdf = save_sdf.withColumn("created_by", lit(self.db_user))
            if "created_proc" not in listColumns:
                save_sdf = save_sdf.withColumn("created_proc", lit("Unknown"))
            
            ''' TODO: add code to accept options() to manage schema specific
                authentication and access to tables '''

            if "saveMode" in kwargs.keys():
                self.spark_save_mode = kwargs['saveMode']
                
            print("Wait a moment while we insert data int %s" % dbTable)
            ''' jdbc:postgresql://<host>:<port>/<database> '''
            
            # driver='org.postgresql.Driver').\
            save_sdf.select(save_sdf.columns).write.format("jdbc").mode(self.spark_save_mode).\
                options(
                    url=self.spark_url,    # 'jdbc:postgresql://10.11.34.33:5432/Datascience', 
                    dbtable=self.db_schema+"."+dbTable,       # '_issuefix_bkdata.customerbookings',
                    user=self.db_user,     # 'postgres',
                    password=self.db_pswd, # 'postgres',
                    driver=self.db_driver).save("append")
#            load_sdf.printSchema()

            print("Save to %s complete!" % (dbTable))
            _num_records_saved = save_sdf.count()

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <insert_sdf_into_table>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

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

        _csv_to_sdf = self.spark_session.sparkContext.emptyRDD()     # initialize the return var
#         _tmp_df = self.spark_session.sparkContext.emptyRDD()
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

            ''' extract data from **kwargs if exists '''
            if 'schema' in kwargs.keys():
                _sdf_cols = kwargs['schema']
            if 'start_datetime' in kwargs.keys():
                _start_dt = kwargs['start_datetime']
            if 'end_datetime' in kwargs.keys():
                _start_dt = kwargs['end_datetime']

            _csv_to_sdf = self.spark_session.read.options( \
                                                          header='True', \
                                                          inferSchema='True', \
                                                          delimiter=',') \
                                            .csv(filesPath)

#            _csv_to_sdf.select(split(_csv_to_sdf.room_rate, '[US$]',2).alias('rate_curr')).show()
            if 'TO_PANDAS' in kwargs.keys() and kwargs['TO_PANDAS']:
                _csv_to_sdf = _csv_to_sdf.toPandas()
                logger.debug("Converted pyspark dataframe to pandas dataframe with %d rows"
                             % _csv_to_sdf.shape[0])

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <read_folder_csv_to_sdf>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
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

        _s_fn_id = "function <read_folder_csv_to_sdf>"
        logger.info("Executing %s in %s",_s_fn_id, __name__)

        try:
            if isinstance(sdf,pd.DataFrame):
                sdf = self.spark_session.createDataFrame(sdf) 
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
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _csv_file_path
          