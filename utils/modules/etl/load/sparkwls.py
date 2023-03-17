#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "sparkwls"
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
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        self._data = None
        self._dType = None
        self._dTypeList = [
            'RDD',     # spark resilient distributed dataset
            'SDF',     # spark DataFrame
            'PANDAS',  # pandas dataframe
            'ARRAY',   # numpy array
            'DICT',    # data dictionary
        ]

        self._session = None
        self._dbConnection = None

        _s_fn_id = "__init__"

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
#             if not os.path.exists(self.tmpDIR):
#                 os.makedirs(self.tmpDIR)

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
#             ''' --- DATABASE ---
#                 set the host IP '''
#             self.host_ip = None
#             if "hostIP" in kwargs.keys():
#                 self.host_ip = kwargs['hostIP']
#             elif appConf.get('HOSTS','HOSTIP'):
#                 self.host_ip = appConf.get('HOSTS','HOSTIP')
#             else:
#                 raise ConnectionError("Undefined host IP. Set the host_ip in app.cfg")

#             ''' set the database type '''
#             self.db_type = None
#             if "dbType" in kwargs.keys():
#                 self.db_type = kwargs['dbType']
#             elif appConf.get('DATABASE','DBTYPE'):
#                 self.db_type = appConf.get('DATABASE','DBTYPE')
#             else:
#                 raise ConnectionError("Undefined database type. Set the db_type in app.cfg")

#             ''' set the database port '''
#             if "dbPort" in kwargs.keys():
#                 self.db_port = kwargs['dbPort']
#             elif appConf.get('DATABASE','DBPORT'):
#                 self.db_port = appConf.get('DATABASE','DBPORT')
#             else:
#                 raise ConnectionError("Undefined database port. Set the db_port in app.cfg")

#             ''' set the database driver '''
#             if "dbDriver" in kwargs.keys():
#                 self.db_driver = kwargs['dbDriver']
#             elif appConf.get('DATABASE','DBDRIVER'):
#                 self.db_driver = appConf.get('DATABASE','DBDRIVER')
#             else:
#                 raise ConnectionError("Undefined database password. Set the db_driver in app.cfg")

#             ''' set the database name '''
#             if "dbName" in kwargs.keys():
#                 self.db_name = kwargs['dbName']
#             elif appConf.get('DATABASE','DBNAME'):
#                 self.db_name = appConf.get('DATABASE','DBNAME')
#             else:
#                 raise ConnectionError("Undefined database name. Set the db_name in app.cfg")

#             ''' set the database schema '''
#             if "dbSchema" in kwargs.keys():
#                 self.db_schema = kwargs['dbSchema']
#             elif appConf.get('DATABASE','DBSCHEMA'):
#                 self.db_schema = appConf.get('DATABASE','DBSCHEMA')
#             else:
#                 raise ConnectionError("Undefined database schema. Set the db_schema in app.cfg")

#             ''' set the database username '''
#             if "dbUser" in kwargs.keys():
#                 self.db_user = kwargs['dbUser']
#             elif appConf.get('DATABASE','DBUSER'):
#                 self.db_user = appConf.get('DATABASE','DBUSER')
#             else:
#                 raise ConnectionError("Undefined database username. Set the db_user in app.cfg")

#             ''' set the database password '''
#             if "dbPswd" in kwargs.keys():
#                 self.db_pswd = kwargs['DBPSWD']
#             elif appConf.get('DATABASE','DBPSWD'):
#                 self.db_pswd = appConf.get('DATABASE','DBPSWD')
#             else:
#                 raise ConnectionError("Undefined database password. Set the db_pswd in app.cfg")


#             ''' --- SPARK --- DEPRECATED Moved to property
#                 set the spark home directory '''
#             if not (sparkPath or appConf.get('SPARK','SPARKHOMEDIR')):
#                 raise ValueError("Spark directory required to proceed. \
#                                 Must be specified in app_config.py or \
#                                 spark_path %s must be valid" % sparkPath)
#             if sparkPath:
#                 ''' override config.spark_install_director '''
#                 ''' TODO validate spark_dir '''
#                 self.spark_dir = sparkPath
#             else:
#                 self.spark_dir = appConf.get('SPARK','SPARKHOMEDIR')
            
#             findspark.init(self.spark_dir)
#             from pyspark.sql import SparkSession
#             logger.info("Importing %s library from spark dir: %s" % (SparkSession.__name__, self.spark_dir))

#             ''' set the db_type specific jar '''
#             if not appConf.get('SPARK','SPARKJARDIR'):
#                 raise ConnectionError("Spark requires a valid jar file to use with %s" % self.db_type)
#             self.spark_jar = appConf.get('SPARK','SPARKJARDIR')
#             logger.info("Defining Spark Jar dir: %s" % (self.spark_jar))

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

#                 self.spark_session=SparkSession(sc)
                
#             elif kwargs['DATA_STORE']=="LOCAL":
#                 print("setting up spark session for local files")
#             self.spark_session = SparkSession \
#                                 .builder \
#                                 .appName(self.__app__) \
#                                 .config("spark.jars", self.spark_jar) \
#                                 .getOrCreate()
#             else:
#                 raise ValueError("Invalid DATA_STORE value defined to set the spark session")

#             logger.info("Starting a Spark Session: %s" % (self.spark_session))

            ''' build the url for db connection '''
#             self.spark_url = "jdbc:"+self.db_type+"://"+self.host_ip+":"+self.db_port+"/"+self.db_name
            logger.info("Defined spark database connection url: %s" % (self.spark_url))

            logger.info("Connection complete! ready to load data.")
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <__init__>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None

    
        ''' Function --- SPARK SESSION ---
            name: session @property and @setter functions
            parameters:

            procedure: 
            return self._session

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def session(self):
        return self._session

    @session.setter
    def session(self,session_args:dict={}):

        __s_fn_id__ = "function <@session.setter>"

        try:
            ''' 
                set the spark home directory '''
            if "HOMEDIR" in session_args.keys():
                _spark_home_dir = session_args['HOMEDIR']
            elif appConf.has_option('SPARK','HOMEDIR'):
                _spark_home_dir = appConf.get('SPARK','HOMEDIR')
            else:
                raise AttributeError("Spark home directory is required to proceed "+ \
                                "It must be specified in app.cfg or "+ \
                                "passed as a **session_args key value pair")
            
            findspark.init(_spark_home_dir)
            from pyspark.sql import SparkSession
            logger.debug("Importing %s library from spark dir: %s"
                         % (SparkSession.__name__, _spark_home_dir))

            if "CONFIG" in session_args.keys():
                _conf_opt = session_args['CONFIG']
            elif appConf.has_option('SPARK','CONFIG'):
                _conf_opt = appConf.get('SPARK','CONFIG')
            else:
                _conf_opt = "spark.jars"

            ''' set master cluster setup local[x], yarn or mesos '''
            if "MASTER" in session_args.keys():
                _master = session_args['MASTER']
            elif appConf.has_option('SPARK','MASTER'):
                _master = appConf.get('SPARK','MASTER')
            else:
                _master = "local[1]"     

            if "APPNAME" in session_args.keys():
                _app_name = session_args['APPNAME']
            elif appConf.has_option('SPARK','APPNAME'):
                _app_name = appConf.get('SPARK','APPNAME')
            else:
                _app_name = self.__app__     

            ''' set the db_type specific jar '''
            if "JARDIR" in session_args.keys():
                _jar_dir = session_args['JARDIR']
            elif appConf.has_option('SPARK','JARDIR'):
                _jar_dir = appConf.get('SPARK','JARDIR')
            else:
                _jar_dir = None
#                 raise ConnectionError("Spark requires a valid jar file to use with %s" % self.db_type)
#             self.spark_jar = appConf.get('SPARK','JARDIR')
#             logger.info("Defining Spark Jar dir: %s" % (self.spark_jar))

            if _jar_dir is None:
                self._session = SparkSession \
                                    .builder \
                                    .master(_master) \
                                    .appName(_app_name) \
                                    .getOrCreate()
            else:
                self._session = SparkSession \
                                    .builder \
                                    .master(_master) \
                                    .appName(_app_name) \
                                    .config(_conf_opt, self.spark_jar) \
                                    .getOrCreate()
                
            logger.info("Starting a Spark Session: %s" % (self._session))

            ''' TODO Deprate after all functions are working '''
            self.spark_session=self._session


        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._session


    ''' Function
            name: reset_type to the original data type
            parameters:

            procedure: 
            return self._dbConnURL

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def dbConnURL(self):
        return self._dbConnURL

    @dbConnURL.setter
    def dbConnURL(self,**kwargs):

        try:
            ''' --- DATABASE ---
                set the host IP '''
            self.host_ip = None
            if "HOSTIP" in kwargs.keys():
                self.host_ip = kwargs['HOSTIP']
            elif appConf.get('HOSTS','HOSTIP'):
                self.host_ip = appConf.get('HOSTS','HOSTIP')
            else:
                raise ConnectionError("Undefined host IP. Set the host_ip in app.cfg")

            ''' set the database type '''
            self.db_type = None
            if "DBTYPE" in kwargs.keys():
                self.db_type = kwargs['DBTYPE']
            elif appConf.get('DATABASE','DBTYPE'):
                self.db_type = appConf.get('DATABASE','DBTYPE')
            else:
                raise ConnectionError("Undefined database type. Set the db_type in app.cfg")

            ''' set the database port '''
            if "DBPORT" in kwargs.keys():
                self.db_port = kwargs['DBPORT']
            elif appConf.get('DATABASE','DBPORT'):
                self.db_port = appConf.get('DATABASE','DBPORT')
            else:
                raise ConnectionError("Undefined database port. Set the db_port in app.cfg")

            ''' set the database driver '''
            if "DBDRIVER" in kwargs.keys():
                self.db_driver = kwargs['DBDRIVER']
            elif appConf.get('DATABASE','DBDRIVER'):
                self.db_driver = appConf.get('DATABASE','DBDRIVER')
            else:
                raise ConnectionError("Undefined database password. Set the db_driver in app.cfg")

            ''' set the database name '''
            if "DBNAME" in kwargs.keys():
                self.db_name = kwargs['DBNAME']
            elif appConf.get('DATABASE','DBNAME'):
                self.db_name = appConf.get('DATABASE','DBNAME')
            else:
                raise ConnectionError("Undefined database name. Set the db_name in app.cfg")

            ''' set the database schema '''
            if "DBSCHEMA" in kwargs.keys():
                self.db_schema = kwargs['DBSCHEMA']
            elif appConf.get('DATABASE','DBSCHEMA'):
                self.db_schema = appConf.get('DATABASE','DBSCHEMA')
            else:
                raise ConnectionError("Undefined database schema. Set the db_schema in app.cfg")

            ''' set the database username '''
            if "DBUSER" in kwargs.keys():
                self.db_user = kwargs['DBUSER']
            elif appConf.get('DATABASE','DBUSER'):
                self.db_user = appConf.get('DATABASE','DBUSER')
            else:
                raise ConnectionError("Undefined database username. Set the db_user in app.cfg")

            ''' set the database password '''
            if "DBPSWD" in kwargs.keys():
                self.db_pswd = kwargs['DBPSWD']
            elif appConf.get('DATABASE','DBPSWD'):
                self.db_pswd = appConf.get('DATABASE','DBPSWD')
            else:
                raise ConnectionError("Undefined database password. Set the db_pswd in app.cfg")

            self._dbConnURL = "jdbc:"+self.db_type+"://"+self.host_ip+":"+self.db_port+"/"+self.db_name

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dbConnURL


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
    def data(self,data):

        __s_fn_id__ = "function <@data.setter>"
        
#         self.spark_session.conf.set("spark.sql.execution.arrow.enabled","true")

        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")

            if not isinstance(data,DataFrame):
                self._data = self.session.createDataFrame(data)
                self.dType = 'OTHER'
            else:
                self._data = data
                self.dType = 'SDF'
#             elif isinstance(data, pd.DataFrame) and not data.empty:
#                 ''' pandas dataframe convert to pyspark DataFrame '''
#                 self.dType = 'PANDAS'
#                 self._data = self.spark_session.createDataFrame(data)
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
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data

    ''' Function
            name: reset_type to the original data type
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def reset_dtype(self,data):
        
        __s_fn_id__ = "function <reset_dtype>"
        reset_data = None

        try:
            if self.dType == 'RDD':
                reset_data=self.data
            elif self.dType == 'PANDAS':
                reset_data=self.data.toPandas()
            elif self.dType == 'DICT':
                print('Method to be done')
            elif self.dType == 'ARRAY':
                print('Method to be done')
            else:
                raise RuntimeError("Something went wrong?")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return reset_data


    ''' Function
            name: dType @property and @setter functions
            parameters:

            procedure: 
            return self._dType

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def dType(self):
        return self._dType

    @dType.setter
    def dType(self,data_type:str):

        __s_fn_id__ = "function <@dType.setter>"

        try:
            if data_type is None and not data_type in self._dTypeList:
                raise AttributeError('Invalid data_type or is set to empty string')
            self._dType = data_type

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dType


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

            _csv_to_sdf = self.spark_session.read.options( \
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
        
#         _csv_to_sdf = self.spark_session.sparkContext.emptyRDD()     # initialize the return var
# #         _tmp_df = self.spark_session.sparkContext.emptyRDD()
#         _start_dt = None
#         _end_dt = None
#         _sdf_cols = []
#         _l_cols = []
#         _traceback = None
        
#         _s_fn_id = "function <read_s3csv_to_sdf>"
#         logger.info("Executing %s in %s",_s_fn_id, __name__)

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
# #             print(self.spark_session.__dict__)
# #             self.spark_session.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
# #             self.spark_session.sparkContext\
# #                     .hadoopConfiguration.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
# #             self.spark_session.sparkContext\
# #                   .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

# #             os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
            
# #             conf = SparkConf().set('spark.executor.extraJavaOptions', \
# #                                    '-Dcom.amazonaws.services.s3.enableV4=true')\
# #                             .set('spark.driver.extraJavaOptions', \
# #                                  '-Dcom.amazonaws.services.s3.enableV4=true')\
# #                             .setAppName('pyspark_aws')\
# #                             .setMaster('local[*]')
            
# #             sc=SparkContext(conf=conf)
# # #             sc=self.spark_session.sparkContext(conf=conf)
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
# #             _csv_to_sdf=self.spark_session.read.csv(
#             _csv=self.spark_session.read.csv(
#                 obj,
# #                 _s3_obj,
#                 header=True,
#                 inferSchema=True)
# #             _csv_to_sdf = self.spark_session.read.csv(_s3_obj)

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return _csv_to_sdf