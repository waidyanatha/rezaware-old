#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "awsS3wls"
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
    findspark.init()
    from pyspark.sql.functions import split, col,substring,regexp_replace, lit, current_timestamp
    from pyspark import SparkContext, SparkConf
    import pandas as pd
    import configparser    
    import logging
    import traceback

    print("All packages in %s loaded successfully!" % __package__)

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
class AWSS3WorkLoads():
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
        self._spark_session = None
        _s_fn_id = "__init__"

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

            ''' build the url for db connection '''
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

    ''' TODO move to sparkwls.py'''
    @property
    def spark_session(self):
        
        return self._spark_session

    @spark_session.setter
    def spark_session(self, mode='HADOOP'):
    
        _s_fn_id = "function @spark_session.setter"
        logger.debug("Set spark session with %s" % mode)

        try:
            if mode == 'HADOOP':
                self._spark_session = self.get_hadoop_session()
            elif mode == '':
                self._spark_session = self.get_local_session()

            logger.info("Starting a Spark Session: %s" % (self._spark_session))


        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <__init__>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._spark_session


    def get_hadoop_session(self):

        try:
            ''' Initialize spark connection parameters '''
            self.spark_dir = None
            self.spark_jar = None
            self.spark_url = None
            self.spark_session = None

            ''' Spark function parameters '''
            self.spark_save_mode = "Append"

            ''' --- SPARK ---
                set the spark home directory '''
            if not (sparkPath or appConf.get('SPARK','SPARKHOMEDIR')):
                raise ValueError("Spark directory required to proceed. \
                                Must be specified in app_config.py or \
                                spark_path %s must be valid" % sparkPath)
            if sparkPath:
                ''' override config.spark_install_director '''
                ''' TODO validate spark_dir '''
                self.spark_dir = sparkPath
            else:
                self.spark_dir = appConf.get('SPARK','SPARKHOMEDIR')
            
            findspark.init(self.spark_dir)
            from pyspark.sql import SparkSession
#             from pyspark import SparkConf
            logger.info("Importing %s library from spark dir: %s" % (SparkSession.__name__, self.spark_dir))

            ''' set the db_type specific jar '''
            if not appConf.get('SPARK','SPARKJARDIR'):
                raise ConnectionError("Spark requires a valid jar file to use with %s" % self.db_type)
            self.spark_jar = appConf.get('SPARK','SPARKJARDIR')
            logger.info("Defining Spark Jar dir: %s" % (self.spark_jar))
            ''' the Spark session should be instantiated as follows '''

            print("setting up spark session for AWS S3")
#             os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

            conf = SparkConf().set('spark.executor.extraJavaOptions', \
                                   '-Dcom.amazonaws.services.s3.enableV4=true')\
                            .set('spark.driver.extraJavaOptions', \
                                 '-Dcom.amazonaws.services.s3.enableV4=true')\
                            .setAppName(self.__app__)\
                            .setMaster('local[*]')

            sc=SparkContext(conf=conf)
            print(sc)
            sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

            hadoopConf = sc._jsc.hadoopConfiguration()
            hadoopConf.set('fs.s3a.access.key', pkgConf.get('AWSAUTH','ACCESSKEY'))
            hadoopConf.set('fs.s3a.secret.key', pkgConf.get('AWSAUTH','SECURITYKEY'))
            hadoopConf.set('fs.s3a.endpoint', pkgConf.get('AWSAUTH','REGION'))
            hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

            self._spark_session=SparkSession(sc)        

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <__init__>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._spark_session


    ''' Function
            name: read_s3obj_to_sdf
            parameters:
                bucketName (str) - s3 bucket name
                objPath (str) - s3 key that points to the objecy
            procedure: 
            return DataFrame

            author: <nuwan.waidyanatha@rezgateway.com>

    '''
    def read_s3csv_to_sdf(self,bucketName:str,keyFPath: str, **kwargs):

        import boto3
        
        _csv_to_sdf = self.spark_session.sparkContext.emptyRDD()     # initialize the return var
#         _tmp_df = self.spark_session.sparkContext.emptyRDD()
        _start_dt = None
        _end_dt = None
        _sdf_cols = []
        _l_cols = []
        _traceback = None
        
        _s_fn_id = "function <read_s3csv_to_sdf>"
        logger.info("Executing %s in %s",_s_fn_id, __name__)

        try:

            if not 'AWSAUTH' in pkgConf.sections():
                raise ValueError('Unable to find AWSAUTH keys and values to continue')
            
            AWS_ACCESS_KEY_ID = pkgConf.get('AWSAUTH','ACCESSKEY')
            AWS_SECRET_ACCESS_KEY = pkgConf.get('AWSAUTH','SECURITYKEY')
            AWS_REGION_NAME = pkgConf.get('AWSAUTH','REGION')

            s3 = boto3.resource(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION_NAME,
            )
#             response = s3.get_object(Bucket=bucketName, Key=str(key))
#             print(self.spark_session.__dict__)
#             self.spark_session.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
#             self.spark_session.sparkContext\
#                     .hadoopConfiguration.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
#             self.spark_session.sparkContext\
#                   .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

#             os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
            
#             conf = SparkConf().set('spark.executor.extraJavaOptions', \
#                                    '-Dcom.amazonaws.services.s3.enableV4=true')\
#                             .set('spark.driver.extraJavaOptions', \
#                                  '-Dcom.amazonaws.services.s3.enableV4=true')\
#                             .setAppName('pyspark_aws')\
#                             .setMaster('local[*]')
            
#             sc=SparkContext(conf=conf)
# #             sc=self.spark_session.sparkContext(conf=conf)
#             sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
            
#             hadoopConf = sc._jsc.hadoopConfiguration()
#             hadoopConf.set('fs.s3a.access.key', AWS_ACCESS_KEY_ID)
#             hadoopConf.set('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)
#             hadoopConf.set('fs.s3a.endpoint', AWS_REGION_NAME)
#             hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            
#             spark=SparkSession(sc)

#             Bucket=bucketName,
#             Key=keyFPath

#             s3 = boto3.resource('s3')
            bucket = s3.Bucket(str(bucketName))
            obj = bucket.objects.filter(Prefix=str(keyFPath))
#             response = s3.get_object(Bucket=bucketName, Key=str(keyFPath))
#             _s3_obj = "s3a://"+bucketName+"/"+objPath
#             _csv_to_sdf=spark.read.csv(
#             _csv_to_sdf=self.spark_session.read.csv(
            _csv=self.spark_session.read.csv(
                obj,
#                 _s3_obj,
                header=True,
                inferSchema=True)
#             _csv_to_sdf = self.spark_session.read.csv(_s3_obj)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _csv_to_sdf