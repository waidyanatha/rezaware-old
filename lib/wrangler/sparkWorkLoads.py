#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import sys
    sys.path.insert(1, './wrangler')
    import app_config as config
    import importlib
    config = importlib.reload(config)
    import os
    from pyspark.sql.functions import split, col,substring,regexp_replace, lit, current_timestamp
    import findspark
#    import pandas as pd
#    from pandas.api.types import is_datetime64_any_dtype as is_datetime
#    import calendar
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

    '''
    def __init__(self, sparkPath:str=None, **kwargs:dict):

        ''' Initialize the DB connection parameters '''
        self.host_ip = None
        self.db_type = None
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
            if "hostIP" in kwargs.keys():
                self.host_ip = kwargs['hostIP']
            elif config.host_ip:
                self.host_ip = config.host_ip
            else:
                raise ConnectionError("Undefined host IP. Set the host_ip in app_config.py")

            ''' set the database type '''
            if "dbType" in kwargs.keys():
                self.db_type = kwargs['dbType']
            elif config.db_type:
                self.db_type = config.db_type
            else:
                raise ConnectionError("Undefined database type. Set the db_type in app_config.py")

            ''' set the database port '''
            if "dbPort" in kwargs.keys():
                self.db_port = kwargs['dbPort']
            elif config.db_port:
                self.db_port = config.db_port
            else:
                raise ConnectionError("Undefined database port. Set the db_port in app_config.py")

            ''' set the database driver '''
            if "dbDriver" in kwargs.keys():
                self.db_driver = kwargs['dbDriver']
            elif config.db_driver:
                self.db_driver = config.db_driver
            else:
                raise ConnectionError("Undefined database password. Set the db_driver in app_config.py")

            ''' set the database name '''
            if "dbName" in kwargs.keys():
                self.db_name = kwargs['dbName']
            elif config.db_name:
                self.db_name = config.db_name
            else:
                raise ConnectionError("Undefined database name. Set the db_name in app_config.py")

            ''' set the database schema '''
            if "dbSchema" in kwargs.keys():
                self.db_schema = kwargs['dbSchema']
            elif config.db_schema:
                self.db_schema = config.db_schema
            else:
                raise ConnectionError("Undefined database schema. Set the db_schema in app_config.py")

            ''' set the database username '''
            if "dbUser" in kwargs.keys():
                self.db_user = kwargs['dbUser']
            elif config.db_user:
                self.db_user = config.db_user
            else:
                raise ConnectionError("Undefined database username. Set the db_user in app_config.py")

            ''' set the database password '''
            if "dbPswd" in kwargs.keys():
                self.db_pswd = kwargs['dbPswd']
            elif config.db_pswd:
                self.db_pswd = config.db_pswd
            else:
                raise ConnectionError("Undefined database password. Set the db_pswd in app_config.py")


            ''' --- SPARK ---
                set the spark home directory '''
            if not (sparkPath or config.spark_home):
                raise ValueError("Spark directory required to proceed. \
                                Must be specified in app_config.py or \
                                spark_path %s must be valid" % sparkPath)
            if sparkPath:
                ''' override config.spark_install_director '''
                ''' TODO validate spark_dir '''
                self.spark_dir = sparkPath
            else:
                self.spark_dir = config.spark_home
            
            findspark.init(self.spark_dir)
            from pyspark.sql import SparkSession

            ''' set the db_type specific jar '''
            if not config.spark_jar:
                raise ConnectionError("Spark requires a valid jar file to use %s" % config.db_type)
            self.spark_jar = config.spark_jar

            ''' the Spark session should be instantiated as follows '''
            self.spark_session = SparkSession \
                    .builder \
                    .appName("rezaware wrangler") \
                    .config("spark.jars", self.spark_jar) \
            .getOrCreate()

            ''' build the url for db connection '''
            self.spark_url = "jdbc:"+self.db_type+"://"+self.host_ip+":"+self.db_port+"/"+self.db_name

            print("Connection complete! ready to load data.")

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <__init__>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None

    ''' Function
            name: get_spark_session
            parameters:
                    @name (str)
                    @enrich (dict)
            procedure: 

            return DataFrame

    '''
    def DEPRECATED_get_spark_session(self, **kwargs):

        try:
            ''' the Spark session should be instantiated as follows '''
            self.spark_session = SparkSession \
                                    .builder \
                                    .appName("rezaware wrangler") \
                                    .config("spark.jars", self.spark_jar) \
                                    .getOrCreate()

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <get_spark_session>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self.spark_session

    ''' Function
            name: get_data_from_table
            parameters:
                    @name (str)
                    @enrich (dict)
            procedure: 

            return DataFrame

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
#            load_sdf.printSchema()

            print("Loading complete!")

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <get_data_from_table>"
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

    '''
    def insert_sdf_into_table(self, save_sdf, dbTable:str, **kwargs):
        
        _num_records_saved = 0
        
        try:
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

    '''
    def read_csv_to_sdf(self,filesPath: str, **kwargs):

        _csv_to_sdf = self.spark_session.sparkContext.emptyRDD()     # initialize the return var
        _tmp_df = self.spark_session.sparkContext.emptyRDD()
        _start_dt = None
        _end_dt = None
        _sdf_cols = []
        _l_cols = []
        try:
            ''' check if the folder and files exists '''
            if not filesPath:
                raise ValueError("Invalid folder path %s" % filesPath)
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

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <read_folder_csv_to_sdf>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _csv_to_sdf
        

          