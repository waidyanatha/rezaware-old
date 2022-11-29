#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "RollingStats"
__module__ = "ml"
__package__ = "timeseries"
__app__ = "utils"
__ini_fname__ = "app.ini"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' standard python packages '''
    import os
    import sys
    import logging
    import traceback
    import configparser
    import functools
#     import pandas as pd
    from datetime import datetime, date, timedelta
#     from pyspark.sql.functions import udf, month, dayofweek
    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame
    from pyspark.sql.window import Window
    from pyspark.sql.types import TimestampType,DateType,StructType,StructField,StringType
#     from pyspark.sql import DataFrame

    print("All packages in %s %s %s %s imported successfully!"
          % (__app__,__module__,__package__,__name__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

''' *******************************************************************************************
    CLASS spefic to calculation moving average, standard deviation workloads
    
    The overall function here is to provide any timeseries dataset, in the form of a
    pandas dataframe, dictionary or an array, to perform a moving average and/or 
    standard deviation of any time minute, hour, or day time window.
    
    We implement pyspark to perform the rolling average and standard devition. When the
    class is instantiated, it will inherit properties and methods from packages 
    (1) utils/etl/load/sparkwls - to read/write data from SQL/NoSQL DBs and CVS/JSON files
    (2) utils/etl/load/filesrw - to read/write files stored in local, remote, or cloud storage
    (3) rezaware - application specific configuration and logging functions
    
    The properties implement
    * data [rdd] - in the form of a pyspark dataframe and any other dataframe will be converted
            into a resilient distributed dataset (rdd). However, the processed data is returned
            in the original dtype
    * datetimeAttr [str/int] - tells the class which of the columns is to be considered as the
            datetime of the timeseries. If unspecified, then the first detected datetime column
            is considered
    * startDateTime/endDateTime [datetime] - define the overall timeseries window boundaries. If
            undefined, the default will consider the Min datetime as the startDateTime and the
            Max datetime as the endDateTime
    * rollingWindow [dict] - with two key value pairs that defines an integer value of the window
            lengthe (LENGTH) and the unit of measure (UNIT) that can only be [MINUTE, HOUR, DAY]

    contributors:
        * nuwan.waidyanatha@rezgateway.com
        
    ******************************************************************************************* 
'''

class RollingStats():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
            
            resources: to implement s3 logger - 

    '''
    def __init__(self, desc : str="data", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__desc__ = desc
        __s_fn_id__ = "__init__"
        
        self._spark = None   # spark session property
        self._data = None    # dataframe property
        self._startDT = None
        self._endDT = None
        self._dtAttr = None  # timestamp property
        self._winSpecUnits = ['MINUTE','HOUR','DAY']
        self._winSpec = {
            'LENGTH':7,   # set default value to 7 days
            'UNIT':'DAY'  # rolling window
        }

        global config
        global logger
#         global clsSparkWL

        try:
            self.cwd=os.path.dirname(__file__)
            sys.path.insert(1,self.cwd)

            config = configparser.ConfigParser()
            config.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = config.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)

            from rezaware import Logger as logs
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

            ''' initialize util class to use common functions '''
#             from utils.modules.etl.load import sparkwls as spark
#             clsSparkWL = spark.SparkWorkLoads(desc=self.__desc__)
#             if clsSparkWL.session is None:
#                 clsSparkWL.session = {}
#             self.spark = clsSparkWL.session

            ''' Set the utils root directory '''
            self.pckgDir = config.get("CWDS",self.__package__)
            self.appDir = config.get("CWDS",self.__app__)

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return None

    ''' Function
            name: data @property and @setter functions
            parameters:

            procedure: uses the clsSparkWL data@setter method to convert the data
                        to a pyspark DataFrame
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def spark(self):
        try:
            if self._spark is None:
                from utils.modules.etl.load import sparkwls as spark
                clsSparkWL = spark.SparkWorkLoads(desc=self.__desc__)
                if clsSparkWL.session is None:
                    clsSparkWL.session = {}
                self._spark = clsSparkWL.session

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._spark

    @spark.setter
    def spark(self,session):

        try:
            ''' TODO validate if active spark session '''
            if not session is None:
                self._spark = session

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._spark

    
    ''' Function
            name: data @property and @setter functions
            parameters:

            procedure: uses the clsSparkWL data@setter method to convert the data
                        to a pyspark DataFrame
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):
        return self._data

    @data.setter
#     def data(self,data:DataFrame=clsSparkWL.spark_session.sparkContext.emptyRDD()):
    def data(self,data):

        __s_fn_id__ = "function <@data.setter>"
#         clsSparkWL.data=data
#         self._data = clsSparkWL.data
        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")

            if not isinstance(data,DataFrame):
                self._data = self.session.createDataFrame(data)
            else:
                self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data


    ''' Function
            name: startDateTime @property and @setter functions
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def startDateTime(self):
        if self._startDT is None and not self.data.isEmpty():
            self._startDT = self.data.select(F.min(self.datetimeAttr)).collect()

        return self._startDT

    @startDateTime.setter
    def startDateTime(self,start_date_time=None):

        __s_fn_id__ = "function <@startDateTime.setter>"

        try:
            if start_date_time is None or not isinstance(start_date_time,datetime):
                raise AttributeError("Invalid datetime input parameter")
            self._startDT = start_date_time

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._startDT


    ''' Function
            name: endDateTime @property and @setter functions
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def endDateTime(self):
        if self._endDT is None and not self.data.isEmpty():
            self._endDT = self.data.select(F.max(self.datetimeAttr)).collect()
        return self._endDT

    @endDateTime.setter
    def endDateTime(self,end_date_time:datetime=datetime.now()):

        __s_fn_id__ = "function <@endDateTime.setter>"

        try:
            if end_date_time is None or not isinstance(end_date_time,datetime):
                raise AttributeError("Invalid datetime input parameter")
            self._endDT = start_date_time

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._endDT


    ''' Function
            name: datetimeAttr @property and @setter functions
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def datetimeAttr(self):

        __s_fn_id__ = "function <@datetimeAttr.setter>"

        try:
            if self._dtAttr is None: # or \
                ''' TODO validate attribute; if invalid attempt 
                to find a datetime column in dataframe'''
#                 self.data.schema[self._dtAttr].dataType !=DateType or \
#                 self.data.schema[self._dtAttr].dataType !=TimestampType:# or \
#                 self.data.schema[self._dtAttr].dataType !=date or \
#                 self.data.schema[self._dtAttr].dataType !=datetime:

                logger.info("The datetimeAttr was not explicitly set as a valid "+ \
                            "DateType of TimestampType and will try to set the first"+ \
                            "found valid column")
                print(self.data.dtypes)
                _dt_attr_list = next(
                    (x for x, y in self.data.dtypes 
                     if y==DateType or y==TimestampType),# or y==date or y==datetime),
                    None)
                print(_dt_attr_list)
                if _dt_attr_list is None:
                    raise AttributeError("Could not locate a valid datetime attribute "+ \
                                         "in the dataset with columns %s" \
                                         % str(self.data.dtypes))
                else:
                    self._dtAttr = _dt_attr_list[0]

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dtAttr


    @datetimeAttr.setter
    def datetimeAttr(self,date_time_attr:str=''):

        __s_fn_id__ = "function <@datetimeAttr.setter>"

        try:
            if self.data is None:
                raise ValueError("The dataset property must be defined before setting the datetimeAttr")
            if date_time_attr!='':
                self._dtAttr = date_time_attr
                self.data = self.data.withColumn(
                    self._dtAttr,
                    self.data[self._dtAttr] \
                        .cast('timestamp'))
            else:
                raise AttributeError("The datetimeAttribute cannot be an empty string")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dtAttr


    ''' Function
            name: windowSpec @property and @setter functions
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def windowSpec(self):
        return self._winSpec

    @windowSpec.setter
    def windowSpec(self,window_spec:dict={}):

        __s_fn_id__ = "function <@windowSpec.setter>"

        try:
            if not len(window_spec)>0 or \
                not 'LENGTH' in window_spec.keys() or \
                not 'UNIT' in window_spec.keys():
                raise AttributeError("Missing one or more keys LENGTH and UNIT")
            if not (isinstance(window_spec['LENGTH'],int) or window_spec['LENGTH'] > 0):
                raise AttributeError("The window_spec LENGTH must be of dtype int > 0")
            if window_spec['UNIT'] not in self._winSpecUnits:
                raise AttributeError("Invalid rolling window UNIT %s must be %s" 
                                     %(window_spec['UNIT'],
                                       self._winSpecUnits))
            if self.datetimeAttr is None:
                raise AttributeError("A valid datetimeAttr property must be specified to proceed")
            
            #function to calculate number of seconds from number of days
            days = lambda i: i * 86400

            if window_spec['UNIT'] == "DAY":
                self._winSpec = Window \
                    .orderBy(F.col(self.datetimeAttr).cast('long')) \
                    .rangeBetween(-days(window_spec['LENGTH']),0)
            elif window_spec['UNIT'] == "HOUR":
                print("method TBD")
            elif window_spec['UNIT'] == "MINUTE":
                print("method TBD")
            else:
                raise RuntimeError("Something was wrong")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._winSpec

    ''' Function
            name: simple_moving_stats functions
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def rollingstat(func):
        
        @functools.wraps(func)
        def calc_roll_stat(
            self,
            column:str='',   # column name to apply the rolling computation
            stat_op:str="mean", # stat operation sum, mean or standard deviation
            data=None,   # data set
            **kwargs,    #
        ):
            __s_fn_id__ = "function <roll_stat_wrapper>"

            try:
                _roll_col_name = func(
                    self,
                    column,
                    stat_op,
                    data,     
                    **kwargs
                )
                if stat_op.upper()=='MEAN':
                    self._data = self.data. \
                                    withColumn(_roll_col_name, F.mean(column). \
                                               over(self.windowSpec))
                elif stat_op.upper()=='STDV':
                    self._data = self.data. \
                                    withColumn(_roll_col_name, F.stddev(column). \
                                               over(self.windowSpec))
                elif stat_op.upper()=='SUM':
                    self._data = self.data. \
                                    withColumn(_roll_col_name, F.sum(column). \
                                               over(self.windowSpec))
                else:
                    raise AttributeError("Invalid stat operation %s" % stat_op)

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                print("[Error]"+__s_fn_id__, err)
                print(traceback.format_exc())

            return self._data
        return calc_roll_stat

    @rollingstat
    def simple_moving_stats(
        self,
        column:str='',   # column name to apply the rolling computation
        stat_op:str="mean", # stat operation sum, mean or standard deviation
        data=None,   # data set
        **kwargs,    # 
    ):

        __s_fn_id__ = "function <simple_moving_stats>"
        _winspec = {}
        _winspec['LENGTH']=7
        _winspec['UNIT']='DAY'

        try:
            if not data is None:
                self.data = data

            if self.data is None:
                raise AttributeError("There is no data to perform a rolling computation")

            ''' datetime column name '''
            if "DATETIMEATTR" in kwargs.keys() and not kwargs['DATETIMEATTR'] is None:
                self.datetimeAttr = kwargs['DATETIMEATTR']
            if self.datetimeAttr is None:
                raise AttributeError("Specify the datetime property to use")
            ''' Rolling window length '''
            if "WINLENGTH" in kwargs.keys() and not kwargs['WINLENGTH'] is None \
                and "WINUNIT" in kwargs.keys() and not kwargs['WINUNIT'] is None:
                _winspec['LENGTH']=kwargs['WINLENGTH']
                _winspec['UNIT']=kwargs['WINUNIT']
                self.windowSpec = _winspec
            elif self.windowSpec is None:
                ''' set default values '''
                self.windowSpec = _winspec
            else:
                pass
            ''' column name to apply rolling computation '''
            if column == '' and self.data.filter(F.col(column).cast("Long").isNotNull()):
                raise ValueError("A column name with int, long, double, or decimal"+ \
                                 "must be specified.")
            if "RESCOLUMN" in kwargs.keys() and kwargs['RESCOLUMN'] != '':
                _roll_col_name = kwargs['RESCOLUMN']
            else:
                _roll_col_name = "rolling_"+stat_op+"_"+column

#             #function to calculate number of seconds from number of days
#             days = lambda i: i * 86400

#             df = self.data.withColumn(self.datetimeAttr, self.data[self.datetimeAttr].cast('timestamp'))
#             df = self.data
#             windowSpec = Window.orderBy(F.col(self.datetimeAttr).cast('long')).rangeBetween(-days(7), 0)
            # Note the OVER clause added to AVG(), to define a windowing column.
            # 3. Create an over() function directly on the avg() function:
#             df2 = df.withColumn('rolling_seven_day_average', F.stddev("dollars").over(windowSpec))

#             self._data = self.data. \
#                     withColumn(_roll_col_name, F.stddev(column). \
#                                over(self.windowSpec)) 
#             self._data.show()

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return _roll_col_name

