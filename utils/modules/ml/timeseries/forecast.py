#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "ForecastModels"
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

class ForecastModels():
    """ ***************************************************************************************
    CLASS executes several time series prediction models and computes the errors and accuracies
    
    The overall function here is to provide any timeseries dataset, in the form of a pyspark/pandas
    dataframe, dictionary list, or an array, to perform a timeseries seasonal predictions.
    
    When the class is instantiated, it will inherit properties and methods from packages 
    (1) rezaware - application specific configuration and logging functions
    
    Resources:
    https://towardsdatascience.com/3-unique-python-packages-for-time-series-forecasting-2926a09aaf5b
    
    contributors:
        * nuwan.waidyanatha@rezgateway.com
        
    ******************************************************************************************* 
    """


    def __init__(self, desc : str="data", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__desc__ = desc
        __s_fn_id__ = "__init__"
        
        self._session = None   # spark session property
        self._data = None    # dataframe property
        self._startDT = None
        self._endDT = None
        self._dtAttr = None  # timestamp property
        self._partAttr=None  # partitionBy column name
        self._winSpecUnits = ['MINUTE','HOUR','DAY']
        self._winSpec = {
            'LENGTH':7,   # set default value to 7 days
            'UNIT':'DAY'  # rolling window
        }

        global config
        global logger
        global clsSpark

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
            from utils.modules.lib.spark import execSession as session
            clsSpark = session.Spawn(desc=self.__desc__)

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

    ''' --- CLASS PROPERTIES GET & SET ---
    
    author(s): samana.thetha@gmail.com
    '''

    @property
    def session(self):
        """
        Description:

        Atributes:

        Returns:
            self._session (SparkSession)
        """

        __s_fn_id__ = "function <@property session>"

        try:
            if self._session is None or self._session=={}:
                clsSpark.session={}

                self._session = clsSpark.session

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._session

    @session.setter
    def session(self,session):
        """
        Description:

        Atributes:

        Returns:
            self._session (SparkSession)
        """
        
        __s_fn_id__ = "function <@session.setter>"

        try:
            ''' TODO validate if active spark session '''
            if not session is None:
                self._session = session

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._session

    ''' --- DATA --- '''
    @property
    def data(self) -> DataFrame:
        """
        Description:

        Atributes:

        Returns:
            self._data (DataFrame)
        """
        
        __s_fn_id__ = "function <@propert data>"
        
        try:
            if self._data is None:
                raise ValueError("Data is of NoneType; cannot be used in any %s %s computations"
                                 % (self.__package__,self.__name__))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data) -> DataFrame:
        """
        Description:

        Atributes:

        Returns:
            self._data (DataFrame)
        """

        __s_fn_id__ = "function <@data.setter>"

        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")

            if not isinstance(data,DataFrame):
                self._data = self.session.createDataFrame(data)
                logger.debug("Data of dtype %s converted to pyspark DataFrame", type(data))
            else:
                self._data = data
                logger.debug("Class property data is a pyspark DataFrame")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data

    ''' --- START DATETIME --- '''
    @property
    def startDateTime(self):
        """
        Description:

        Atributes:

        Returns:
            self._startDateTime (timestamp)
        """
        if self._startDT is None and isinstance(self.data,DataFrame):
            self._startDT = self.data.select(F.min(self.datetimeAttr)).collect()

        return self._startDT

    @startDateTime.setter
    def startDateTime(self,start_date_time=None):
        """
        Description:
            Define the overall timeseries window boundaries. If undefined, the default
            will consider the Min datetime as the startDateTime and the Max datetime 
            as the endDateTime
        Atributes:
            start_date_time (timestamp)
        Returns:
            self._startDateTime (timestamp)
        """

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

    ''' --- END DATETIME --- '''
    @property
    def endDateTime(self):
        """
        Description:

        Atributes:

        Returns:
            self._endDateTime (timestamp)
        """
        if self._endDT is None and isinstance(self.data,DataFrame):
            self._endDT = self.data.select(F.max(self.datetimeAttr)).collect()
        return self._endDT

    @endDateTime.setter
    def endDateTime(self,end_date_time:datetime=datetime.now()):
        """
        Description:

        Atributes:

        Returns:
            self._endDateTime (timestamp)
        """

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

    ''' --- DATETIME ATTRIBUTE --- '''
    @property
    def datetimeAttr(self):
        """
        Description:

        Atributes:

        Returns:
            self._datetimeAttr (timestamp)
        """

        __s_fn_id__ = "function <@property datetimeAttr>"

        try:
            if self._dtAttr is None: # or \
                ''' TODO validate attribute; if invalid attempt 
                to find a datetime column in dataframe'''

                logger.debug("The datetimeAttr was not explicitly set as a valid "+ \
                            "DateType or TimestampType and will try to set the first"+ \
                            "found valid column %s",self.data.dtypes)

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
        """
        Description:

        Atributes:

        Returns:
            self._datetimeAttr (timestamp)
        """

        __s_fn_id__ = "function <@datetimeAttr.setter>"

        try:
            if self.data is None or self.data.count()<=0:
                raise ValueError("The dataset property must be defined before setting the datetimeAttr")
            if "".join(date_time_attr.split())!="" or \
                    self.data.filter(F.col(date_time_attr).cast("Timestamp").isNotNull()).count()>0:
                self._dtAttr = date_time_attr
            else:
                raise AttributeError("The datetimeAttribute cannot be an empty string")
            ''' cast the datetime attr to a timestamp '''
            self.data = self.data.withColumn(self._dtAttr,F.to_timestamp(self._dtAttr))
            logger.debug("Cast column %s to timestamp",self._dtAttr)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._dtAttr

    @property
    def partitionAttr(self) -> str:
        """
        Description:
            Gets and returns the partition column name
        Attribute:

        Returns:
            self._partAttr
        """
        try:
            if self._partAttr is None:
                logger.warning("No partition column set")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._partAttr

    
    def partitionAttr(self,partition_attr) -> str:
        """
        Description:
            Gets and returns the partition column name
        Attribute:

        Returns:
            self._partAttr
        """

        __s_fn_id__ = "function <@property windowSpec>"

        try:
            if partition_attr is not None or "".join(partition_attr.split())!="":
                self._partAttr=partition_attr
                logger.debug("Set partition column attribute name as %s",self._partAttr)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._partAttr


    ''' --- WINDOW SPECIFICATION --- '''
    @property
    def windowSpec(self) -> int:
        """
        Description:
            If the property is None, will set to a default 7 DAY window specification.
            Requires a valid datetimeAttr.
        Atributes:
            None
        Returns:
            self._winSpec (int)
        """

        __s_fn_id__ = "function <@property windowSpec>"
        __win_len__ = 7

        try:
            ''' function to calculate number of seconds from number of days '''
            if self._winSpec is None:
                days = lambda i: i * 86400
                self._winSpec = Window \
                    .orderBy(F.col(self.datetimeAttr).cast('long')) \
                    .rangeBetween(-days(__win_len__),0)
                logger.debug("Class property winSpec was not explicitly set "+\
                             "Setting to $d DAY",__win_len__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._winSpec

    @windowSpec.setter
    def windowSpec(self,window_spec:dict={}) -> int:
        """
        Description:
            With two key value pairs that defines an integer value of the window
            lengthe (LENGTH) and the unit of measure (UNIT) that can only be 
            [MINUTE, HOUR, DAY]. The datetimeAttr is required to 
        Atributes:
            window_spec(dict) - LENGTH (int) and UNIT (str) MINUTE, HOUR, DAY 
        Returns:
            self._winSpec (int)
        """

        __s_fn_id__ = "function <@windowSpec.setter>"

        try:
            if not len(window_spec)>0 or \
                not 'LENGTH' in window_spec.keys() or \
                not 'UNIT' in window_spec.keys():
                raise AttributeError("Missing one or more keys LENGTH and UNIT input parameters")
            if not (isinstance(window_spec['LENGTH'],int) or window_spec['LENGTH'] > 0):
                raise AttributeError("The window_spec LENGTH must be of dtype int > 0")
            if window_spec['UNIT'] not in self._winSpecUnits:
                raise AttributeError("Invalid rolling window UNIT %s must be %s" 
                                     %(window_spec['UNIT'],
                                       self._winSpecUnits))
            if self.datetimeAttr is None:
                raise AttributeError("A valid datetimeAttr property must be specified to proceed")
            
            ''' function to calculate number of seconds from number of days '''
            if window_spec['UNIT'] == "DAY":
                _time_attr = lambda i: i * 86400
            elif window_spec['UNIT'] == "HOUR":
                _time_attr = lambda i: i * 3600
            elif window_spec['UNIT'] == "MINUTE":
                _time_attr = lambda i: i * 60
            else:
                raise RuntimeError("Something was wrong")

            if self.partitionAttr:
                self._winSpec = Window \
                    .partitionBy(self.partitionAttr) \
                    .orderBy(F.col(self.datetimeAttr).cast('long')) \
                    .rangeBetween(-_time_attr(window_spec['LENGTH']),0)
            else:
                self._winSpec = Window \
                    .orderBy(F.col(self.datetimeAttr).cast('long')) \
                    .rangeBetween(-_time_attr(window_spec['LENGTH']),0)

            logger.debug("WindowSpec set to %s ", str(self._winSpec))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._winSpec


    ''' --- RUN FORECAST --- 
    
    author(s): samana.thetha@gmail.com
    '''
    def get_forecast(
        self,
        ts_data:DataFrame=None,
        date_attr:str="",
        from_datetime:datetime=datetime.now(),
        to_datetime: datetime=datetime.now()+timedelta(days=7),
    )->DataFrame:
        """
        Description:
        Attributes:
        Returns:
        """

        __s_fn_id__ = "function <get_forecast>"

        try:
            self.data = ts_data
            datetimeAttr=date_attr

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._data


    