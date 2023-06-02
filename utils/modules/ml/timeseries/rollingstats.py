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
    from pyspark.sql.types import TimestampType,DateType,DoubleType, StructType,StructField,StringType
    from pyspark.sql.functions import pandas_udf

    print("All packages in %s %s %s %s imported successfully!"
          % (__app__,__module__,__package__,__name__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

class RollingStats():
    """ ***************************************************************************************
    CLASS spefic to calculation moving average, standard deviation workloads
    
    The overall function here is to provide any timeseries dataset, in the form of a
    pandas dataframe, dictionary or an array, to perform a moving average and/or 
    standard deviation of any time minute, hour, or day time window.
    
    We implement pyspark to perform the rolling average and standard devition. When the
    class is instantiated, it will inherit properties and methods from packages 
    (1) utils/etl/load/sparkwls - to read/write data from SQL/NoSQL DBs and CVS/JSON files
    (2) utils/etl/load/filesrw - to read/write files stored in local, remote, or cloud storage
    (3) rezaware - application specific configuration and logging functions
    
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

        __s_fn_id__ = f"{self.__name__} function <__init__>"

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
#             if clsSparkWL.session is None:
#                 clsSparkWL.session = {}
#             self.session = clsSparkWL.session

            ''' Set the utils root directory '''
            self.pckgDir = config.get("CWDS",self.__package__)
            self.appDir = config.get("CWDS",self.__app__)

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s.",
                         self.__app__,self.__module__,self.__package__,
                         self.__name__,self.__desc__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None

    ''' Function
            name: data @property and @setter functions
            parameters:

            procedure: uses the clsSparkWL data@setter method to convert the data
                        to a pyspark DataFrame
    The properties implement
    * data [rdd] - in the form of a pyspark dataframe and any other dataframe will be converted
            into a resilient distributed dataset (rdd). However, the processed data is returned
            in the original dtype
    * datetimeAttr [str/int] - tells the class which of the columns is to be considered as the
            datetime of the timeseries. If unspecified, then the first detected datetime column
            is considered

            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def session(self):
        """
        Description:

        Atributes:

        Returns:
            self._session (SparkSession)
        """

        __s_fn_id__ = f"{self.__name__} function <@property session>"

        try:
            if self._session is None or self._session=={}:
                clsSpark.session={}
                
#                 from utils.modules.etl.load import sparkDBwls as spark
#                 clsSparkWL = spark.SparkWorkLoads(desc=self.__desc__)
#                 if clsSparkWL.session is None:
#                     clsSparkWL.session = {}
                self._session = clsSpark.session

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._session

    @session.setter
    def session(self,session):
        """
        Description:

        Atributes:

        Returns:
            self._session (SparkSession)
        """
        
        __s_fn_id__ = f"{self.__name__} function <@session.setter>"

        try:
            ''' TODO validate if active spark session '''
            if not session is None:
                self._session = session

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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
        
        __s_fn_id__ = f"{self.__name__} function <@propert data>"
        
        try:
            if self._data is None:
                raise ValueError("%s Data is of NoneType; cannot be used in any computations"
                                 % __s_fn_id__)
            if not isinstance(self._data,DataFrame):
                self._data = self.session.createDataFrame(self._data)
                logger.debug("%s Data converted to pyspark %s dtype",
                             __s_fn_id__,type(self._data))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
#     def data(self,data:DataFrame=clsSparkWL.spark_session.sparkContext.emptyRDD()):
    def data(self,data) -> DataFrame:
        """
        Description:

        Atributes:

        Returns:
            self._data (DataFrame)
        """

        __s_fn_id__ = f"{self.__name__} function <@data.setter>"
#         clsSparkWL.data=data
#         self._data = clsSparkWL.data

        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")

            if not isinstance(data,DataFrame):
                self._data = self.session.createDataFrame(data)
                logger.debug("%s Data of dtype %s converted to pyspark DataFrame",
                             __s_fn_id__,type(data))
            else:
                self._data = data
                logger.debug("%s Class property data is a pyspark DataFrame",__s_fn_id__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <@endDateTime.setter>"

        try:
            if self._startDT is None and not self.data.isEmpty():
                self._startDT = self.data.select(F.min(self.datetimeAttr)).collect()

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <@startDateTime.setter>"

        try:
            if start_date_time is None or not isinstance(start_date_time,datetime):
                raise AttributeError("Invalid datetime input parameter")
            self._startDT = start_date_time

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <@endDateTime.setter>"

        try:
            if self._endDT is None and not self.data.isEmpty():
                self._endDT = self.data.select(F.max(self.datetimeAttr)).collect()

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._endDT

    @endDateTime.setter
    def endDateTime(self,end_date_time:datetime=datetime.now()):
        """
        Description:

        Atributes:

        Returns:
            self._endDateTime (timestamp)
        """

        __s_fn_id__ = f"{self.__name__} function <@endDateTime.setter>"

        try:
            if end_date_time is None or not isinstance(end_date_time,datetime):
                raise AttributeError("Invalid datetime input parameter")
            self._endDT = start_date_time

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <@property datetimeAttr>"

        try:
            if self._dtAttr is None: # or \
                ''' TODO validate attribute; if invalid attempt 
                to find a datetime column in dataframe'''
#                 self.data.schema[self._dtAttr].dataType !=DateType or \
#                 self.data.schema[self._dtAttr].dataType !=TimestampType:# or \
#                 self.data.schema[self._dtAttr].dataType !=date or \
#                 self.data.schema[self._dtAttr].dataType !=datetime:

                logger.debug("%s The datetimeAttr was not explicitly set as a valid "+ \
                            "DateType or TimestampType and will try to set the first"+ \
                            "found valid column %s",__s_fn_id__,self.data.dtypes)
#                 print(self.data.dtypes)
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
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dtAttr

    @datetimeAttr.setter
    def datetimeAttr(self,date_time_attr:str=''):
        """
        Description:

        Atributes:

        Returns:
            self._datetimeAttr (timestamp)
        """

        __s_fn_id__ = f"{self.__name__} function <@datetimeAttr.setter>"

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
            logger.debug("%s Cast column %s to timestamp",__s_fn_id__,self._dtAttr)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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
        
        __s_fn_id__ = f"{self.__name__} function <@property partitionAttr>"
        
        try:
            if self._partAttr is None:
                logger.warning("%s No partition column set",__s_fn_id__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._partAttr

    @partitionAttr.setter
    def partitionAttr(self,partition_attr) -> str:
        """
        Description:
            Gets and returns the partition column name
        Attribute:

        Returns:
            self._partAttr
        """

        __s_fn_id__ = f"{self.__name__} function <@partitionAttr.setter>"

        try:
            if partition_attr is not None or "".join(partition_attr.split())!="":
                self._partAttr=partition_attr
                logger.debug("%s Set partition column attribute name as %s",
                             __s_fn_id__,self._partAttr)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <@property windowSpec>"
        __win_len__ = 7

        try:
            ''' function to calculate number of seconds from number of days '''
            if self._winSpec is None:
                days = lambda i: i * 86400
                self._winSpec = Window \
                    .orderBy(F.col(self.datetimeAttr).cast('long')) \
                    .rangeBetween(-days(__win_len__),0)
                logger.debug("%s Class property winSpec was not explicitly set "+\
                             "Setting to $d DAY",__s_fn_id__,__win_len__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

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

        __s_fn_id__ = f"{self.__name__} function <@windowSpec.setter>"

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

            logger.debug("%s WindowSpec set to %s ",__s_fn_id__,str(self._winSpec))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._winSpec

    ''' Function --- SIMPLE MOVING STATS ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def rollingstat(func):
        """
        Description:
        
        Attributes:
        
        Returns:
        """
        @functools.wraps(func)
        def calc_roll_stat(self,num_col,date_col,part_col,win_len,win_unit,stat_op,data,**kwargs):
            """
            Description:

            Attributes:

            Returns:
            """
            __s_fn_id__ = f"{self.__name__} function <calc_roll_stat>"

            try:
                _roll_col_name = func(self,num_col,date_col,part_col,
                                      win_len,win_unit,stat_op,data,**kwargs)

                if stat_op.upper() in ['MEAN','AVG','AVERAGE']:
                    self._data = self.data. \
                                    withColumn(_roll_col_name, F.mean(num_col). \
                                               over(self.windowSpec))
                elif stat_op.upper() in ['EMA','EXPMOVEAVG','EXPONENTIALMOVINGAVERAGE']:
                    self._data = self.data. \
                                    withColumn(_roll_col_name, F.mean(num_col). \
                                               over(self.windowSpec))
                elif stat_op.upper() in ['EWM','EWMA','EXPWEIGHTAVG','EXPWEIGHTMOVEAVG']:
                    self.data = RollingStats.exp_ma(
                        df=self._data,
                        group_col=part_col,
                        sort_col =date_col,
                        val_col_name=num_col,
                        ewa_col_name=_roll_col_name,
                    )

                elif stat_op.upper() in ['STDDEV','STDV','SD','SDV','STANDARD DEVIATION']:
                    self._data = self.data. \
                                    withColumn(_roll_col_name, F.stddev(num_col). \
                                               over(self.windowSpec))
                elif stat_op.upper()=='SUM':
                    self._data = self.data. \
                                    withColumn(_roll_col_name, F.sum(num_col). \
                                               over(self.windowSpec))
                else:
                    raise AttributeError("Invalid stat operation %s" % stat_op)

                if self._data is None or self._data.count()<=0:
                    raise AttributeError("%s returned an empty %s dataframe" 
                                         %(stat_op.upper(),type(self._data)))
                logger.debug("%s successfully computed %s and returned %d rows and %d columns",
                             __s_fn_id__,stat_op.upper(),self._data.count(),len(self._data.columns))

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data
        return calc_roll_stat

    @rollingstat
    def simple_moving_stats(
        self,
        num_col :str='',  # numeric column name to apply the rolling computation
        date_col:str='',  # datetime column name to use as the time stamp
        part_col:str='',  # partition column name to apply rolling stats to windows
        win_len :int=7,   # window length in days, hous, min
        win_unit:str='DAY', # window length unit of measure by days, hours, minutes
        stat_op:str="mean", # stat operation sum, mean or standard deviation
        data=None,   # data set; that can be converted to pyspark DataFrame
        **kwargs,    # key/value pairs to set other parameters
    ):
        """
        Description:
            For a specified valid numeric column, the function will set the window length
            based on the defined date column. Thereafter, apply the moving sum, average, mean,
            exonential averate, standard deviation, and so on. The computed values, for each
            partition column, is saved to the dataframe as a new results column.
                * simple_moving_stats - validates the input paramenters
                * rollingstat - is a wrapper that computes the rolling stats
        Attributes:
            num_col (str), numeric column name to apply the rolling computation
            date_col (str, datetime column name to use as the time stamp
            part_col(str), partition column name to apply rolling stats to windows
            win_len (int), window length; default is 7 days
            win_unit(str), window length unit of measure by days, hours, minutes; default is DAY
            stat_op (str), stat operation sum, mean or standard deviation
            data (any), any data set; that can be converted to pyspark DataFrame
            kwargs (dict), key/value pairs to set other parameters
                RESULTSCOL - specify a column name to use  for storing the generated values;
                    else, will use the default joining 'rolling',stat operation, & numeric
                    column name
        Returns:
            self._data (DataFrame)
        Exceptions:
            data count is < 2; cannot compute moving stats
            num_col is not numeric; abort
            
        """

        __s_fn_id__ = f"{self.__name__} function <simple_moving_stats>"

        _winspec = {}
        __def_win_len__ =7
        __def_win_unit__='DAY'

        try:
#             if not data is None:

#             if self.data is None:
#                 raise AttributeError("There is no data to perform a rolling computation")

            self.data = data
            if self._data.count() < 2:
                raise ValueError("%s Cannot processing moving stats with %d<2 rows" 
                                 % (type(self._data),self._data.count()))

            ''' column name to apply rolling computation '''
            if "".join(num_col.split())=="" or \
                    self.data.filter(F.col(num_col).cast("Long").isNotNull()).count()==0:
                raise ValueError("A num_col name from %s with dtype = int,long,double, "+ \
                                 "or decimal must be specified." % self.data.dtypes)
            ''' rolling functions only works with decimal dtypes '''
            self.data = self.data.withColumn(num_col,F.col(num_col).cast('decimal(38,18)'))
            ''' setting the datetimeAttr property will validate and cast column to timestamp '''
            self.datetimeAttr=date_col
            ''' set the partition column name '''
            self.partitionAttr=part_col
            ''' set the rolling window specs '''
            if isinstance(win_len,int) and win_len > 1:
                _winspec['LENGTH']=win_len
            else:
                _winspec['LENGTH']=__def_win_len__
                logger.warning("%s invalid win_len; setting with default value %d",
                               __s_fn_id__,_winspec['LENGTH'])
            if win_unit is None or "".join(win_unit.split())=="":
                _winspec['UNIT']=__def_win_unit__
                logger.warning("%s invalid win_unit; setting with default value %s",
                               __s_fn_id__,_winspec['UNIT'])
            else:
                _winspec['UNIT']=win_unit
            ''' set default values '''
            self.windowSpec = _winspec
            logger.debug("%s Class property windowSpec set with %s",__s_fn_id__,str(_winspec))

            if "RESULTCOL" in kwargs.keys() and "".join(kwargs['RESULTCOL'].split()) != "":
                _roll_col_name = kwargs['RESULTCOL']
            else:
                _roll_col_name = "_".join(["roll",stat_op,num_col])

            logger.debug("%s Simple Moving Stats results, for dataframe with %d rows "+\
                         "and %d columns, written to column %s",
                         __s_fn_id__,self.data.count(),len(self.data.columns),_roll_col_name.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _roll_col_name
    
    
    
    @rollingstat
    def DEPRECATED_simple_moving_stats(
        self,
        num_col :str='',  # numeric column name to apply the rolling computation
        date_col:str='',  # datetime column name to use as the time stamp
        part_col:str='',  # partition column name to apply rolling stats to windows
        win_len :int=7,   # window length in days, hous, min
        win_unit:str='DAY', # window length unit of measure by days, hours, minutes
        stat_op:str="mean", # stat operation sum, mean or standard deviation
        data=None,   # data set; that can be converted to pyspark DataFrame
        **kwargs,    # key/value pairs to set other parameters
    ):
        """
        Description:
        
        Attributes:
        
        Returns:
        """

        __s_fn_id__ = f"{self.__name__} function <simple_moving_stats>"

        _winspec = {}
        _winspec['LENGTH']=7
        _winspec['UNIT']='DAY'

        try:
#             if not data is None:

#             if self.data is None:
#                 raise AttributeError("There is no data to perform a rolling computation")

            self.data = data

            ''' column name to apply rolling computation '''
            if "".join(num_col.split())=="" or \
                    self.data.filter(F.col(num_col).cast("Long").isNotNull()).count()==0:
                raise ValueError("A num_col name from %s with dtype = int,long,double, "+ \
                                 "or decimal must be specified." % self.data.dtypes)
            self.data = self.data.withColumn(num_col,F.col(num_col).cast('decimal(38,18)'))
            ''' setting the datetimeAttr property will validate and cast column to timestamp '''
            self.datetimeAttr=date_col
            ''' set the partition column name '''
            self.partitionAttr=part_col
            ''' set the rolling window specs '''
            if "WINLENGTH" in kwargs.keys() and not kwargs['WINLENGTH'] is None \
                and "WINUNIT" in kwargs.keys() and not kwargs['WINUNIT'] is None:
                _winspec['LENGTH']=kwargs['WINLENGTH']
                _winspec['UNIT']=kwargs['WINUNIT']
                self.windowSpec = _winspec
            else:
#                 self.windowSpec is None:
                ''' set default values '''
                self.windowSpec = _winspec
#             else:
#                 pass
            logger.debug("%s Class property windowSpec set with %s",__s_fn_id__,str(_winspec))

            if "RESULTCOL" in kwargs.keys() and "".join(kwargs['RESULTCOL'].split()) != "":
                _roll_col_name = kwargs['RESULTCOL']
            else:
                _roll_col_name = "rolling_"+stat_op+"_"+num_col

            logger.debug("%s Simple Moving Stats results, for dataframe with %d rows "+\
                         "and %d columns, written to column %s",
                         __s_fn_id__,self.data.count(),len(self.data.columns),_roll_col_name.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _roll_col_name

    ''' Function --- EXPONENTIAL WEIGHTED MOVING AVERAGE ---
    
        author: <samana.thetha@gmail.com>
        
        resoource: https://stackoverflow.com/questions/50105631/how-to-run-exponential-weighted-moving-average-in-pyspark
    '''
    @staticmethod
    def exp_ma(
        df,
        group_col='asset_name',
        sort_col='mcap_date',
        val_col_name='mcap_value',
        ewa_col_name='ewma',
        **kwargs):
        schema = (df.select('*')
            .schema.add(StructField(ewa_col_name, DoubleType())))

        @pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
        def ema(pdf):
            pdf[ewa_col_name] = pdf[val_col_name].ewm(span=5, min_periods=1).mean()
            return pdf

        return df.groupby(group_col).apply(ema)


    ''' Function --- FAST FOURIER DENOISER ---
    
        author: <samana.thetha@gmail.com>
        
        resoource: https://medium.com/swlh/5-tips-for-working-with-time-series-in-python-d889109e676d
    '''
    def fft_denoiser(x, n_components, to_real=True):
        """
        Description:
        
        Attributes:
        
        Returns:
        """

        __s_fn_id__ = f"{RollingStats.__name__} @staticmethod <simple_moving_stats>"

        clean_data = None
        
        try:
            n = len(x)

            # compute the fft
            fft = np.fft.fft(x, n)

            # compute power spectrum density
            # squared magnitud of each fft coefficient
            PSD = fft * np.conj(fft) / n

            # keep high frequencies
            _mask = PSD > n_components
            fft = _mask * fft

            # inverse fourier transform
            clean_data = np.fft.ifft(fft)

            if to_real:
                clean_data = clean_data.real

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return clean_data

