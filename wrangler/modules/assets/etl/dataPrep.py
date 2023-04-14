#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "dataPrep"
__package__ = "etl"
__module__ = "assets"
__app__ = "wrangler"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages '''
    import os
    import sys
    import configparser    
    import logging
    import traceback
    import functools
    ''' function specific python packages '''
    import pandas as pd
    import numpy as np
    from datetime import datetime, date, timedelta

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql import DataFrame

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))


'''
    CLASS spefic to preparing the data based on Ration Of Returns (ROR).
    
'''

# class Warehouse():
class RateOfReturns():

    ''' Function --- INIT ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="market cap data prep", **kwargs):
        """
        Decription:
            Initializes the SQLWorkLoads: class property attributes, app configurations, 
                logger function, data store directory paths, and global classes 
        Attributes:
            desc (str) identify the specific instantiation and purpose
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

        self._data = None
#         self._portfolio=None

        global pkgConf
        global appConf
        global logger
        global clsSDB
        global clsSCNR
        global clsNoSQL

        __s_fn_id__ = f"{self.__name__} function <__init__>"
        
        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)

            ''' innitialize the logger '''
            from rezaware import Logger as logs
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)

            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class",self.__name__)

            ''' import spark database work load utils to read and write data '''
            from utils.modules.etl.load import sparkDBwls as sparkDB
            clsSDB = sparkDB.SQLWorkLoads(desc=self.__desc__)
            ''' import spark clean-n-rich work load utils to transform the data '''
            from utils.modules.etl.transform import sparkCleanNRich as sparkCNR
            clsSCNR = sparkCNR.Transformer(desc=self.__desc__)
            ''' import mongo work load utils to read and write data '''
            from utils.modules.etl.load import sparkNoSQLwls as nosql
            clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)

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


    ''' Function --- DATA ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):
        """
        Description:
            data @property and @setter functions. make sure it is a valid spark dataframe
        Attributes:
            data in @setter will instantiate self._data    
        Returns (dataframe) self._data
        """

        __s_fn_id__ = f"{self.__name__} function <@property data>"

        try:
            if self._data is None:
                raise ValueError("Data is of NoneType; cannot be used in any %s computations"
                                 %self.__name__)
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):

        __s_fn_id__ = "function <@setter data>"

        try:
            if data is None:
                raise AttributeError("Invalid data attribute, must be a valid pyspark dataframe")

            self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

#     ''' Function --- PORTFOLIO ---

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     @property
#     def portfolio(self) -> list:
#         """
#         Description:
#             portfolio @property function. make sure it is a valid spark dataframe
#         Attributes:
#             data in @setter will instantiate self._data    
#         Returns (dataframe) self._data
#         """

#         __s_fn_id__ = "function <@property portfolio>"

#         try:
#             if not isinstance(self._portfolio,list):
# #                 pass
# #             elif isinstance(self._portfolio,pd.DataFrame):
# #                 self._portfolio = clsSDB.data(self._portfolio)
# #             elif isinstance(self._portfolio,list):
# #                 self._portfolio = clsSDB.data(pd.DataFrame(self._portfolio))
# #             elif isinstance(self._portfolio,dict):
# #                 self._portfolio = clsSDB.data(pd.DataFrame([self._portfolio]))
# #             else:
#                 raise AttributeError("Invalid self._portfolio; must a valid pyspark DataFrame dtype")

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._portfolio

#     @portfolio.setter
#     def portfolio(self,portfolio:list=[]) -> list:

#         __s_fn_id__ = "function <@setter portfolio>"

#         try:
#             if len(portfolio)<=0:

#                 raise AttributeError("Invalid portfolio attribute, must be a non-empy list")

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._portfolio

    ''' Function --- PREVIOUS VAL DIFFERENCE ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @staticmethod
    def prev_val_diff(
        data:DataFrame,
        num_column:str,
        part_column:str,
        **kwargs,
    ) -> DataFrame:
        """
        Description:
            for a given numeric column, the function computes the difference between
            the current cell and the previous cell
        Attributes:
            data (DataFrame) a valid pyspark dataframe
            column - specifies column to compute the difference
            **kwargs
                DIFFCOLNAME - the column name
        Returns:
        """

        __s_fn_id__ = f"{RateOfReturns.__name__} function <unpivot_table>"
        _diff_data = None
        _diff_col = "diff"
        __prev_val_pofix__ = "lag"
        _prev_val=None

        try:
            if data.count() <= 2:
                raise AttributeError("Dataframe must have, at least, 2 rows to compute the difference ")
            if num_column not in data.columns and \
                not isinstance(data.num_column,int) and\
                not isinstance(data.num_column,float):
                raise AttributeError("%s must be a numeric dataframe column" % num_column)
            if part_column not in data.columns:
                raise AttributeError("%s must be a column in the dataframe: %s" 
                                     % (num_column,data.columns))
            if "DIFFCOLNAME" in kwargs.keys():
                _diff_col = kwargs['DIFFCOLNAME']
            _prev_val = "_".join([num_column,__prev_val_pofix__])
            if "PREVALCOLNAME" in kwargs.keys():
                _prev_val = kwargs['PREVALCOLNAME']

            _win = Window.partitionBy(part_column).orderBy(part_column)
            _diff_data = data.withColumn(_prev_val, F.lag(data[num_column]).over(_win))
            _diff_data = _diff_data.withColumn(_diff_col,\
                                    F.when(\
                                      F.isnull(_diff_data[num_column] - _diff_data[_prev_val]), 0)\
                                      .otherwise(_diff_data[num_column] - _diff_data[_prev_val]))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _diff_data, _prev_val, _diff_col


    ''' Function --- READ N CLEAN ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def impute(func):
        """
        Description:
            set the function as an input for the wrapper
        Attributes:
            func - defines and inherits read_n_clean_mcap 
        Returns:
            impute_wrapper (func)
        """

        @functools.wraps(func)
        def impute_wrapper(self, query:str="", **kwargs):
            """
            Description:
                Takes the ticker data read from the database to cleanup any Nulls, NaN, or
                non-numeric values. 
                Uses the utils/etl/transform/CleanNRich package to. 
                  * pivots the date with ticker names as columns. 
                  * Imputes any missing values with the specified strategy for respective dates. 
                  * Counts the number of Nulls to ensure the timeseries is complete.
                  * unpivots the dataframe to reconstuct in original form
            Attributes:
                query (str) - not used in this wrapper but required for embedding the func
                **kwargs
                    PARTCOLNAME - column name to pivot the values into columns (e,g., asset_name)
                    DATECOLNAME - datetime column name; to group the pivot columns (e.g. mcap_date)
                    VALUECOLNAME- column with the numeric valuesto apply the imputation (e.g. mcap_value)
                    AGGREGATE - to set the impute strategy as 'mean','median','mode' ...
            Returns:
                self._data (dataframe)
            Exceptions:
                * If essential **kwargs are not specified then will use default values; log a warning
                * If the CleanNRich (clsSCNR) class methods pivot, impute, & unpivot return empty or
                    Nonetype dataframes, then exception is logged and process will abort
            """

            __s_fn_id__ = f"{self.__name__} function <impute_wrapper>"
            __def_part_col__='asset_name'
            __def_date_col__="mcap_date"
            __def_val_col__ ="mcap_value"
            __def_agg_st__ = 'mean'
            
            try:
                self._data = func(self, query, **kwargs)

                ''' set default attribute values '''
                if "PARTCOLNAME" not in kwargs.keys():
                    kwargs['PARTCOLNAME']=__def_part_col__
                    logger.warning("%s undefined kwargs PARTCOLNAME, setting to default: %s",
                                   __s_fn_id__,kwargs['PARTCOLNAME'].upper())
                else:
                    logger.debug("%s using set kwargs PARTCOLNAME: %s ",
                                 __s_fn_id__,kwargs['PARTCOLNAME'].upper())
                if "DATECOLNAME" not in kwargs.keys():
                    kwargs['DATECOLNAME']=__def_date_col__
                    logger.warning("%s undefined kwargs DATECOLNAME, setting to default: %s",
                                   __s_fn_id__,kwargs['DATECOLNAME'].upper())
                else:
                    logger.debug("%s using set kwargs DATECOLNAME: %s ",
                                 __s_fn_id__,kwargs['DATECOLNAME'].upper())
                if "VALUECOLNAME" not in kwargs.keys():
                    kwargs['VALUECOLNAME']=__def_val_col__
                    logger.warning("%s undefined kwargs VALUECOLNAME, setting to default to: %s",
                                   __s_fn_id__,kwargs['VALUECOLNAME'].upper())
                else:
                    logger.debug("%s using set kwargs VALUECOLNAME: %s ",
                                 __s_fn_id__,kwargs['VALUECOLNAME'].upper())

                ''' transpose to get assets in the columns '''
                _pivot_sdf=clsSCNR.pivot_data(
                    data=self._data,
                    group_columns=kwargs['DATECOLNAME'], # mcap_date,price_date,volume_date,etc
                    pivot_column =kwargs['PARTCOLNAME'], # asset_name,
                    agg_column = kwargs['VALUECOLNAME'], # mcap_value, price_value, volume_size,etc
                    **kwargs,
                )
                if _pivot_sdf is None or _pivot_sdf.count()<=0:
                    raise RuntimeError("Pivot_data method call returned a %s type object, aborting!" 
                                       % type(_pivot_sdf))
                logger.debug("%s returned a pivot table with %d columns and %d rows with " +\
                             "group column: %s, pivot column: %s, and aggregate column: %s ",
                             __s_fn_id__,len(_pivot_sdf.columns),_pivot_sdf.count(),
                             kwargs['DATECOLNAME'].upper(),kwargs['PARTCOLNAME'].upper(),
                             kwargs['VALUECOLNAME'].upper())

                ''' impute to fill the gaps '''
                if "IMPUTESTRATEGY" not in kwargs.keys() \
                    and kwargs['IMPUTESTRATEGY'] not in ['mean','median','mode']:
                    kwargs['IMPUTESTRATEGY']=__def_agg_st__
                    logger.debug("%s undefined kwargs IMPUTESTRATEGY, setting to default %s",
                                   __s_fn_id__,kwargs['IMPUTESTRATEGY'].upper())
                else:
                    logger.debug("%s set aggregate strategy to %s ",
                                 __s_fn_id__,kwargs['IMPUTESTRATEGY'].upper())
                ''' select columns excluding date column '''
                _col_subset = _pivot_sdf.columns
                _col_subset.remove(kwargs['DATECOLNAME'])
                ''' impute the data columns '''
                _pivot_sdf = clsSCNR.impute_data(
                    data = _pivot_sdf,
                    column_subset=_col_subset,
                    strategy = kwargs['IMPUTESTRATEGY'],
                )
                if _pivot_sdf is None or _pivot_sdf.count()<=0:
                    raise RuntimeError("Impute_data method call returned a %s object, aborting! "
                                       % type(_pivot_sdf))
                logger.debug("%s ran an impute_data method on all %d %s partition columns; " +\
                             "excluding %s column; received %d rows.",
                             __s_fn_id__,len(_col_subset),kwargs['PARTCOLNAME'].upper(),
                             kwargs['DATECOLNAME'].upper(),_pivot_sdf.count())

                ''' ensure there are no non-numeric values '''
    #             _col_subset = mcap_sdf.columns
    #             _col_subset.remove(kwargs['DATECOLNAME'])
                _nan_counts_sdf = clsSCNR.count_column_nulls(
                    data=_pivot_sdf,
                    column_subset=_col_subset
                )
                ''' check for assets with nulls '''
                ''' CAUSING MEMORY ISSUES '''
#                 try:
#                     ''' TODO remove tickers with Null counts > 0 '''
#                     logger.debug("%s Checking for asset tickers with non-numerics",__s_fn_id__)
#                     for _ticker in _col_subset:
#                         _tic_nan_count = _nan_counts_sdf.collect()[0].__getitem__(_ticker)
#                         if _tic_nan_count!=0:
#                             raise ValueError("%s has %d Null, NaN, or Non-numeric values"
#                                              %(_ticker,_tic_nan_count))
#                 except Exception as ticker_err:
#                     logger.warning("%s",ticker_err)

                ''' unpivot dataframe '''
                _piv_col_subset = _pivot_sdf.columns
                _piv_col_subset.remove(kwargs['DATECOLNAME'])

                _unpivot_sdf = clsSCNR.unpivot_table(
                    table = _pivot_sdf,
                    unpivot_columns=_piv_col_subset,
                    index_column=kwargs['DATECOLNAME'],
#                     value_columns=['asset_name','mcap_value'],
                    value_columns=[kwargs['PARTCOLNAME'],kwargs['VALUECOLNAME']],
                    where_cols = kwargs['VALUECOLNAME'],
                    **kwargs
                )

                if _unpivot_sdf is None or _unpivot_sdf.count() <= 0:
                    raise ValueError("Unpivot returned %s dataset" % type(_unpivot_sdf))
#                 self._data = self._data.unionByName(_unpivot_sdf,allowMissingColumns=True)
                self._data=self._data.drop(kwargs['VALUECOLNAME'])
                self._data = self._data.join(_unpivot_sdf,
                                             [kwargs['PARTCOLNAME'],kwargs['DATECOLNAME']],
                                             "fullouter")
                logger.debug("%s After unpivot, dataframe with rows %d columns %d",
                             __s_fn_id__,self._data.count(),len(self._data.columns))

                ''' add missing values; e.g. asset_symbol '''
                null_cols_list = ["currency","asset_symbol"]
                for _null_col in null_cols_list:
                    self._data = self._data.orderBy(F.col(_null_col).asc())
                    self._data = self._data.withColumn(_null_col, F.first(_null_col,ignorenulls=True)\
                                       .over(Window.partitionBy(kwargs['PARTCOLNAME'])))
                logger.debug("%s Replaced Null values in dataframe columns %s",
                             __s_fn_id__,str(null_cols_list))

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data
        return impute_wrapper

    @impute
    def read_n_clean_mcap(self, query:str="", **kwargs):
        """
        Description:
            The key feature is to read the mcap data from postgresql and impute to ensure
            the data is clean and useable. There are two options for reading the data
             (* giving an SQL query as a string
              * defining the table name to read the entire dataset
            Makes use of sparkDBwls package to read the data from the DB
        Arguments:
            query (str) - valid SQL select statement with any SQl clauses
            **kwargs - specifying key value pairs
                TABLENAME - db_able name with or without the schema name
                COLUMN - partition column name
                FROMDATETIME - timestamp setting the partition column lower-bound
                TODATETIME - timestamp setting the partition column upper-bound
        Returns: self._data (dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <read_n_clean_mcap>"

        _table ='warehouse.mcap_past'
        _column='mcap_date'
        _to_date =date.today()
        _from_date=_to_date-timedelta(days=7)

        try:
            if "TABLENAME" in kwargs.keys():
                _table=kwargs['TABLENAME']
            if "COLUMN" in kwargs.keys():
                _column=kwargs['COLUMN']
            if "FROMDATETIME" in kwargs.keys():
                _from_date=kwargs['FROMDATETIME']
            if "TODATETIME" in kwargs.keys():
                _to_date=kwargs['TODATETIME']

            if query is not None and "".join(query.split())!="":
                self._data = clsSDB.read_data_from_table(select=query, **kwargs)
#             else:
#                 self._data = clsSDB.read_data_from_table(
#                     db_table=_table,
#                     db_column=_column,
#                     lower_bound=_from_date,
#                     upper_bound=_to_date,
#                     **kwargs)

            if self._data.count() <= 0:
                raise RuntimeError("%s did not read any data",__s_fn_id__)
            logger.debug("%s loaded %d rows",__s_fn_id__,self._data.count())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' Function --- GET ROR ---

            author: <nuwan.waidyanatha@rezgateway.com>
            
            resources:
                all ROR methods: https://en.wikipedia.org/wiki/Rate_of_return
                Logarithmic ROR: https://www.rateofreturnexpert.com/log-return/
    '''
    def ror_type_cast(func):
        """
        Description:
            wrapper function to calculate an ROR method
        Attributes:
            func inherits get_ror
        Returns:
            ror_calc_wrapper
        """
        @functools.wraps(func)
        def cast_ror_wrapper(self,data,ror_type,num_col,part_col,date_col,**kwargs):
            """
            Description:
                The ror_type defines the ROR method to execute.
                Executes log10, log2, log1p, and simple ROR methods  
            Attributes:
                Same as get_ror function input attributes
                **kwargs
                    RORCOLNAME key value defines the name to use for 
            Returns:
                self_data (DataFrame) with the requrested ROR computed column
            """
            __s_fn_id__ = f"{self.__name__} function <cast_ror_wrapper>"
            ''' declare local arguments '''
            _ror_col = "ror"

            try:
                _data, _prev_col, _diff_col = func(self,data,ror_type,num_col,part_col,date_col,**kwargs)

                ''' validate dataframe before applying ROR'''
                if _data.count()<=0:
                    raise RuntimeError("Empty dataframe. Aborting ROR computation")

                ''' set ROR parameters '''
                if "RORCOLNAME" in kwargs.keys():
                    _ror_col=kwargs['RORCOLNAME']
                else:
                    _ror_col="_".join([_ror_col,num_col])
                logger.debug("%s set ROR column name to %s to store ROR type %s values",
                             __s_fn_id__,_ror_col.upper(),ror_type.upper())

                ''' compute the log of the prev / current '''
                if ror_type.upper()=='LOG10':
                    self._data=_data.withColumn(_ror_col,
                                                F.log10(F.col(num_col)/F.col(_prev_col)))
                elif ror_type.upper()=='LOG2':
                    self._data=s_data.withColumn(_ror_col,
                                                 F.log2(F.col(num_col)/F.col(_prev_col)))
                elif ror_type.upper() in ['NATLOG','NATURALLOG']:
                    self._data=_data.withColumn(_ror_col,
                                                F.log1p(F.col(num_col)/F.col(_prev_col)))
                elif ror_type.upper() in ['SIMPLE','SIMP']:
                    self._data=_data.withColumn(_ror_col,
                                                ((F.col(num_col)-F.col(_prev_col))\
                                                 /F.col(num_col)))
                else:
                    raise RuntimeError("Something went wrong determining the log base.")

                if self._data is None and self._data.count()<=0:
                    raise RuntimeError("%s ROR computation returned %s dataframe" 
                                       % (ror_type.upper(),type(self._data)))
                ''' drop diff and prev value columns '''
                self._data=self._data.drop(_prev_col,_diff_col)
                logger.debug("%s %s ROR computation returned %d rows and %d columns",
                             __s_fn_id__,ror_type.upper(),self._data.count(),len(self._data.columns))

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data.sort(F.col(date_col),F.col(_ror_col)), _ror_col

        return cast_ror_wrapper

    @ror_type_cast
    def calc_ror(
        self,
        data,
        ror_type:str="log10",
        num_col :str="",
        part_col:str="",
        date_col:str="",
        **kwargs,
    ):
        """
        Description:
            Computes the logarithmic ratio of returns for a specific numeric columns. If the log base
            is the kwargs LOGBASE key, then it defaults to log10.
            If the numeric columns are specified, then the log ROR is calculated for them;
            else for all identified numeric columns. The dataset is augmented with a new
            column with the log ROR for those respective numeric columns.
        Attributes:
            data (dataframe) - mandatory valid timeseries dataframe with, at least, one numeric column
            columns (List(str)) - optional list of column names to compute and augment the log ROR
            **kwargs
                DIFFCOLNAME (str) assigns the the column name to use for the timeseries previous values
                RORCOLNAME (str) assigns the column name to use for the log ror values
                LOGBASE (str) assigns the log base value to use 10,2,e accepted values
        Returns:
            self._data (dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <calc_ror>"
        ''' declare local arguments '''
        _diff_col = None
        __diff_prefix__ = "diff"
        _lag_num_col=None

        try:
            if not isinstance(data,DataFrame) or data.count()==0:
                raise AttributeError("data attribute must be a vaild non-empty DataFrame")
            if num_col not in data.columns and \
                data.select(F.col(num_col)).dtypes[0][1] in ['string','date','timestamp','boolean']:
                raise AttributeError("%s is invalid dtype, select a numeric column from %s"
                                     % (num_col,data.dtypes))
            if part_col not in data.columns:
                raise AttributeError("%s is an invalid column name, select a proper column from %s"
                                     % (part_col,data.dtypes))

            ''' get the difference from the previous value '''
            if "DIFFCOLNAME" not in kwargs.keys():
                kwargs['DIFFCOLNAME']="_".join([__diff_prefix__,num_col])
#             else:
            _diff_col=kwargs['DIFFCOLNAME']

            self._data, _lag_num_col, _diff_col=RateOfReturns.prev_val_diff(
                data=data.sort(date_col,num_col),   # sort by date get previou date value
                num_column=num_col,   #'mcap_value',
                part_column=part_col, #'asset_name',
                **kwargs,
            )
            if self._data.count()>0:
                logger.debug("Created new columns %s %s with previous value (lag) "+\
                             "and difference for %d rows",
                            _lag_num_col,_diff_col,self._data.count())
            else:
                raise RuntimeError("previous value (lag) and difference computation +"\
                                   "did not return and rows")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data, _lag_num_col, _diff_col


    ''' Function --- WRITE DATA TO DB ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def write_data_to_db(
        self,
        data,
        **kwargs,
    ):
        """
        Description:
        Attributes:
        Returns:
        """

        __s_fn_id__ = f"{self.__name__} function <write_data_to_db>"
        ''' declare local args '''
        __def_tbl_name__='mcap_past'
        __def_db_name__='tip'
        _pk = ['mcap_past_pk']
        _cols_not_for_update = ['mcap_past_pk','uuid','data_source','asset_name',
                                'mcap_date','price_date','volume_date',
                                'created_dt','created_by','created_proc']
        _options={
            "BATCHSIZE":1000,   # batch size to partition the dtaframe
            "PARTITIONS":1,    # number of parallel clusters to run
            "OMITCOLS":_cols_not_for_update,    # columns to be excluded from update
        }
        records_=0

        try:
            if "TABLENAME" not in kwargs.keys() or "".join(kwargs['TABLENAME'].split())=="":
                kwargs['TABLENAME']=__def_tbl_name__
                logger.warning("%s set kwargs TABLENAME to default %s",
                               __s_fn_id__,kwargs['TABLENAME'].upper())
            if "DBNAME" not in kwargs.keys() or "".join(kwargs['DBNAME'].split())=="":
                kwargs['DBNAME']=__def_db_name__
                logger.warning("%s set kwargs DBNAME to default %s",
                               __s_fn_id__,kwargs['DBNAME'].upper())

            ''' replace cell nulls with known values '''
            
            ''' split the data with mcap_past_pk and without '''
            ''' --- data has no PK --- '''
            data_no_pk_ = data.filter(F.col('mcap_past_pk').isNull())
            if data_no_pk_ is not None and data_no_pk_.count()>0:
                data_no_pk_=data_no_pk_.drop('mcap_past_pk')
                no_pk_rec_count_=clsSDB.insert_sdf_into_table(
                    save_sdf=data_no_pk_,
                    db_name =kwargs['DBNAME'],
                    db_table=kwargs['TABLENAME'],
                    session_args = kwargs
                )
                if no_pk_rec_count_ is None or no_pk_rec_count_<=0:
                    no_pk_rec_count_=0
                    logger.error("%s Failed insert %d records into %s %s database in %s table",
                                 __s_fn_id__,data_no_pk_.count(),clsSDB.dbType,
                                 clsSDB.dbName,kwargs['TABLENAME'])
                else:
                    logger.debug("%s Inserted %d of %d records into %s %s database in %s table",
                                 __s_fn_id__,no_pk_rec_count_,data_no_pk_.count(),
                                 clsSDB.dbType,clsSDB.dbName,kwargs['TABLENAME'])
            else:
                no_pk_rec_count_=0
                logger.debug("%s No data obtained for attribute mcap_past_pk isNull()",__s_fn_id__)
            ''' --- data has PK --- '''
            data_has_pk_ = data.filter(F.col('mcap_past_pk').isNotNull())
            if data_has_pk_ is not None and data_has_pk_.count()>0:
                has_pk_rec_count_=clsSDB.upsert_sdf_to_table(
                    save_sdf=data_has_pk_,
                    db_table=kwargs['TABLENAME'],
                    unique_keys=_pk,
                    **_options,
                )
                if has_pk_rec_count_ is None or has_pk_rec_count_<=0:
                    has_pk_rec_count_=0
                    logger.error("%s Failed upserty %d records into %s %s database in %s table",
                                 __s_fn_id__,data_has_pk_.count(),clsSDB.dbType,
                                 clsSDB.dbName,kwargs['TABLENAME'])
                else:
                    logger.debug("%s Upsert %d of %d records into %s %s database in %s table",
                                 __s_fn_id__,has_pk_rec_count_,data_has_pk_.count(),
                                 clsSDB.dbType,clsSDB.dbName,kwargs['TABLENAME'])
            else:
                has_pk_rec_count_=0
                logger.debug("%s No data obtained for attribute mcap_past_pk isNotNull()",__s_fn_id__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return no_pk_rec_count_+has_pk_rec_count_
