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

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)

#             self.pckgDir = pkgConf.get("CWDS",self.__package__)
#             self.appDir = pkgConf.get("CWDS",self.__app__)
#             ''' DEPRECATED: get the path to the input and output data '''
#             self.dataDir = pkgConf.get("CWDS","DATA")

#             ''' set app configparser '''
#             appConf = configparser.ConfigParser()
#             appConf.read(os.path.join(self.appDir, self.__conf_fname__))

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
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

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

        __s_fn_id__ = "function <@property data>"

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

        return self._portfolio

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

        __s_fn_id__ = "function <unpivot_table>"
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
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

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
                **kwargs
                    AGGREGATE - to set the impute strategy as mean, stddev, avg, ...
            Returns:
                self._data (dataframe)
            """

            __s_fn_id__ = "function <impute_wrapper>"
            __strategy__='mean'
            try:
#                 _asset_data = func(self, query:str="", **kwargs)
                self._data = func(self, query, **kwargs)
                ''' transpose to get assets in the columns '''
#                 self._data=clsSCNR.pivot_data(
                _pivot_sdf=clsSCNR.pivot_data(
                    data=self._data,
                    group_columns='mcap_date',
                    pivot_column='asset_name',
                    agg_column='mcap_value',
                    **kwargs,
                )

                ''' impute to fill the gaps '''
                _agg_strategy = __strategy__
                if "AGGREGATE" in kwargs.keys():
                    _agg_strategy = kwargs['AGGREGATE']
#                 _col_subset = self._data.columns
                _col_subset = _pivot_sdf.columns
                _col_subset.remove('mcap_date')
#                 _piv_ticker_sdf = clsSCNR.impute_data(
                _pivot_sdf = clsSCNR.impute_data(
#                     data=self._data,
                    data=_pivot_sdf,
                    column_subset=_col_subset,
                    strategy=_agg_strategy,
                )
                logger.debug("%s ran an impute on all %d asset tickers"
                             ,__s_fn_id__,len(_col_subset))

                ''' ensure there are no non-numeric values '''
    #             _col_subset = mcap_sdf.columns
    #             _col_subset.remove('mcap_date')
                _nan_counts_sdf = clsSCNR.count_column_nulls(
#                     data=self._data,
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
                _piv_col_subset.remove('mcap_date')

                _unpivot_sdf = clsSCNR.unpivot_table(
                    table = _pivot_sdf,
                    unpivot_columns=_piv_col_subset,
                    index_column='mcap_date',
                    value_columns=['asset_name','mcap_value'],
                    where_cols = 'mcap_value',
                    **kwargs
                )

                if _unpivot_sdf.count() > 0:
                    self._data = self._data.unionByName(_unpivot_sdf,allowMissingColumns=True)
                    logger.debug("After unpivot, dataframe with rows %d columns %d"
                                 ,self._data.count(),len(self._data.columns))
                else:
                    raise ValueError("Irregular unpivot ticker dataset; something went wrong")

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
             (i) giving an SQL query as a string
            (ii) defining the table name to read the entire dataset
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

        __s_fn_id__ = "function <read_n_clean_mcap>"
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
            else:
                self._data = clsSDB.read_data_from_table(
                    db_table=_table,
                    db_column=_column,
                    lower_bound=_from_date,
                    upper_bound=_to_date,
                    **kwargs)

            if self._data.count() > 0:
                logger.debug("%s loaded %d rows",__s_fn_id__,self._data.count())
            else:
                raise ValueError("%s did not read any data",__s_fn_id__)

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
    def calc_ror(func):
        """
        Description:
            wrapper function to calculate an ROR method
        Attributes:
            func inherits get_ror
        Returns:
            ror_calc_wrapper
        """
        @functools.wraps(func)
        def ror_calc_wrapper(self,data,ror_type,num_col,part_col,date_col,**kwargs):
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
            __s_fn_id__ = "function <ror_calc_wrapper>"
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

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data.sort(F.col(date_col),F.col(_ror_col)), _ror_col

        return ror_calc_wrapper

    @calc_ror
    def get_ror(
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

        __s_fn_id__ = "function <get_ror>"
        _diff_col = None
        __diff_prefix__ = "diff"
        _lag_num_col=None

        try:
            if not isinstance(data,DataFrame) or data.count()==0:
                raise AttributeError("data attribute must be a vaild non-empty DataFrame")
            if num_col not in data.columns and \
                data.select(F.col(num_col)).dtypes[0][1]=='string':
                raise AttributeError("%s is invalid dtype, select a numeric column from %s"
                                     % (num_col,data.dtypes))
            if part_col not in data.columns:
                raise AttributeError("%s is an invalid column name, select a proper column from %s"
                                     % (part_col,data.dtypes))

            ''' get the difference from the previous value '''
            if "DIFFCOLNAME" not in kwargs.keys():
                kwargs['DIFFCOLNAME']="_".join([__diff_prefix__,num_col])
            else:
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
#             ''' compute the log of the prev / current '''
#             _log_base = '10'
#             if "LOGBASE" in kwargs.keys() and kwargs['LOGBASE'] in ['2','10','e']:
#                 _log_base = kwargs['LOGBASE']
#             if _log_base=='10':
#                 self._data=self._data.withColumn(_ror_col_name,
#                                                  F.log10(F.col(_lag_num_col)/F.col(num_col)))
#             elif _log_base=='2':
#                 self._data=self._data.withColumn(_ror_col_name,
#                                                  F.log2(F.col(_lag_num_col)/F.col(num_col)))
#             elif _log_base=='e':
#                 self._data=self._data.withColumn(_ror_col_name,
#                                                  F.log1p(F.col(_lag_num_col)/F.col(num_col)))
#             else:
#                 raise RuntimeError("Something went wrong determining the log base.")

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

        __s_fn_id__ = "function <write_data_to_db>"

        _tbl_name='warehouse.mcap_past'
        _pk = ['mcap_past_pk']
        _cols_not_for_update = ['mcap_past_pk','uuid','data_source','asset_name','asset_symbol',
                           'mcap_date','created_dt','created_by','created_proc']
        _options={
            "BATCHSIZE":1000,   # batch size to partition the dtaframe
            "PARTITIONS":1,    # number of parallel clusters to run
            "OMITCOLS":_cols_not_for_update,    # columns to be excluded from update
        }
        records_=0

        try:
            if "TABLENAME" in kwargs.keys():
                _tbl_name=kwargs['TABLENAME']

            records_=clsSDB.upsert_sdf_to_table(
                save_sdf=data,
                db_table=_tbl_name,
                unique_keys=_pk,
                **_options,
            )

            logger.debug("Upserted %d records into %s", records_,_tbl_name)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return records_
