#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "logReturns"
__package__ = "etp"
__module__ = "assets"
__app__ = "mining"
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

class RatioOfReturns():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="market cap data prep", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        self._data = None

        global pkgConf
        global appConf
        global logger
        global clsSDB
        global clsSCNR

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

            ''' import file work load utils to read and write data '''
            from utils.modules.etl.load import sparkDBwls as sparkDB
            clsSDB = sparkDB.SQLWorkLoads(desc=self.__desc__)
            from utils.modules.etl.transform import sparkCleanNRich as sparkCNR
            clsSCNR = sparkCNR.Transformer(desc=self.__desc__)
            from utils.modules.ml.timeseries import rollingstats as stats
            clsStats = stats.RollingStats(desc=self.__desc__)
#             clsRW.storeMode = pkgConf.get("DATASTORE","MODE")
#             clsRW.storeRoot = pkgConf.get("DATASTORE","ROOT")
#             logger.info("Files RW mode %s with root %s set",clsRW.storeMode,clsRW.storeRoot)
#             ''' set the package specific storage path '''
#             self.storePath = os.path.join(
#                 self.__app__,
#                 "data/",
#                 self.__module__,
#                 self.__package__,
#             )
#             logger.info("%s package files stored in %s",self.__package__,self.storePath)

#             ''' import mongo work load utils to read and write data '''
#             from utils.modules.etl.load import noSQLwls as nosql
#             clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)

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



    ''' Function
            name: data @property and @setter functions
            parameters: data in @setter will instantiate self._data

            procedure: make sure it is a valid spark dataframe
            
            return (dataframe) self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):

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
                self._data=clsSCNR.pivot_data(
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
#                 _col_subset = pivot_mcap.columns
                _col_subset = self._data.columns
                _col_subset.remove('mcap_date')
#                 _piv_ticker_sdf = clsSCNR.impute_data(
                self._data = clsSCNR.impute_data(
                    data=self._data,
                    column_subset=_col_subset,
                    strategy=_agg_strategy,
                )
                logger.debug("%s ran an impute on all %d asset tickers"
                             ,__s_fn_id__,len(_col_subset))

                ''' ensure there are no non-numeric values '''
    #             _col_subset = mcap_sdf.columns
    #             _col_subset.remove('mcap_date')
                _nan_counts_sdf = clsSCNR.count_column_nulls(
                    data=self._data,
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
#                 _piv_col_subset = _piv_ticker_sdf.columns
                _piv_col_subset = self._data.columns
                _piv_col_subset.remove('mcap_date')
#                 _unpivot_sdf = clsSCNR.unpivot_table(
                self._data = clsSCNR.unpivot_table(
                    table = self._data,
                    unpivot_columns=_piv_col_subset,
                    index_column='mcap_date',
                    value_columns=['asset_name','mcap_value'],
                    where_cols = 'mcap_value',
                    **kwargs
                )
#                 if _unpivot_sdf.count() > 0:
                if self._data.count() > 0:
#                     self._data = _unpivot_sdf
                    logger.debug("After unpivot, dataframe with rows %d columns %d"
                                 ,self._data.count(),len(self._data.columns))
#                                  ,_unpivot_sdf.count(),len(_unpivot_sdf.columns))
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


    ''' Function --- GET LOG ROR ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_log_ror(
        self,
        data,
        num_col_name:str="",
        part_column :str="",
        **kwargs,
    ):
        """
        Description:
            Computes the logarithmic ratio of returns for a specific numeric columns.
            If the numeric columns are specified, then the log ROR is calculated for them;
            else for all identified numeric columns. The dataset is augmented with a new
            column with the log ROR for those respective numeric columns.
        Attributes:
            data (dataframe) - mandatory valid timeseries dataframe with, at least, one numeric column
            columns (list) - optional list of column names to compute and augment the log ROR
            **kwargs
        Returns:
            self._data (dataframe)
        """

        __s_fn_id__ = "function <get_log_ror>"
        _log_ror_col_name = None
        __log_col_prefix__="log_ror_"
        _diff_col_name = None
        __diff_prefix__ = 'diff_'
        _prev_day_num_col=None

        try:
            if data.count()==0:
                raise AttributeError("DataFrame cannot be empty")
            if num_col_name not in data.columns:
                raise AttributeError("%s is invalid, select a numeric column %s"
                                     % (num_col_name,data.dtypes))
            if part_column not in data.columns:
                raise AttributeError("%s isinvalid, select a proper column %s"
                                     % (part_column,data.dtypes))

            ''' get the difference from the previous value '''
            if "DIFFCOLNAME" not in kwargs.keys():
#                 kwargs['DIFFCOLNAME']=_diff_col_name
                kwargs['DIFFCOLNAME']=__diff_prefix__+column
            else:
                _diff_col_name=kwargs['DIFFCOLNAME']
            self._data, _prev_day_num_col, _diff_col_name=RatioOfReturns.prev_val_diff(
                data=data.sort('mcap_date',num_col_name),   # sort by date get previou date value
                num_column=num_col_name,   #'mcap_value',
                part_column=part_column, #'asset_name',
                **kwargs,
            )
            ''' compute the log10 of the difference '''
            if "LOGCOLNAME" in kwargs.keys():
                _log_ror_col_name=kwargs['LOGCOLNAME']
            else:
                _log_ror_col_name=__log_col_prefix__+_diff_col_name
#             self._data.select(col("mcap_date"),to_date(col("mcap_date"),"MM-dd-yyyy").alias("mcap_date"))
#             self._data=self._data.withColumn(_log_ror_col_name,F.log10(F.col(_diff_col_name)))
            ''' compute the log of the prev / current '''
#             self._data=self._data.withColumn(_log_ror_col_name,
#                                              F.when(
#                                                  (F.col(_prev_day_num_col) - F.col(num_col_name))<0,-1,
#                                              otherwise(
#                                                  F.log10(F.col(_prev_day_num_col)/F.col(num_col_name))))
            self._data=self._data.withColumn(_log_ror_col_name,
                                             F.log10(F.col(_prev_day_num_col)/F.col(num_col_name)))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data.sort(F.col("mcap_date"),F.col(_log_ror_col_name)), _log_ror_col_name


    ''' Function ---  ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def bestweights(func):
        """
        Description:
            wrapper function to get the best weights
        Attributes:
            func - inherits update_weighted_portfolio
        Returns:
            bestweights_wrapper
        """
        @functools.wraps()
        def bestweights_wrapper(self,data,date_col,val_col,topN,size,**kwargs):
            """
            Description:
                Loops through the unique dates to compute the optimal weights and the
                respective portfolio weighted sum and assets
            Attributes:
                same attributes as in the update_weighted_portfolio function; see below
            """

            try:
                _asset_data = func(data,date_col,val_col,topN,size,**kwargs)

#                 for _date in _dates_list:
#                     ''' convert dataframe to an array '''
#                     _date_df = data.filter(col(date_col)==_date).toPandas()

#                 ''' initialize vars'''
#                 sum_weighted_index=np.nan
#                 weighted_mc_returns=np.nan
#                 _max_weight_row=np.nan
#                 _top_assets_byDate_df = data_df.loc[data_df['Date'] == date]
#                 rand_arr = []
#                 ''' get random weights for the number of significant assets '''
#                 weights=np.random.dirichlet(np.ones(_top_assets_byDate_df.shape[0]),size=size)
# #                _top_asset_arr = np.array(_top_assets_byDate_df[value_col_name])
# #                weighted_return_arr = np.multiply(_top_asset_arr,weights)
#                 weighted_return_arr = np.multiply(np.array(_top_assets_byDate_df[value_col_name]),weights)
#                 ''' compute the market cap weighted sum '''
#                 sum_weighted_index = np.sum(weighted_return_arr, axis=1)
#                 _max_weight_row = np.argmax(np.array(sum_weighted_index), axis=0)
#                 if 'market_cap' in _top_assets_byDate_df.columns:
# #                     weighted_mc_returns = np.sum(np.multiply(np.array(_top_assets_byDate_df['market_cap']),weights))
#                     weighted_mc_returns = np.sum(np.multiply(np.array(_top_assets_byDate_df['market_cap']),
#                                                              weights[_max_weight_row]))
#                 _l_exp_ret.append({
#                                 'date' : _top_assets_byDate_df['Date'].max(),
#                                 # 'date' : str(date.astype('datetime64[D]')),
#                                 'coins' : list(_top_assets_byDate_df['ID']),
#                                 'max_sum_row' : _max_weight_row,
#                                 value_col_name: list(_top_assets_byDate_df[value_col_name]),
#                                 #'weighted_sum' : sum_weighted_index,
#                                 'best_weights': list(weights[_max_weight_row]),
#                                 'weighted_'+value_col_name+'_sum' : sum_weighted_index[_max_weight_row],
#                                 'weighted_market_cap_returns': weighted_mc_returns,
#                                 })

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return None

        return bestweights_wrapper

#     @bestweights
    def update_weighted_portfolio(
        self,
        data,
        date_col:str="",   # the date or datetime column name
        val_col :str="",   # the mcap value column name
        topN:int=23,   # number of top N assets to consider
        size:int=100,  # size of the randomized weights array
        **kwargs,
    ):
        """
        Description:
            Given a DataFrame of assets and mcap values for a given data range,
            compute the weights that offer the maximum weighted sum
        Attribute:
            data (DataFrame) with mcap date and value
            date_col
            val_col (str) representing the numeric column name
            size (int) of the randomly generated weights array
            topN (int) number of assets to consider for a portfolio (default = 23)
            **kwargs
        Results:
            
        """

        __s_fn_id__ = "function <get_log_ror>"

        try:
            if data.count()==0:
                raise AttributeError("DataFrame cannot be empty")
            if date_col not in data.columns or \
                data.select(F.col(date_col)).dtypes[0][1] not in ['date','timestamp']:
                raise AttributeError("%s is invalid, select a numeric column from %s"
                                     % (date_col,data.dtypes))
            if val_col not in data.columns or \
                data.select(F.col(val_col)).dtypes[0][1] == 'string':
                raise AttributeError("%s is invalid, select a numeric column from %s"
                                     % (val_col,data.dtypes))
            ''' remove any rows with null mcap value '''
            _valid_asset_data = data.where(F.col(val_col).isNotNull())
            if _valid_asset_data.count()==0:
                raise AttributeError("In DataFrame with %d rows %s columns values are Null."
                                     % (data.count(),val_col))
            if topN < 2:
                raise AttributeError("topN %d is invalid must be > 1"% (topN))
            if size < 1:
                raise AttributeError("Size %d is invalid must be > 0"% (size))

            ''' get a list of the unique dates '''
            _dates_list = []
#             _dates_list = list(set([x[0] for x in _mcap_log_ror.select('mcap_date').collect()]))
            _dates_list = list(_valid_asset_data.select(F.col(date_col)).toPandas()[date_col].unique())
            ''' ensure there is data for the date '''
            _valid_date_list = []
            for _date in _dates_list:
                if _valid_asset_data.filter(F.col(date_col)==_date).count() >= topN:
                    _valid_date_list.append(_date)
            if len(_valid_date_list)<=0:
                raise AttributeError("No data for the dates %s with data >= %d were found."
                                     % (str(_dates_list),topN))
            logger.debug("Proceeding with data for with %s dates",str(_valid_date_list))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _valid_date_list, _valid_asset_data


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
        __prev_val_pofix__ = "_prev_val"
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
            if "PREVALCOLNAME" in kwargs.keys():
                _prev_val = kwargs['PREVALCOLNAME']
            _prev_val = num_column+__prev_val_pofix__

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
