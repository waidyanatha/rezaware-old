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
            from utils.modules.etl.loader import sparkDBwls as sparkDB
            clsSDB = sparkDB.SQLWorkLoads(desc=self.__desc__)
            ''' import spark clean-n-rich work load utils to transform the data '''
            from utils.modules.etl.transform import sparkCleanNRich as sparkCNR
            clsSCNR = sparkCNR.Transformer(desc=self.__desc__)
            ''' import mongo work load utils to read and write data '''
            from utils.modules.etl.loader import sparkNoSQLwls as nosql
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
            __def_batch_size__=100
            
            try:
                data_ = func(self, query, **kwargs)

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
                if "BATCHSIZE" not in kwargs.keys():
                    kwargs['BATCHSIZE']=__def_batch_size__
                    logger.warning("%s undefined kwargs BATCHSIZE, setting to default to: %d",
                                   __s_fn_id__,kwargs['BATCHSIZE'])
                else:
                    logger.debug("%s using set kwargs BATCHSIZE: %d ",
                                 __s_fn_id__,kwargs['BATCHSIZE'])

                _part_col_list = list(set([x[0] 
                                           for x in data_.select(F.col(kwargs['PARTCOLNAME']))\
                                           .collect()]))
                _part_col_list.sort()
                logger.debug("%s Exctracted a sorted list of %d unique partition columns",
                             __s_fn_id__,len(_part_col_list))
#                 ''' start data property from begining '''
                data_col_list = data_.columns
                ''' loop to transpose, impute, transpose, add missing values to each batch''' 
                for batch_num in range(0,len(_part_col_list),kwargs['BATCHSIZE']):
                    part_val_list = _part_col_list[batch_num : batch_num+kwargs['BATCHSIZE']]
                    logger.debug("%s processing batch from %d to %d partition values.",
                                 __s_fn_id__,batch_num,(batch_num+kwargs['BATCHSIZE']))
                    part_data_ = data_.filter(F.col(kwargs['PARTCOLNAME']).isin(part_val_list))
                    logger.debug("%s filtered %d data rows",__s_fn_id__,part_data_.count())
                    
                    try:
                        ''' transpose to get assets in the columns '''
                        _pivot_sdf=clsSCNR.pivot_data(
        #                     data=self._data,
                            data=part_data_,
                            group_columns=kwargs['DATECOLNAME'], # mcap_date,price_date,volume_date,etc
                            pivot_column =kwargs['PARTCOLNAME'], # asset_name,
                            agg_column = kwargs['VALUECOLNAME'], # mcap_value, price_value
                            **kwargs,
                        )
                        if _pivot_sdf is None or _pivot_sdf.count()<=0:
                            raise RuntimeError("Pivot_data method returned a %s type object, aborting!" 
                                               % type(_pivot_sdf))
                        logger.debug("%s pivot method returned table with %d columns and %d rows " +\
                                     "for group by: %s, pivot column: %s, and aggregate column: %s ",
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
                            raise RuntimeError("Impute_data method returned a %s object, aborting! "
                                               % type(_pivot_sdf))
                        logger.debug("%s impute_data method on all %d %s partition columns; " +\
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
                            value_columns=[kwargs['PARTCOLNAME'],kwargs['VALUECOLNAME']],
                            where_cols = kwargs['VALUECOLNAME'],
                            **kwargs
                        )

                        if _unpivot_sdf is None or _unpivot_sdf.count() <= 0:
                            raise ValueError("Unpivot returned %s dataset" % type(_unpivot_sdf))
                        part_data_=part_data_.drop(kwargs['VALUECOLNAME'])
                        part_data_ = part_data_.join(_unpivot_sdf,
                                                     [kwargs['PARTCOLNAME'],kwargs['DATECOLNAME']],
                                                     "fullouter")
                        logger.debug("%s After unpivot, dataframe with rows %d columns %d",
                                     __s_fn_id__,part_data_.count(),len(part_data_.columns))

                        ''' add missing values; e.g. asset_symbol '''
                        null_cols_list = ["currency","asset_symbol"]
                        for _null_col in null_cols_list:
                            part_data_ = part_data_.orderBy(F.col(_null_col).asc())
                            part_data_ = part_data_.withColumn(_null_col,\
                                                               F.first(_null_col,ignorenulls=True)\
                                               .over(Window.partitionBy(kwargs['PARTCOLNAME'])))
                        logger.debug("%s Replaced Null values in dataframe columns %s",
                                     __s_fn_id__,str(null_cols_list))
                        ''' merge data from partioned and imputed dataframe with full dataset ''' 
                        self._data = self._data.alias('data').join(
                            part_data_.alias('part'),
                            [kwargs['PARTCOLNAME'], kwargs['DATECOLNAME'], kwargs['VALUECOLNAME']],
                             'leftouter').select(*[f"data.{x}" for x in data_col_list])
                        logger.debug("%s Batch %d join augmented %d of %d rows "+\
                                     "to dataframe with %d columns",
                                     __s_fn_id__,batch_num/kwargs['BATCHSIZE'],part_data_.count(),
                                     self._data.count(),len(self._data.columns))

                    except Exception as imp_err:
                        logger.warning("%s %s had errors \n%s",__s_fn_id__,part_val_list,imp_err)

                if self._data is None or self._data.count()<=0:
                    raise RuntimeError("class property data retuned an empty %s dataframe"
                                       % type(self._data))
                logger.debug("%s successfully completed processing %d batches "+\
                             "of %d rows and %d columns",
                             __s_fn_id__,1+batch_num/kwargs['BATCHSIZE'],
                             self._data.count(),len(self._data.columns))

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


    ''' Function --- CALC ROR ---

            author: <nuwan.waidyanatha@rezgateway.com>
            
            resources:
                all ROR methods: https://en.wikipedia.org/wiki/Rate_of_return
                Logarithmic ROR: https://www.rateofreturnexpert.com/log-return/
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
            if "DIFFCOLNAME" in kwargs.keys() and "".join(kwargs['DIFFCOLNAME'].split())!="":
                _diff_col = kwargs['DIFFCOLNAME']
            _prev_val = "_".join([num_column,__prev_val_pofix__])
            if "PREVALCOLNAME" in kwargs.keys() and "".join(kwargs['PREVALCOLNAME'].split())!="":
                _prev_val = kwargs['PREVALCOLNAME']

            _win = Window.partitionBy(part_column).orderBy(part_column)
            _diff_data = data.withColumn(_prev_val, F.lag(data[num_column]).over(_win))
            _diff_data = _diff_data.withColumn(_diff_col,\
                                    F.when(\
                                      F.isnull(_diff_data[num_column] - _diff_data[_prev_val]), None)\
                                      .otherwise(_diff_data[num_column] - _diff_data[_prev_val]))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _diff_data, _prev_val, _diff_col


    def apply_ror_method(func):
        """
        Description:
            wrapper function to calculate an ROR method
        Attributes:
            func inherits get_ror
        Returns:
            ror_calc_wrapper
        """
        @functools.wraps(func)
        def ror_method_wrapper(self,data,ror_type,num_col,part_col,date_col,**kwargs):
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
            __s_fn_id__ = f"{self.__name__} function <ror_method_wrapper>"
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
                                                F.when(F.isnull(
                                                    F.log10(F.col(num_col)/F.col(_prev_col))),
                                                       None)\
                                                .otherwise(
                                                    F.log10(F.col(num_col)/F.col(_prev_col))))
                elif ror_type.upper()=='LOG2':
                    self._data=_data.withColumn(_ror_col,
                                                F.when(F.isnull(
                                                    F.log2(F.col(num_col)/F.col(_prev_col))),
                                                       None)\
                                                .otherwise(
                                                    F.log2(F.col(num_col)/F.col(_prev_col))))
#                 elif ror_type.upper()=='LOG2':
#                     self._data=_data.withColumn(_ror_col,
#                                                  F.log2(F.col(num_col)/F.col(_prev_col)))
                elif ror_type.upper() in ['NATLOG','NATURALLOG','LN']:
                    self._data=_data.withColumn(_ror_col,
                                                F.when(F.isnull(
                                                    F.log1p(F.col(num_col)/F.col(_prev_col))),
                                                       None)\
                                                .otherwise(
                                                    F.log1p(F.col(num_col)/F.col(_prev_col))))
                elif ror_type.upper() in ['SIMPLE','SIMP']:
                    self._data=_data.withColumn(_ror_col,
                                                F.when(F.isnull(
                                                    ((F.col(num_col)-F.col(_prev_col))\
                                                     /F.col(num_col))), None)\
                                                .otherwise(((F.col(num_col)-F.col(_prev_col))\
                                                     /F.col(num_col))))
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

        return ror_method_wrapper

    @apply_ror_method
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
            if "DIFFCOLNAME" not in kwargs.keys() or "".join(kwargs['DIFFCOLNAME'].split())=="":
                kwargs['DIFFCOLNAME']="_".join([__diff_prefix__,num_col])

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
#                 data_has_pk_.write.options(header='True', delimiter=',') \
#                             .mode('overwrite') \
#                             .csv(os.path.join(pkgConf.get("CWDS","DATA"),
#                                               "tmp","data_has_pk.csv"))
                has_pk_rec_count_=clsSDB.upsert_sdf_to_table(
                    save_sdf=data_has_pk_,
                    db_table=kwargs['TABLENAME'],
                    unique_keys=_pk,
                    **_options,
                )
                if has_pk_rec_count_ is None or has_pk_rec_count_<=0:
                    has_pk_rec_count_=0
                    logger.error("%s Failed upsert %d records into %s %s database in %s table",
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


    ''' Function --- BATCH ETL ROR ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def enrich_with_ror(
        self,
        from_date:date=None, # start date of the time period
        to_date:date = None, # end date of the time period
        batch_size:int = 100,    # maximum number of records to process each time
        value_limit:float=100000, # mcap or price minimum value to filter records
        **kwargs,
    ):
        """
        Description:
            * Loop through to apply the process for each mcap and price asset data 
            * For a given start and end date range and minimum value limit,
                the process will execute the read_n_clean_mcap function to:
                - read the data from the SQL database table
                - impute any missing data with a desired strategy like 'mean'
                - fill in any missing categorical values, like 'currency'unit
            * Execute calc_ror function to calculate the Log and Simp ROR for:
                - mcap value
                - price value
        Attributes:
            from_date (date)-start date of the time period
            to_date (date) - end date of the time period
            batch_size (int)-maximum number of records to process each time
            value_limit (float) - mcap or price minimum value to filter records
        Returns:
            self._data (DataFrame) with the mcap and price ROR data
        """
        __s_fn_id__ = f"{self.__name__} function <enrich_with_ror>"

        ''' default storage places '''
        __def_ror_db_name__ = "tip"
        __def_ror_tbl_name__= "mcap_past"
        ''' default data filters '''
        __def_from_date__=date.today()
        __def_to_date__ = date.today()
        __def_value_limit__=10000000
        __def_batch_size__ =100
        ''' default computation parameters '''
        __def_col_prefixes__ = ["mcap","price"]
        __def_ror_method__  = ["LOG2","SIMP"]
        __def_aggr_method__ = "avg"
        __def_impute_method__="mean"
        ''' default column names '''
        __def_partition_col__= "asset_name"

        try:
            ''' valid input parameters '''
            # from (start) date; e.g. 2022-01-01
            if from_date is None or not isinstance(from_date,date):
                from_date = __def_from_date__
                logger.warning("%s Undefined from_date input parameter set to %s",
                               __s_fn_id__,str(from_date))
            else:
                logger.debug("%s from_date input parameter properly defined in input as %s",
                             __s_fn_id__,str(from_date))
            # to (end) date; e.g. 2022-01-02
            if to_date is None or not isinstance(to_date,date):
                to_date = __def_to_date__
                logger.warning("%s Undefined to_date input parameter set to %s",
                               __s_fn_id__,str(to_date))
            else:
                logger.debug("%s to_date input parameter properly defined in input as %s",
                             __s_fn_id__,str(to_date))
            # Value limit filter on mcap & price
            if not value_limit:
                value_limit=__def_value_limit__
                logger.warning("%s Undefined value_limit kwarg set to %s",
                               __s_fn_id__,value_limit)
            else:
                logger.debug("%s value_limit kwarg was defined in input as %s",
                             __s_fn_id__,value_limit)

            ''' validate and set key/value arguments '''
            # database name
            if "DBNAME" not in kwargs.keys() or "".join(split(kwargs['DBNAME']))=="":
                kwargs['DBNAME']=__def_ror_db_name__
                logger.warning("%s Undefined DBNAME kwarg set to %s",
                               __s_fn_id__,kwargs['DBNAME'].upper())
            else:
                logger.debug("%s DBNAME kwarg was defined in input as %s",
                             __s_fn_id__,kwargs['DBNAME'].upper())
            # table name
            if "TABLENAME" not in kwargs.keys() or "".join(split(kwargs['TABLENAME']))=="":
                kwargs['TABLENAME']=__def_ror_tbl_name__
                logger.warning("%s Undefined TABLENAME kwarg set to %s",
                               __s_fn_id__,kwargs['TABLENAME'].upper())
            else:
                logger.debug("%s TABLENAME kwarg was defined in input as %s",
                             __s_fn_id__,kwargs['TABLENAME'].upper())
            # partition column name
            if "PARTCOLNAME" not in kwargs.keys() or "".join(split(kwargs['PARTCOLNAME']))=="":
                kwargs['PARTCOLNAME']=__def_partition_col__
                logger.warning("%s Undefined PARTCOLNAME kwarg set to %s",
                               __s_fn_id__,kwargs['PARTCOLNAME'].upper())
            else:
                logger.debug("%s PARTCOLNAME kwarg was defined in input as %s",
                             __s_fn_id__,kwargs['PARTCOLNAME'].upper())
            # column prefix list
            if "VALCOLPREFIX" not in kwargs.keys() \
                or not isinstance(kwargs['VALCOLPREFIX'],list) \
                or len(list(set(kwargs['VALCOLPREFIX']).intersection(set(__def_col_prefixes__))))<=0:
                kwargs['VALCOLPREFIX']=__def_col_prefixes__
                logger.warning("%s Undefined VALCOLPREFIX kwarg set to %s",
                               __s_fn_id__,str(kwargs['VALCOLPREFIX']).upper())
            else:
                logger.debug("%s VALCOLPREFIX kwarg was defined in input as %s",
                             __s_fn_id__,str(kwargs['VALCOLPREFIX']).upper())
            # aggregate method
            if "AGGREGATE" not in kwargs.keys() or "".join(split(kwargs['AGGREGATE']))=="":
                kwargs['AGGREGATE']=__def_aggr_method__
                logger.warning("%s Undefined AGGREGATE kwarg set to %s",
                               __s_fn_id__,kwargs['AGGREGATE'].upper())
            else:
                logger.debug("%s AGGREGATE kwarg was defined in input as %s",
                             __s_fn_id__,kwargs['AGGREGATE'].upper())
            # impute method
            if "IMPUTESTRATEGY" not in kwargs.keys() or "".join(split(kwargs['IMPUTESTRATEGY']))=="":
                kwargs['IMPUTESTRATEGY']=__def_impute_method__
                logger.warning("%s Undefined IMPUTESTRATEGY kwarg set to %s",
                               __s_fn_id__,kwargs['IMPUTESTRATEGY'].upper())
            else:
                logger.debug("%s IMPUTESTRATEGY kwarg was defined in input as %s",
                             __s_fn_id__,kwargs['IMPUTESTRATEGY'].upper())
            # ROR computation types
            if "RORTYPE" not in kwargs.keys() or \
                len(list(set(kwargs['RORTYPE']).intersection(set(__def_ror_method__))))<=0:
                kwargs['RORTYPE']=__def_ror_method__
                logger.warning("%s Undefined RORTYPE kwarg set to %s",
                               __s_fn_id__,str(kwargs['RORTYPE']).upper())
            else:
                logger.debug("%s RORTYPE kwarg was defined in input as %s",
                             __s_fn_id__,str(kwargs['RORTYPE']).upper())

            ''' loop through each column prefix to impute and calc ror '''
            for _attr_prefix in kwargs['VALCOLPREFIX']:

                kwargs['DATECOLNAME'] = "_".join([_attr_prefix.lower(),'date'])
                kwargs['VALUECOLNAME']= "_".join([_attr_prefix.lower(),'value'])

                _query =f"SELECT wmp.mcap_past_pk, wmp.uuid, wmp.asset_symbol, "+\
                        f"wmp.{kwargs['DATECOLNAME']}, wmp.{kwargs['VALUECOLNAME']}, "+\
                        f"wmp.{kwargs['PARTCOLNAME']}, wmp.currency, "+\
                        f"wmp.created_dt,wmp.created_by,wmp.created_proc "+\
                        f"FROM warehouse.mcap_past wmp WHERE 1=1 "+\
                        f"AND wmp.{kwargs['DATECOLNAME']} between '{from_date}' AND '{to_date}' "+\
                        f"AND wmp.mcap_value > {value_limit} "+\
                        f"AND wmp.deactivate_dt IS NULL"

                try:
                    clean_sdf = self.read_n_clean_mcap(query=_query,**kwargs)
                    if clean_sdf is None or clean_sdf.count()<=0:
                        raise RuntimeError("read and impute data resulted in empty %s dataframe" 
                                           % type(clean_sdf))
                    logger.debug("%s read and impute returned %d rows and %d columns",
                                 __s_fn_id__,clean_sdf.count(),len(clean_sdf.columns))
                    clean_col_list = clean_sdf.columns
                    ''' Calculate the ROR for each value column; i.e. mcap and price '''
                    for _ror in kwargs['RORTYPE']:
                        ''' set all column prefix specific column names '''
                        try:
                            if _ror.upper() in ['NATLOG','LOG2','LOG10','LN']:
                                kwargs["RORCOLNAME"]="_".join([_attr_prefix,'log','ror'])
                            elif _ror.upper()=='SIMP':
                                kwargs["RORCOLNAME"]="_".join([_attr_prefix,_ror.lower(),'ror'])
                            else:
                                raise ValueError("Unrecognized ROR type")
                            kwargs["PREVALCOLNAME"]="_".join([_attr_prefix,'lag'])
                            kwargs["DIFFCOLNAME"] = "_".join([_attr_prefix,'diff'])
        #                     kwargs["RORCOLNAME"] = _ror_col
                            kwargs["PARTITIONS"] = 2

                            _ror_sdf, _ror_col = self.calc_ror(
                                data=clean_sdf,
                                ror_type=_ror,
                                num_col =kwargs['VALUECOLNAME'],
                                part_col=kwargs['PARTCOLNAME'],
                                date_col=kwargs['DATECOLNAME'],
                                **kwargs,
                            )
                            ''' write valid rows with ROR data to DB '''
                            _upsert_sdf=_ror_sdf.select('*')\
                                                .filter(F.col(kwargs["RORCOLNAME"]).isNotNull())
                            if _upsert_sdf is None or _upsert_sdf.count() <= 0:
                                raise RuntimeError("cal_ror returned an empty % dataframe" 
                                                   % type(_upsert_sdf))
                            _upsert_sdf=_upsert_sdf.withColumn("created_proc",
                                                        F.when(F.col('created_proc').isNull(),
                                                               "_".join([self.__app__,self.__module__,
                                                                         self.__package__,__s_fn_id__]))\
                                                   .otherwise(F.col('created_proc')))
                            logger.debug("%s ROR for %s calculation returned "+\
                                         "%d valid ROR value rows of %d cleaned data.",
                                         __s_fn_id__,_ror_col,_upsert_sdf.count(),clean_sdf.count())

                            ''' write data to database '''
                            _records=self.write_data_to_db(data=_upsert_sdf,)
                            if _records<=0:
                                raise RuntimeError("write_data_to_db failed saving %d rows to DB" 
                                                   % _upsert_sdf.count())
                            ''' merge full dataframe with upsert data to update dataframe '''
                            self._data = self._data.alias('data').join(
                                _upsert_sdf.alias('upsert'),
                                [kwargs['VALUECOLNAME'],kwargs['PARTCOLNAME'],kwargs['DATECOLNAME']], 
                                'leftouter').select(*[f"data.{x}" for x in clean_col_list])

                        except Exception as ror_err:
                            logger.warning("%s calc_ror %s and upsert had errors %s",
                                           __s_fn_id__,_ror, ror_err)

                except Exception as attr_pref_err:
                    logger.warning("%s read_n_clean %s had errors %s",
                                   __s_fn_id__,_attr_prefix, attr_pref_err)
                    
                if self._data is None or self._data.count()<=0:
                    raise RuntimeError("Failed to enrich with ROR data")
                logger.debug("Successfully completed enriching with %d ROR records",
                             self._data.count())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data