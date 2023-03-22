#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "dailyTopN"
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

class WeightedPortfolio():

    def __init__(self, desc : str="market cap data prep", **kwargs):
        """
        Desciption:
            Initialize the class
        Attributes:
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
        self._portfolio=None

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

    ''' Function --- PORTFOLIO ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def portfolio(self) -> list:
        """
        Description:
            portfolio @property function. make sure it is a valid spark dataframe
        Attributes:
            data in @setter will instantiate self._data    
        Returns (dataframe) self._data
        """

        __s_fn_id__ = "function <@property portfolio>"

        try:
            if not isinstance(self._portfolio,list):
#                 pass
#             elif isinstance(self._portfolio,pd.DataFrame):
#                 self._portfolio = clsSDB.data(self._portfolio)
#             elif isinstance(self._portfolio,list):
#                 self._portfolio = clsSDB.data(pd.DataFrame(self._portfolio))
#             elif isinstance(self._portfolio,dict):
#                 self._portfolio = clsSDB.data(pd.DataFrame([self._portfolio]))
#             else:
                raise AttributeError("Invalid self._portfolio; must a valid pyspark DataFrame dtype")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio

    @portfolio.setter
    def portfolio(self,portfolio:list=[]) -> list:

        __s_fn_id__ = "function <@setter portfolio>"

        try:
            if len(portfolio)<=0:
#             if portfolio is None:
#                 raise AttributeError("Invalid portfolio attribute, must be a valid non-empty list")
#             if isinstance(portfolio,DataFrame):
#                 self._portfolio = portfolio
#             elif isinstance(portfolio,pd.DataFrame):
#                 self._portfolio = clsSDB.data(portfolio)
#             elif isinstance(portfolio,list):
#                 self._portfolio = clsSDB.data(pd.DataFrame(portfolio))
#             elif isinstance(portfolio,dict):
#                 self._portfolio = clsSDB.data(pd.DataFrame([portfolio]))
#             else:
                raise AttributeError("Invalid portfolio attribute, must be a non-empy list")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio

#     ''' Function --- PREVIOUS VAL DIFFERENCE ---

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     @staticmethod
#     def prev_val_diff(
#         data:DataFrame,
#         num_column:str,
#         part_column:str,
#         **kwargs,
#     ) -> DataFrame:
#         """
#         Description:
#             for a given numeric column, the function computes the difference between
#             the current cell and the previous cell
#         Attributes:
#             data (DataFrame) a valid pyspark dataframe
#             column - specifies column to compute the difference
#             **kwargs
#                 DIFFCOLNAME - the column name
#         Returns:
#         """

#         __s_fn_id__ = "function <unpivot_table>"
#         _diff_data = None
#         _diff_col = "diff"
#         __prev_val_pofix__ = "_prev_val"
#         _prev_val=None

#         try:
#             if data.count() <= 2:
#                 raise AttributeError("Dataframe must have, at least, 2 rows to compute the difference ")
#             if num_column not in data.columns and \
#                 not isinstance(data.num_column,int) and\
#                 not isinstance(data.num_column,float):
#                 raise AttributeError("%s must be a numeric dataframe column" % num_column)
#             if part_column not in data.columns:
#                 raise AttributeError("%s must be a column in the dataframe: %s" 
#                                      % (num_column,data.columns))
#             if "DIFFCOLNAME" in kwargs.keys():
#                 _diff_col = kwargs['DIFFCOLNAME']
#             if "PREVALCOLNAME" in kwargs.keys():
#                 _prev_val = kwargs['PREVALCOLNAME']
#             _prev_val = num_column+__prev_val_pofix__

#             _win = Window.partitionBy(part_column).orderBy(part_column)
#             _diff_data = data.withColumn(_prev_val, F.lag(data[num_column]).over(_win))
#             _diff_data = _diff_data.withColumn(_diff_col,\
#                                     F.when(\
#                                       F.isnull(_diff_data[num_column] - _diff_data[_prev_val]), 0)\
#                                       .otherwise(_diff_data[num_column] - _diff_data[_prev_val]))

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             print("[Error]"+__s_fn_id__, err)
#             print(traceback.format_exc())

#         return _diff_data, _prev_val, _diff_col


    ''' Function --- READ ROR ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    def read_ror(self, select:str="", **kwargs):
        """
        Description:
            The key feature is to read the mcap data from postgresql and impute to ensure
            the data is clean and useable. There are two options for reading the data
             (i) giving an SQL select as a string
            (ii) defining the table name to read the entire dataset
            Makes use of sparkDBwls package to read the data from the DB
        Arguments:
            select (str) - valid SQL select statement with any SQl clauses
            **kwargs - specifying key value pairs
                TABLENAME - db_able name with or without the schema name
                COLUMN - partition column name
                FROMDATETIME - timestamp setting the partition column lower-bound
                TODATETIME - timestamp setting the partition column upper-bound
        Returns: self._data (dataframe)
        """

        __s_fn_id__ = "function <read_ror>"
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

            if select is not None and "".join(select.split())!="":
                self._data = clsSDB.read_data_from_table(select=select, **kwargs)
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


    ''' Function --- GET MAXIMUM WEIGHTED PORTFOLIO ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def maxportfolio(func):
        """
        Description:
            wrapper function to get the best weights
        Attributes:
            func - inherits get_weighted_mpt
        Returns:
            maxweights_wrapper
        """

        @functools.wraps(func)
        def maxportfolio_wrapper(self,data,cols_dict,topN,size,**kwargs):
            """
            Description:
                Loops through the unique dates to compute the optimal weights and the
                respective portfolio weighted sum and assets
            Attributes:
                same attributes as in the get_weighted_mpt function; see below
            Returns:
                _l_exp_ret (list) maximum weighted portfolio results
            """

            __s_fn_id__ = "function <maxportfolio_wrapper>"
            _l_exp_ret=[]   # hold list of weighted portfolio results

            try:
                _date_list, _asset_data = func(self,data,cols_dict,topN,size,**kwargs)

                for _date in _date_list:
                    ''' initialize vars'''
                    _sum_wt_index=np.nan   # sum of weigted index
                    _wt_mc_rets=np.nan     # weighted marcket cap returns
                    _max_weight_row=np.nan  # row with maximum weights
                    _asset_byDate_df = None # dataframe of rows for particular date
                    try:
                        ''' convert dataframe to an array '''
                        _asset_byDate_df = _asset_data.filter(F.col(cols_dict['DATECOLUMN'])==_date)\
                                            .toPandas()
                        _asset_byDate_df.sort_values(by=[cols_dict['NUMCOLUMN']],axis=0,
                                                     ascending=False,na_position='last',inplace=True)
                        _asset_byDate_df.dropna(subset=[cols_dict['NUMCOLUMN']],
                                                how='any',axis=0,inplace=True)
                        if _asset_byDate_df.shape[0] <= 0:
                            raise ValueError("No asset data rows for date: %s",str(_date))
                        logger.debug("Retrieved %d asset data rows for %s"
                                     ,_asset_byDate_df.shape[0],str(_date))
                        _topN_byDate = topN
                        if _asset_byDate_df.shape[0] < topN:
                            _topN_byDate = _asset_byDate_df.shape[0]
                        _top_assets_byDate_df = _asset_byDate_df.iloc[:_topN_byDate]
                        logger.debug("%d rows for %d topN retrieved assets: %s "
                                     ,_top_assets_byDate_df.shape[0]
                                     ,_topN_byDate
                                     ,str(_top_assets_byDate_df[cols_dict['NAMECOLUMN']].unique()))
#                         _top_asset_arr = np.array(_top_assets_byDate_df[val_col])
#                         logger.debug("Creating top %d asset mcap value %s dimensional array"
#                                      ,_topN_byDate,str(_top_asset_arr.ndim))

                        ''' get random weights for the number of significant assets '''
                        weights=np.random.dirichlet(np.ones(_topN_byDate),size=size)
                        logger.debug("Generated random weights with dimensions %s", str(weights.ndim))
                        logger.debug("%s",str(weights))

                        _wt_ret_arr = np.multiply(\
                                        np.array(\
                                                 _top_assets_byDate_df[cols_dict['NUMCOLUMN']]\
                                                 .astype('float')),\
                                                  weights)
                        logger.debug("Multiplied array generated weighted returns with %s dimensions"
                                     ,str(_wt_ret_arr.ndim))
                        logger.debug("%s",str(_wt_ret_arr))

                        ''' compute the market cap weighted sum '''
                        _sum_wt_index = np.sum(_wt_ret_arr, axis=1)
                        logger.debug("Sum of each randomized weighted portfolio with %s dimensions %s"
                                     ,str(_sum_wt_index.ndim),str(_sum_wt_index))
                        _max_wt_row = np.argmax(np.array(_sum_wt_index), axis=0)
                        logger.debug("Maximum weighted row index %s",str(_max_wt_row))

                        ''' append results dictionary to list '''
                        _l_exp_ret.append({
                                        cols_dict["MCAPSOURCE"]:'warehouse.mcap_past',
                                        cols_dict["PRIMARYKEY"]:list(\
                                                         _top_assets_byDate_df['mcap_past_pk']),
                                        cols_dict['DATECOLUMN']: _date,
                                        cols_dict['NAMECOLUMN']: list(\
                                                         _top_assets_byDate_df[cols_dict['NAMECOLUMN']]),
                                        cols_dict['NUMCOLUMN']:list(\
                                                         _top_assets_byDate_df[cols_dict['NUMCOLUMN']]),
                                        cols_dict['WEIGHTCOLUMN']: list(weights[_max_wt_row]),
                                        cols_dict['MCAPCOLUMN']:list(\
                                                         _top_assets_byDate_df[cols_dict['MCAPCOLUMN']]),
                                        })

                    except Exception as asset_err:
                        logger.warning("Dates loop %s",asset_err)
                        pass

                if len(_l_exp_ret) > 0:
                    self._portfolio = _l_exp_ret
                    logger.info("Completed maximized weights for %d portfolio(s)"
                                ,len(self._portfolio))
                else:
                    raise ValueError("Empty weighted portfolios")

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._portfolio, cols_dict

        return maxportfolio_wrapper


    @maxportfolio
    def get_weighted_mpt(
        self,
        data,
        cols_dict:dict={},   # dict defining the column names
        topN:int=23,   # number of top N assets to consider
        size:int=100,  # size of the randomized weights array
        **kwargs,
    ):
        """
        Description:
            Given a DataFrame of assets and mcap ror values for a given data range:
            - validate the dataframe and parameters
            - Process will remove any rows will null ror column values
            - compute the maximal weights that offer the maximum weighted sum.
            - return the weighted portfolio data dictionary.
        Attribute:
            data (DataFrame) with mcap date and value
            date_col
            val_col (str) representing the numeric column name
            name_col (str) representing the asset name column
            size (int) of the randomly generated weights array
            topN (int) number of assets to consider for a portfolio (default = 23)
            **kwargs
        Results:
            valid_date_list
            valid_data
        """

        __s_fn_id__ = "function <get_weighted_mpt>"
        __date_col_name__= 'mcap_date'
        __ror_col_name__ = 'log_ror'
        __coin_col_name__= 'asset_name'
        __mcap_col_name__= 'mcap_value'
        __wts_col_name__ = 'weights'

        try:
            ''' validate dataframe '''
            if data is None or data.count()==0:
                raise AttributeError("DataFrame cannot be empty")
            logger.debug("%s ready process %d rows in dataframe",__s_fn_id__,data.count())
            ''' validate parameters '''
            if "NUMCOLUMN" not in cols_dict.keys():
                cols_dict['NUMCOLUMN']=__ror_col_name__
            if data.select(F.col(cols_dict['NUMCOLUMN'])).dtypes[0][1] == 'string':
                raise AttributeError("%s is invalid ror column, select a numeric column from %s"
                                     % (cols_dict['NUMCOLUMN'],data.dtypes))
            ''' remove any rows with null ror value '''
            valid_asset_data_ = data.where(F.col(cols_dict['NUMCOLUMN']).isNotNull())
            if valid_asset_data_.count()==0:
                raise AttributeError("In DataFrame with %d rows %s columns all values are Null."
                                     % (data.count(),cols_dict['NUMCOLUMN']))
            logger.debug("Retrieved a valid dataframe with %d of %d rows from orignal dataframe"
                         ,valid_asset_data_.count(),data.count())

            ''' if not defined then add the column names to dict '''
            if "NAMECOLUMN" not in cols_dict.keys():
                cols_dict['NAMECOLUMN']=__coin_col_name__
            if "DATECOLUMN" not in cols_dict.keys():
                cols_dict['DATECOLUMN']=__date_col_name__
            if valid_asset_data_.select(F.col(cols_dict['DATECOLUMN']))\
                                        .dtypes[0][1] not in ['date','timestamp']:
                raise AttributeError("%s is invalid date column select from %s"
                                     % (cols_dict['DATECOLUMN'],valid_asset_data_.dtypes))
            if "MCAPCOLUMN" not in cols_dict.keys():
                cols_dict['MCAPCOLUMN']=__mcap_col_name__
            if valid_asset_data_.select(F.col(cols_dict['MCAPCOLUMN'])).dtypes[0][1] == 'string':
                raise AttributeError("%s is invalid mcap value column, select a numeric column from %s"
                                     % (cols_dict['MCAPCOLUMN'],valid_asset_data_.dtypes))
            if "WEIGHTCOLUMN" not in cols_dict.keys():
                cols_dict['WEIGHTCOLUMN']=__wts_col_name__

            if topN < 2:
                raise AttributeError("topN %d is invalid must be > 1"% (topN))
            if size < 1:
                raise AttributeError("Size %d is invalid must be > 0"% (size))

            ''' get a list of the unique dates '''
            _dates_list = []
            _dates_list = valid_asset_data_\
                            .select(F.col(cols_dict['DATECOLUMN']))\
                            .distinct().collect()
            ''' ensure there is data for the date '''
            valid_date_list_ = []
            for _date in _dates_list:
                if valid_asset_data_.filter(F.col(cols_dict['DATECOLUMN'])==_date[0]).count() > 0:
                    valid_date_list_.append(_date[0])
            ''' are there any dates with valid data? '''
            if len(valid_date_list_)<=0:
                raise AttributeError("No data for the dates %s with data >= %d were found."
                                     % (str(_dates_list),topN))
            logger.debug("Proceeding with data for %s dates",str(valid_date_list_))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return valid_date_list_, valid_asset_data_


    ''' Function --- PORTFOLIO To DB ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def dbwrite(func):
        """
        Description:
            wrapper function to write dicts to nosqldb
        Attributes:
            func - inherits write_mpt_to_db
        Returns:
            dbwrite_wrapper
        """
        @functools.wraps(func)
        def dbwrite_wrapper(self,mpt_data,cols_dict,**kwargs):

            __s_fn_id__ = "function <dbwrite_wrapper>"

            __destin_db_name__ = "tip-daily-mpt"
            __destin_coll_prefix__ = 'mpt'
            __uids__ = ['date',  # portfolio date
                        'asset']  # asset name

            try:
                _data = func(self,mpt_data,cols_dict,**kwargs)

                ''' set database name & check exists'''
                _destin_db = __destin_db_name__
                if "DESTINDBNAME" in kwargs.keys():
                    _destin_db = kwargs["DESTINDBNAME"]
                if "DBAUTHSOURCE" in kwargs.keys():
                    clsNoSQL.connect={'DBAUTHSOURCE':kwargs['DBAUTHSOURCE']}
                else:
                    clsNoSQL.connect={'DBAUTHSOURCE':_destin_db}
                ''' confirm database exists '''
                if not _destin_db in clsNoSQL.connect.list_database_names():
                    raise RuntimeError("%s does not exist",_destin_db)

                ''' set collection prefix '''
                _mpt_coll_prefix = __destin_coll_prefix__
                if "COLLPREFIX" in kwargs.keys():
                    _mpt_coll_prefix = kwargs["COLLPREFIX"]

                ''' write portfolio to collection '''
                _colls_list=[]
                _uniq_dates = set([x['date'] for x in _data])
                for _date in _uniq_dates:
                    _mpt_for_date = list(filter(lambda d: d['date'] == _date, _data))
                    _destin_coll = '.'.join([
                        _mpt_coll_prefix,"top"+str(len(_mpt_for_date)),_date.split('T')[0]
                    ])
                    _mpt_coll = clsNoSQL.write_documents(
                        db_name=_destin_db,
                        db_coll=_destin_coll,
                        data=_mpt_for_date,
                        uuid_list=__uids__)
                    _colls_list.append(_mpt_coll)
                    logger.debug("%d documents written to %s collection"
                                 ,len(_mpt_for_date),_destin_coll)
                ''' confirm returned collection counts '''
                if len(_colls_list) > 0:
                    self._portfolio = _colls_list
                    logger.info("Wrote %d mpt collections successfully to %s %s",
                                len(self._portfolio),clsNoSQL.dbType,_destin_db)
                else:
                    raise RuntimeError("Something was wrong with writing mpt to collections in %s %s"
                                       ,clsNoSQL.dbType,_destin_db)

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._portfolio

        return dbwrite_wrapper

    @dbwrite
    def write_mpt_to_db(
        self,
        mpt_data:list=[],   # data with portfolio weights, ror, and assets
        cols_dict:dict={},  # dict defining the column names
        **kwargs
    ):
        """
        Description:
            writes the mpt data; namely the assets, weights, and mcap for each date
        Attributes:
            data (list) of dicts with the asset-wise, weight, log-ror, & mcap-value
        Returns:
            
        """

        __s_fn_id__ = "function <write_mpt_to_db>"
        _mpt_list = []

        try:
            if len(mpt_data)==0:
                raise AttributeError("Cannot process an empty mpt list")
            if cols_dict=={}:
                raise AttributeError("Cannot process with an empty cols_dict")

            for idx,data in enumerate(mpt_data):
#                 _mpt_dict = {}
                for x in zip(data[cols_dict['NAMECOLUMN']],
                             data[cols_dict['WEIGHTCOLUMN']],
                             data[cols_dict['NUMCOLUMN']],
                             data[cols_dict['MCAPCOLUMN']],
                             data[cols_dict['PRIMARYKEY']],
#                              data[cols_dict['MCAPSOURCE']],
                            ):
                    ''' convert column names to nosql naming '''
                    _mpt_dict = {}
                    _mpt_dict["date"]=data[cols_dict['DATECOLUMN']].strftime("%Y-%m-%dT%H:%M:%S")
                    _mpt_dict["asset"]=x[0]
                    _mpt_dict["mcap.weight"]=float(x[1])
                    _mpt_dict["mcap.ror"]=float(x[2])
                    _mpt_dict["mcap.value"]=float(x[3])
                    _mpt_dict["mcap.db.fk"]=str(x[4])
                    _mpt_dict["mcap.db.source"]=data[cols_dict['MCAPSOURCE']]
                    _mpt_dict["audit.mod.by"]=os.environ.get('USERNAME').upper()
                    _mpt_dict["audit.mod.dt"]=datetime.strftime(datetime.now(),'%Y-%m-%dT%H:%M:%S')
                    _mpt_dict["audit.mod.proc"]="-".join([self.__name__,__s_fn_id__])
                    ''' append the mpt dict to list '''
                    _mpt_list.append(_mpt_dict)

            if len(_mpt_list)>0:
                self._portfolio=_mpt_list
                logger.debug("Created database collection ready dict list with %d documents"
                          ,len(self._portfolio))
            else:
                raise ValueError("Empty mpt list, not data to write")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio

