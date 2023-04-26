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
        self._index= None

        ''' functions '''
        global pkgConf
        global appConf
        global logger
        ''' classes '''
        global clsSDB
        global clsCNR
        global clsNoSQL
        global clsIndx
        global clsPCA

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
            ''' import mongo work load utils to read and write data '''
            from utils.modules.etl.load import sparkNoSQLwls as nosql
            clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)
            ''' import assset performance index class '''
            from mining.modules.assets.etp import performIndex as indx
            clsIndx =indx.Portfolio(desc=self.__desc__)
            ''' import feature engineering class '''
            from utils.modules.ml.dimreduc import pca
            clsPCA = pca.FeatureEngineer(desc=self.__desc__)
            ''' import spark clean-n-rich work load utils to transform the data '''
            from utils.modules.etl.transform import sparkCleanNRich as sparkCNR
            clsCNR = sparkCNR.Transformer(desc=self.__desc__)

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

            author: <samana.thetha@gmail.com
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

            author: <samana.thetha@gmail.com>
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
                raise AttributeError("Invalid portfolio attribute, must be a non-empy list")
            self._portfolio=portfolio

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio


    ''' Function --- INDEXES ---

            author: <samana.thetha@gmail.com
    '''
    @property
    def index(self):
        """
        Description:
            index @property and @setter functions to hold index measurements
        Attributes:
            None
        Returns (dataframe) self._index
        """

        __s_fn_id__ = "function <@property index>"

        try:
            if self._index is None:
                raise ValueError("%s Index is of NoneType; cannot be used in any %s computations"
                                 %(__s_fn_id__,self.__name__))
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._index

    @index.setter
    def index(self,index):

        __s_fn_id__ = "function <@setter index>"

        try:
            if index is None:
                raise AttributeError("Invalid index attribute")

            self._index = index

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._index


    ''' Function --- READ ROR ---

            author: <samana.thetha@gmail.com>
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

            author: <samana.thetha@gmail.com>
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
                    _sum_wt_index=np.nan    # sum of weigted index
                    _wt_mc_rets=np.nan      # weighted marcket cap returns
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


    ''' Function --- BUILD WEIGTED PORTFOLIO ---

            author: <samana.thetha@gmail.com>
    '''
    def build_weighted_portfolio(
        self,
        mcap_date:date=None,
        pick_asset_db_name:str=None,
        ror_db_name:str=None,
        **kwargs,
    ) -> DataFrame:
        """
        Description:
        Attributes:
        Returns:
        Exception:
        """
        
        __s_fn_id__ = "function <build_weighted_portfolio>"

        __def_portf_db_name__="tip-portfolio"
        __def_mcap_db_name__ = "warehouse.mcap_past"
        __def_portf_coll_prefix__="cherry_picked"
        __def_mpt_dest_db__ = 'tip-daily-mpt'
        __def_mpt_coll_prefix__ = 'mpt'

        _cols={
            "PRIMARYKEY":'mcap_past_pk',
            "UUID" : 'uuid',
            "NAMECOLUMN":'asset_name',
            "DATECOLUMN":'mcap_date',
            "NUMCOLUMN" :'mcap_log_ror',
            "MCAPCOLUMN":'mcap_value',
            "WEIGHTCOLUMN":'weights',
            "MCAPSOURCE":'source',
        }

        try:
            ''' validate input & initialize parameters '''
            if not isinstance(mcap_date,date):
                mcap_date=date.today()
            if not isinstance(pick_asset_db_name,str) or "".join(pick_asset_db_name.split())=="":
                pick_asset_db_name=__def_portf_db_name__
            ''' explicitly set DBAUTHSAOURCE '''
            kwargs['DBAUTHSOURCE']=pick_asset_db_name
            ''' list the keywords to filter list of collections '''
            kwargs['HASINNAME']=[str(mcap_date),__def_portf_coll_prefix__]

            ''' --- READ CHERRY-PICKED ASSETS --- '''
            self._portfolio = clsNoSQL.read_documents(
                as_type="list",
                db_name=pick_asset_db_name,
                db_coll=[],
                doc_find={},
                **kwargs
            )
            if self._portfolio is None or len(self._portfolio)<=0:
                raise RuntimeError("read documents returned an empty %s" 
                                   % (type(self._portfolio)))
            logger.debug("%s document read from %s returned %d rows",
                         __s_fn_id__,pick_asset_db_name,len(self._portfolio))

            ''' --- READ ROR DATA FROM DB --- '''
            _from_date = min([x['asset.mcap.date'] for x in self.portfolio]).split('T')[0]
            _to_date = max([x['asset.mcap.date'] for x in self.portfolio]).split('T')[0]
            _asset_collect = ",".join([f"'{x}'" for x in \
                                       [_p['asset.name'] for _p in self.portfolio]])

            if not isinstance(ror_db_name,str) or "".join(ror_db_name.split())=="":
                ror_db_name=__def_mcap_db_name__
            ''' execute sparkDBwls query '''
            _query =f"select * from {ror_db_name} "+\
                    f"where mcap_date between '{_from_date}' and '{_to_date}' "+\
                    f"and asset_name in ({_asset_collect}) and {_cols['NUMCOLUMN']} is not null"
            self._data = self.read_ror(select=_query,**kwargs)
            
            ''' --- BUILD WEIGHTED PORTFOLIO --- '''
            self._portfolio,_cols_dict=self.get_weighted_mpt(
                data=self._data,
                cols_dict=_cols,
                topN=3,
                size=5,
            )
            ''' --- WRITE PORTFOLIO TO DB ---
                write portfolio to mpt destination DB'''
            if not "DESTINDBNAME" in kwargs.keys() \
                    or "".join(split(kwargs['DESTINDBNAME']))=="":
                kwargs['DESTINDBNAME']=__def_mpt_dest_db__
            ''' Collection name will be set to mpt.YYYY-MM-DD '''
            if not "COLLPREFIX" in kwargs.keys() \
                    or "".join(split(kwargs['COLLPREFIX']))=="":
                kwargs['COLLPREFIX']=__def_mpt_coll_prefix__
            kwargs['DBAUTHSOURCE']=__def_mpt_dest_db__

            mpt_list_ = self.write_mpt_to_db(
                portfolio_data=self._portfolio,
                cols_dict=_cols,
                **kwargs,
            )
            if mpt_list_ is None or len(mpt_list_)<=0:
                raise RuntimeError("write_mpt_to_db returned an empty %s object" % type(mpt_list_))
            logger.debug("%s call to write_mpt_to_db succeded %d documents in %s DB",
                         __s_fn_id__,len(mpt_list_),kwargs['DESTINDBNAME'])

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio,self._data

#         return valid_date_list_, valid_asset_data_


    ''' Function --- SELECT TOP ASSETS ---

            author: <samana.thetha@gmail.com>
    '''
    def select_assets(func):
        """
        Description:
        Attributes:
        Returns:
            pca_wrapper
        """
        @functools.wraps(func)
        def select_wrapper(self,mcap_date,mcap_value_lb,**kwargs):
            """
            Description:
            Attributes:
            Returns:
                self._portfolio
            """

            __s_fn_id__ = "function <select_wrapper>"
            
            _pca_lb = 0.7

            try:
                _indic_df = func(self,mcap_date,mcap_value_lb,**kwargs)
 
                ''' Normalize the columns between 0 and 1.0 '''
                self._index=clsCNR.scale(
                    data=_indic_df,
                    col_list=['PC[0]','PC[1]'],
                    col_prefix=kwargs['COLPREFIX'],
                    scale_method=kwargs['STANDARDIZE'],
                )
                self._index = self._index.withColumn("1-PC[0]",1-F.col(kwargs['COLPREFIX']+'_PC[0]'))\
                            .withColumn("1-PC[1]",1-F.col(kwargs['COLPREFIX']+'_PC[1]'))

                if "PCALOWERBOUND" in kwargs.keys() \
                    and isinstance(kwargs['PCALOWERBOUND'],float) \
                    and 0<=kwargs['PCALOWERBOUND']<=1.0:
                    _pca_lb=kwargs['PCALOWERBOUND']
                self._portfolio=self._index.filter(F.col('1-PC[0]') >= _pca_lb)\
                                            .rdd.map(lambda row: row.asDict()).collect()
                if len(self._portfolio)<=0:
                    raise RuntimeError("%s filter on 1-PC[0]>=%0.4f returned empty portfolio "+\
                                       "from %d indexed assets" 
                                       % __s_fn_id__,_pca_lb,self._index.count())
                logger.debug("%s Filter 1-PC[0]>=%0.4f returned %d of %d indexed assets",
                             __s_fn_id__,_pca_lb,len(self._portfolio),self._index.count())

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._portfolio

        return select_wrapper

    def calc_pca(func):
        """
        Description:
        Attributes:
            func - inherits pca_wrapper
        Returns:
        """
        @functools.wraps(func)
        def pca_wrapper(self,mcap_date,mcap_value_lb,**kwargs):
            """
            Description:
                wrapper applies a dimension reduction on the indicator measuraments by
                applying PCA to compute the 1st and 2nd components.
            Attributes:
                same as select_top_assets
            Returns:
                self._data (DataFrame) with the PCA feature components
            """

            __s_fn_id__ = "function <pca_wrapper>"
            _idx_types=['adx','rsi','mfi','macd'] # performance indicator list to compute

            try:
                _indic_df = func(self,mcap_date,mcap_value_lb,**kwargs)

                if "INDEXTYPELIST" in kwargs.keys():
                    _idx_types=kwargs['INDEXTYPELIST']

                _pca = clsPCA.get_features(
                    data=_indic_df,
                    feature_cols=_idx_types,
                    **kwargs,
                )
                if _pca is None or _pca.count()<=0:
                    raise RuntimeError("%s did not return any PCA features",__s_fn_id__)
                logger.debug("%s received %d PCA features for %d assets",
                             __s_fn_id__,len(_pca.columns),_pca.count())
                _pca = _pca.withColumn("row_idx",
                                       F.row_number()\
                                       .over(Window.partitionBy(F.lit(0))\
                                             .orderBy(F.monotonically_increasing_id())))
                self._index = clsPCA.data.withColumn("row_idx",
                                                   F.row_number()\
                                               .over(Window.partitionBy(F.lit(0))\
                                                     .orderBy(F.monotonically_increasing_id())))

                self._index = self._index.join(_pca, _pca.row_idx == self._index.row_idx).drop('row_idx')
                if self._index.count()<=0:
                    raise RuntimeError("%s could not merge %d PCA features and %d asssets "+\
                                       "with %d PCA data columns and %d rows",
                                       __s_fn_id__,len(_pca.columns),_pca.count(),
                                       len(clsPCA.data.columns),clsPCA.data.count())
                logger.debug("%s successfuly merged %d PCA features and %d asssets "+\
                                       "with %d PCA data columns and %d rows",
                                       __s_fn_id__,len(_pca.columns),_pca.count(),
                                       len(clsPCA.data.columns),clsPCA.data.count())

                self._data = self._data.unionByName(self._index,allowMissingColumns=True)
                if self._data.count()>0:
                    logger.debug("%s successfuly merged %d index rows and %d columns "+\
                                       "with class property data %d columns and %d rows",
                                       __s_fn_id__,len(self._index.columns),self._index.count(),
                                       len(self._data.columns),self._data.count())

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._index

        return pca_wrapper

    def calc_indicators(func):
        """
        Description:
            declaration of a wraper function to compute the perforamnce indicators
        Attributes:
            func - inherits the wrapper function
        Returns:
            index_wrapper (func)
        """
        @functools.wraps(func)
        def indicators_wrapper(self,mcap_date,mcap_value_lb,**kwargs):
            """
            Description:
            Attributes:
            Returns:
                self._data (DataFrame) with the PCA components
            """

            __s_fn_id__ = "function <indicators_wrapper>"

            _idx_types=['adx','rsi','mfi','macd'] # performance indicator list to compute
            __val_col__="simp_ror" # the column name with the mcap value
            __name_col__='asset_name' # the column name with the mcap asset names
            __date_col__='mcap_date'  # the column name with mcap date for which value was set
            __rf_assets__=['btc']  # of 'risk free' assets to use as the baseline
            __rf_val_col__="simp_ror" # the column name with the mcap value
            __rf_name_col__='asset_name' # the column name with the mcap asset names
            __rf_date_col__='mcap_date'  # the column name with mcap date for which value was set

            try:
                _data, _portf = func(self,mcap_date,mcap_value_lb,**kwargs)

                if "INDEXTYPELIST" in kwargs.keys():
                    _idx_types=kwargs['INDEXTYPELIST']

                _index_df=pd.DataFrame()
                __idx_dict={}
                for asset_portf in _portf:
                    _idx_dict = clsIndx.get_index(
                        portfolio=[asset_portf],
                        asset_eval_date=mcap_date,
                        asset_name_col=__name_col__,
                        asset_val_col =__val_col__,
                        asset_date_col=__date_col__,
                        index_type=_idx_types,
                        risk_free_assets=__rf_assets__,
                        risk_free_name_col=__rf_name_col__,
                        risk_free_val_col=__rf_val_col__,
                        risk_free_date_col=__rf_date_col__,
                        **kwargs,
                    )
                    _idx_dict['mcap_past_pk']=asset_portf['mcapid']
                    _idx_dict['uuid']=asset_portf['uuid']
                    _idx_dict['asset_name']=asset_portf['asset']
                    _idx_dict['mcap_date']=mcap_date
                    _idx_dict['mcap_value']=asset_portf['mcap.value']
                    _index_df=pd.concat([_index_df,pd.DataFrame([_idx_dict])])
                _index_df.insert(0, 'mcap_past_pk', _index_df.pop('mcap_past_pk'))
                _index_df.insert(1, 'uuid', _index_df.pop('uuid'))
                _index_df.insert(2, 'asset_name', _index_df.pop('asset_name'))
                _index_df.insert(3, 'mcap_date', _index_df.pop('mcap_date'))
                _index_df.insert(4, 'mcap_value', _index_df.pop('mcap_value'))

                ''' remove rows with NaN,None values '''
                _drop_idx_df = pd.DataFrame()
                self._index = pd.DataFrame()
                self._index = _index_df.dropna(subset=_idx_types)
                _drop_idx_df = pd.concat([_index_df,self._index]).drop_duplicates(keep=False)
                if _drop_idx_df.shape[0]>0:
                    logger.warning("%s Dropped assets with NaN/None/Null index values: %s",
                                   __s_fn_id__,str(_drop_idx_df['asset_name'].to_list()))

                if self._index.shape[0]<=0:
                    raise RuntimeError("%s resulted in an empty index dataframe, aborting" % __s_fn_id__)
                logger.debug("%s resulted in an index dataframe with %d rows and %d columns",
                             __s_fn_id__,self._index.shape[0],self._index.shape[1])

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._index

        return indicators_wrapper

#     @select_assets
#     @calc_pca
#     @calc_indicators
    def select_top_assets(
        self,
        mcap_date:date=None,
        mcap_value_lb:float=10000.0,
        **kwargs,
    ) -> DataFrame:
        """
        Description:
            For a given date, the procedure selects a limited number of assets from a sorted
            list with mcap value. The indexes, such as ADX, RSI, MFI, SHARP, etc for each asset
            is calculated. Thereafter, a PCA dimensionality reduction is applied on the
            standardized index measures to find the statistically top most assets with trending
            variances above a given threshold; e.g. 0.7 from a sandardized scale of [0,1]
        Attributes:
            mcap_date (date) - date to select the statistically significant set of assets
            mcap_value_lb (float) - lower bound cutoff value to filter assets with mcap_value
            **kwargs -
                TABLENAME (str) - change the table name from the default <warehouse.mcap_past>
                ASSETCOUNT (int)- number of assets to limit in the set; default <23>
        Returns:
            self._data (DataFrame) with the statistically significan top N assets
        Exceptions:
        """

        __s_fn_id__ = "function <select_top_assets>"
        ''' define default values '''
        __def_mcap_lower__ = 10000.0
        _rf_asset_name='bitcoin'   # default risk free asset name
        _rf_val_col = "price_simp_ror" # default risk free ror value column to filter by
        _table_name = "warehouse.mcap_past"
        _asset_count= 23

        try:
            ''' validate input attributes '''
            if not isinstance(mcap_date,date):
                mcap_date=date.today()
            mcap_date=datetime.strftime(mcap_date,'%Y-%m-%dT00:00:00')
            if not isinstance(mcap_value_lb,float) and mcap_value_lb<=0:
                mcap_value_lb=__def_mcap_lower__
            if "TABLENAME" in kwargs.keys() and "".join(kwargs["TABLENAME"].split())!="":
                _table_name=kwargs["TABLENAME"]
            if "ASSETCOUNT" in kwargs.keys() \
                and isinstance(kwargs["ASSETCOUNT"],int) \
                and kwargs["ASSETCOUNT"]>0:
                _asset_count=kwargs["ASSETCOUNT"]
            if "RFASSETNAME" in kwargs.keys() \
                and "".join(kwargs["RFASSETNAME"].split())!="":
                _rf_asset_name=kwargs["RFASSETNAME"]
            if "RFVALCOL" in kwargs.keys() \
                and "".join(kwargs["RFVALCOL"].split())!="":
                _rf_val_col=kwargs["RFVALCOL"]

            ''' read data from database '''
#             _query WHAT WE WANT
#              =f"select * from {_table_name} wmp where wmp.mcap_date = '{mcap_date}' " +\
#                     f"and wmp.mcap_value > {mcap_value_lb} " +\
#                     f"and wmp.{_rf_val_col} >= (select MAX({_rf_val_col}) from {_table_name} " +\
#                     f"where asset_name='{_rf_asset_name}' and mcap_date = '{mcap_date}') " +\
#                     f"order by wmp.mcap_value DESC limit {_asset_count} "
#             _query TESTING ONLY
            _query =f"select * from {_table_name} wmp where wmp.mcap_date = '{mcap_date}' " +\
                    f"and wmp.mcap_value > {mcap_value_lb} " +\
                    f"and wmp.{_rf_val_col} >= 0 " +\
                    f"order by wmp.mcap_value DESC limit {_asset_count} "
            print(_query)
            
#             self._data = clsSDB.read_data_from_table(select=_query, **kwargs)
#             if self._data.count()<=0:
#                 raise RuntimeError("%s query resulted in empty dataframe; aborting." % __s_fn_id__)
#             logger.debug("%s query returned %d dataframe rows",__s_fn_id__,self._data.count())

#             ''' construct portfolio with selected assets '''
#             _assets=self._data.select(F.col('mcap_past_pk'),F.col('uuid'),F.col('mcap_date'),
#                                       F.col('asset_name'),F.col('mcap_value'))\
#                                 .distinct()
#             if _assets.count()<=0:
#                 raise RuntimeError("%s select distinct assets resulted in empty dataframe"
#                                    % __s_fn_id__)
#             self._portfolio=[]
#             for _asset in _assets.collect():
#                 _asset_dict={}
#                 _asset_dict={
#                     "mcapid":_asset[0],
#                     "uuid" : _asset[1],
#                     "date" : datetime.strftime(_asset[2],'%Y-%m-%dT00:00:00'),
#                     "asset": _asset[3],
#                     'mcap.weight': 1.0,
#                     'mcap.value' : float(_asset[4]),
#                 }
#                 self._portfolio.append(_asset_dict)
#                 self._portfolio=sorted(self._portfolio, key=lambda d: d['mcap.value'], reverse=True)
#             if len(self._portfolio)<=0:
#                 raise RuntimeError("%s failed to construct portfolio for %d assets"
#                                    %(__s_fn_id__,_assets.count()))
#             logger.debug("%s constructed portfolio with %d assets",__s_fn_id__,len(self._portfolio))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data,self._portfolio


    ''' Function --- GET REFERENCE INDEX ---

            author: <samana.thetha@gmail.com>
    '''
    @calc_indicators
    def get_reference_index(
        self,
        mcap_date:date=None,
        mcap_value_lb:float=10000.0,
        asset_name:str='btc',
        **kwargs,
    ) -> DataFrame:

        __s_fn_id__ = "function <get_reference_index>"
        ''' define default values '''
        _table_name = "warehouse.mcap_past"

        try:
            ''' validate input attributes '''
            if not isinstance(mcap_date,date):
                mcap_date=date.today()
            mcap_date=datetime.strftime(mcap_date,'%Y-%m-%dT00:00:00')

            if "TABLENAME" in kwargs.keys() and "".join(kwargs["TABLENAME"].split())!="":
                _table_name=kwargs["TABLENAME"]
            
            ''' read data from database '''
            _query =f"select * from {_table_name} wmp where wmp.mcap_date = '{mcap_date}' " +\
                    f"and wmp.asset_name = '{asset_name}' "
            self._data = clsSDB.read_data_from_table(select=_query, **kwargs)

            if self._data.count()<=0:
                raise RuntimeError("%s query resulted in empty dataframe; aborting." % __s_fn_id__)
            logger.debug("%s query returned %d dataframe rows",__s_fn_id__,self._data.count())

            ''' construct portfolio with selected assets '''
            _assets=self._data.select(F.col('mcap_past_pk'),F.col('uuid'),F.col('mcap_date'),
                                      F.col('asset_name'),F.col('mcap_value'))\
                                .distinct()
            if _assets.count()<=0:
                raise RuntimeError("%s select distinct assets resulted in empty dataframe"
                                   % __s_fn_id__)
            self._portfolio=[]
            for _asset in _assets.collect():
                _asset_dict={}
                _asset_dict={
                    "mcapid":_asset[0],
                    "uuid" : _asset[1],
                    "date" : datetime.strftime(_asset[2],'%Y-%m-%dT00:00:00'),
                    "asset": _asset[3],
                    'mcap.weight': 1.0,
                    'mcap.value' : float(_asset[4]),
                }
                self._portfolio.append(_asset_dict)
                self._portfolio=sorted(self._portfolio, key=lambda d: d['mcap.value'], reverse=True)
            if len(self._portfolio)<=0:
                raise RuntimeError("%s failed to construct portfolio for %d assets"
                                   %(__s_fn_id__,_assets.count()))
            logger.debug("%s constructed portfolio with %d assets",__s_fn_id__,len(self._portfolio))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data,self._portfolio


    ''' Function --- MPT PORTFOLIO To DB ---

            author: <samana.thetha@gmail.com>
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
        def dbwrite_wrapper(self,portfolio_data,cols_dict,**kwargs):

            __s_fn_id__ = "function <dbwrite_wrapper>"

            __destin_db_name__ = "tip-daily-mpt"
            __destin_coll_prefix__ = 'mpt'
            _uids = ['date',  # portfolio date
                     'asset']  # asset name
            _def_data_col = 'date'

            try:
                _data = func(self,portfolio_data,cols_dict,**kwargs)

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
                    raise RuntimeError("%s does not exist" % _destin_db)
                ''' set index attributes '''
                if "UIDSLIST" in kwargs.keys():
                    _uids = kwargs["UIDSLIST"]               

                ''' set collection prefix '''
                _mpt_coll_prefix = __destin_coll_prefix__
                if "COLLPREFIX" in kwargs.keys():
                    _mpt_coll_prefix = kwargs["COLLPREFIX"]

                ''' write portfolio to collection '''
                _colls_list=[]
                if not "DATECOLNAME" in kwargs.keys():
                    kwargs['DATECOLNAME']='date'
                _uniq_dates = set([x[kwargs['DATECOLNAME']] for x in _data])
                for _date in _uniq_dates:
                    _mpt_for_date = list(filter(lambda d: d[kwargs['DATECOLNAME']] == _date, _data))
                    _destin_coll = '.'.join([
                        _mpt_coll_prefix,"top"+str(len(_mpt_for_date)),_date.split('T')[0]
                    ])
                    _mpt_coll = clsNoSQL.write_documents(
                        db_name=_destin_db,
                        db_coll=_destin_coll,
                        data=_mpt_for_date,
                        uuid_list=_uids)
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
        portfolio_data:list=[],   # data with portfolio weights, ror, and assets
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
            if len(portfolio_data)==0:
                raise AttributeError("Cannot process an empty mpt list")
            if cols_dict=={}:
                raise AttributeError("Cannot process with an empty cols_dict")

            for idx,data in enumerate(portfolio_data):
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


    ''' Function --- SELECTED ASSETS TO DB ---

            author: <samana.thetha@gmail.com>
    '''
    @dbwrite
    def write_asset_picks_to_db(
        self,
        portfolio_data:list=[],   # data with portfolio weights, ror, and assets
        cols_dict:dict={},   # dict defining the column name mapping
        **kwargs
    ) -> str:
        """
        Description:
        Attribute:
        Returns:
        Exception:
        """

        __s_fn_id__ = "function <write_asset_picks_to_db>"
        
        __def_cols_dict__={
            "mcap_past_pk":"mcap.db.fk",
            "uuid" : "uuid",
            "asset_name": "asset.name",
            "mcap_date" : "asset.mcap.date",
            "mcap_value": "asset.mcap.value",
            "adx" : "index.adx",
            "sharp":"index.sharp",
            "rsi" : "index.rsi",
            "mfi" : "index.mfi",
            "beta": "index.beta",
            "scaled_adx" : "index.scaled.adx",
            "scaled_sharp":"index.scaled.sharp",
            "scaled_rsi" : "index.scaled.rsi",
            "scaled_mfi" : "index.scaled.mfi",
            "scaled_beta": "index.scaled.beta",
            "PC[0]" : "index.pc.0",
            "PC[1]" : "index.pc.1",
            "scaled_PC[0]":'index.scaled.pc.0',
            "scaled_PC[1]":'index.scaled.pc.1',
            "1-PC[0]":'index.scaled.1-pc.0',
            "1-PC[1]":'index.scaled.1-pc.1',
        }
        
        try:
            self.portfolio = portfolio_data
            if not isinstance(cols_dict,dict) or cols_dict=={}:
                cols_dict=__def_cols_dict__
            _port_list=[]
            for _port_dict in self._portfolio:
                for _key,_val in cols_dict.items():
                    _port_dict = {_val if k==_key else k:v for k,v in _port_dict.items()}
                if "DATECOLNAME" in kwargs.keys():
                    _port_dict[kwargs['DATECOLNAME']]=datetime.strftime(
                        _port_dict[kwargs['DATECOLNAME']],
                        '%Y-%m-%dT00:00:00')
                _port_dict["audit.mod.by"]=os.environ.get('USERNAME').upper()
                _port_dict["audit.mod.dt"]=datetime.strftime(datetime.now(),'%Y-%m-%dT%H:%M:%S')
                _port_dict["audit.mod.proc"]="-".join([self.__name__,__s_fn_id__])
                _port_list.append(_port_dict)

            if len(_port_list)<=0:
                raise ValueError("Empty mpt list, not data to write")
            self._portfolio=_port_list
            logger.debug("Created database collection ready dict list with %d documents"
                      ,len(self._portfolio))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio
