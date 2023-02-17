#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "performIndex"
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
    from itertools import chain
    ''' function specific python packages '''
    import pandas as pd
    import numpy as np
    from datetime import datetime, date, timedelta

    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))


'''
    CLASS spefic to preparing the data based on Ration Of Returns (ROR).
    
'''

class Portfolio():

    ''' --- INIT CLASS ---

            author: <samana.thetha@gmail.com>
    '''
    def __init__(self, desc : str="mcap risk, strength, & adjusted price index calculation", **kwargs):
        """
        Description:
            Intantiate -
            * the class and class properties
            * associated and extended utils and mining etp classes.
            * logger for module/package
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
        self._idxValue=None
        self._idxType =None
        self._idxTypeList =[
            'RSI',   # Relative Strength Index
            'DMI',   # Directional Movement Indicator
            'ADX',   # Adjusted Directional Index
            'SORTIONO', # Sortino ratio 
            'SHARP',    # Sharp ratio of all positive
        ] 
        
        global pkgConf
        global logger
        global clsNoSQL
        global clsSCNR
        global clsStats
        global clsMPT

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

#             ''' import spark database work load utils to read and write data '''
#             from utils.modules.etl.load import sparkDBwls as sparkDB
#             clsSDB = sparkDB.SQLWorkLoads(desc=self.__desc__)
#             ''' import spark clean-n-rich work load utils to transform the data '''
            from utils.modules.etl.transform import sparkCleanNRich as sparkCNR
            clsSCNR = sparkCNR.Transformer(desc=self.__desc__)
            ''' import spark mongo work load utils to read and write data '''
            from utils.modules.etl.load import sparkNoSQLwls as nosql
            clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)
            ''' import spark time-series work load utils for rolling mean/variance computations '''
            from utils.modules.ml.timeseries import rollingstats as stats
            clsStats = stats.RollingStats(desc=self.__desc__)
            from mining.modules.assets.etp import dailyTopN as topN
            clsMPT =topN.WeightedPortfolio(desc=self.__desc__)

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

    ''' --- DATA CLASS PROPERTY---

            author: <samana.thetha@gmail.com>
    '''
    @property
    def data(self) -> DataFrame:
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
    def data(self,data) -> DataFrame:

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

    ''' Function --- PORTFOLIO CLASS PROPERTIES ---

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
            if self._portfolio is None:
                self._portfolio = self.get_portfolio(
                    db_name="tip_daily_mpt",
                    db_coll="",
                    mpt_date=date.today(),
                )
                logger.warning("Portfolio is Non-type; retrieved portfolio data for %s "+\
                               "of dtype %s with %d records"
                               ,str(date.today()),type(self._portfolio),len(self._portfolio))

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
            self._portfolio = portfolio
            logger.debug("Portfolio class property set with %s of length %d"
                         %(type(self._portfolio),len(self._portfolio)))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio


    ''' --- GET PORTFOLIO ---

            author: <samana.thetha@gmail.com>
    '''

    def get_portfolio(
        self,
        db_name:str="",
        db_coll:list=[],
        coll_date:date=date.today(),
        **kwargs,
    ) -> list:
        """
        Description:
            reads the portfolio from the MongoDB database. If collection is given
            then read directly from the collection; else read all collections with
            a specific date postfix.
        Attributes:
            db_name (str) specifies the name of the database; else use default value
            db_coll (str) specifies the collection name; else default to date postfix
            mpt_date (date) specifies the date of the portfolio
        Returns (list) self._portfolio is a list of dictionaries
        """

        __s_fn_id__ = "function <get_portfolio>"

        __def_db_type__ ='MongoDB'
        __def_db_name__ = "tip-daily-mpt"
#         __def_coll_pref__ = "mpt"
        
        _mpts = []
        _db_coll_list = []

        try:
            ''' confirm and set database qualifiers '''
            if "".join(db_name.strip())=="":
                _db_name = __def_db_name__
            else:
                _db_name = db_name
            if "DBTYPE" not in kwargs.keys():
                kwargs['DBTYPE']=__def_db_type__
            if "DBAUTHSOURCE" not in kwargs.keys():
                kwargs['DBAUTHSOURCE']=_db_name

            ''' make the connection '''
            clsNoSQL.connect=kwargs
            ''' confirm database exists '''
            if not _db_name in clsNoSQL.connect.list_database_names():
                raise RuntimeError("%s does not exist in %s",_db_name,clsNoSQL.dbType)
            clsNoSQL.dbName=_db_name
            _all_colls = clsNoSQL.collections

            ''' confirm and set collection list '''
            if len(db_coll)>0:
                ''' check if listed collections exist '''
                for _coll in db_coll:
                    if _coll not in _all_colls:
                        raise AttributeError("Invalid collection name: %s was not found in %s" 
                                             % (_coll,_all_colls))
            elif coll_date <= date.today():
                clsNoSQL.collections={"HASINNAME":str(coll_date)}
                db_coll=clsNoSQL.collections
                ''' get all collections containing date postfix '''
            else:
                raise AttributeError("Either a db_coll list or mpt_date must be specified")

            ''' read all collections into a list '''
            _mpts = clsNoSQL.read_documents(
                as_type="LIST",
                db_name="",
                db_coll=db_coll,
                doc_find={},
                **kwargs)
            if len(_mpts)>0:
                self._portfolio = _mpts
                logger.info("Loaded %d portfolios",len(self._portfolio))
            else:
                raise ValueError("No portfolios found for collections: %s in %s %s"
                                 % (db_coll,clsNoSQL.dbType,clsNoSQL.dbName))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio


    ''' --- INDEX CLASS PROPERTIES ---

            author: <samana.thetha@gmail.com>
    '''
    @property
    def idxValue(self) -> float:
        """
        Description:
            index @property functions confirms the index value base on the type
        Attributes:
                
        Returns (float) self._idxValue
        """

        __s_fn_id__ = "function <@property idxValue>"

        try:
            if self._idxValue is None:
                raise ValueError("NoneType index; cannot be used in any %s computations")
            if self._idxValue<0 or self._idxValue>1.0:
                raise ValueError("Invalid index value %d; it must be between 0 and 1.0"
                                 % self._idxValue )

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._idxValue

    @idxValue.setter
    def idxValue(self,index_value) -> float:

        __s_fn_id__ = "function <@setter idxValue>"

        try:
            if index_value is None:
                raise AttributeError("Invalid index_value, must be a valid float")
            if float(index_value)>=0 and float(index_value)<=1.0:
                self._idxValue = float(index_value)
                logger.debug("Valid index value %d set for class property", self._idxValue)
            else:
                self._idxValue = index_value
                logger.warning("Unacceptable index value %d but class property was set"
                               ,self._idxValue)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._idxValue

    @property
    def idxType(self) -> str:
        """
        Description:
            idxType @property getter functions to retrieve the string value
        Attributes:  
        Returns (str) self._idxType
        """

        __s_fn_id__ = "function <@property idxType>"

        try:
            if self._idxType not in self.idxTypeList:
                raise AttributeError("Invalid index type; must be one of %s"
                                 %self.idxTypeList)
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._idxType.upper()

    @idxType.setter
    def idxType(self,index_type) -> DataFrame:

        __s_fn_id__ = "function <@setter idxType>"

        try:
            if index_type.upper() not in self.idxTypeList:
                raise AttributeError("Invalid index type %s; must be one of %s"
                                 %(index_type.upper(),self.idxTypeList))

            self._idxType=index_type.upper()

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._idxType

#     ''' --- GET PORTFOLIO ---

#             author: <samana.thetha@gmail.com>
#     '''

#     def get_portfolio(
#         self,
#         db_name:str="",
#         db_coll:list=[],
#         coll_date:date=date.today(),
#         **kwargs,
#     ) -> list:
#         """
#         Description:
#             reads the portfolio from the MongoDB database. If collection is given
#             then read directly from the collection; else read all collections with
#             a specific date postfix.
#         Attributes:
#             db_name (str) specifies the name of the database; else use default value
#             db_coll (str) specifies the collection name; else default to date postfix
#             mpt_date (date) specifies the date of the portfolio
#         Returns (list) self._portfolio is a list of dictionaries
#         """

#         __s_fn_id__ = "function <get_portfolio>"

#         __def_db_type__ ='MongoDB'
#         __def_db_name__ = "tip-daily-mpt"
# #         __def_coll_pref__ = "mpt"
        
#         _mpts = []
#         _db_coll_list = []

#         try:
#             ''' confirm and set database qualifiers '''
#             if "".join(db_name.strip())=="":
#                 _db_name = __def_db_name__
#             else:
#                 _db_name = db_name
#             if "DBTYPE" not in kwargs.keys():
#                 kwargs['DBTYPE']=__def_db_type__
#             if "DBAUTHSOURCE" not in kwargs.keys():
#                 kwargs['DBAUTHSOURCE']=_db_name

#             ''' make the connection '''
#             clsNoSQL.connect=kwargs
#             ''' confirm database exists '''
#             if not _db_name in clsNoSQL.connect.list_database_names():
#                 raise RuntimeError("%s does not exist in %s",_db_name,clsNoSQL.dbType)
#             clsNoSQL.dbName=_db_name
#             _all_colls = clsNoSQL.collections

#             ''' confirm and set collection list '''
#             if len(db_coll)>0:
#                 ''' check if listed collections exist '''
#                 for _coll in db_coll:
#                     if _coll not in _all_colls:
#                         raise AttributeError("Invalid collection name: %s was not found in %s" 
#                                              % (_coll,_all_colls))
#             elif coll_date <= date.today():
#                 clsNoSQL.collections={"HASINNAME":str(coll_date)}
#                 db_coll=clsNoSQL.collections
#                 ''' get all collections containing date postfix '''
#             else:
#                 raise AttributeError("Either a db_coll list or mpt_date must be specified")

#             ''' read all collections into a list '''
#             _mpts = clsNoSQL.read_documents(
#                 as_type="LIST",
#                 db_name="",
#                 db_coll=db_coll,
#                 doc_find={},
#                 **kwargs)
#             if len(_mpts)>0:
#                 self._portfolio = _mpts
#                 logger.info("Loaded %d portfolios",len(self._portfolio))
#             else:
#                 raise ValueError("No portfolios found for collections: %s in %s %s"
#                                  % (db_coll,clsNoSQL.dbType,clsNoSQL.dbName))

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._portfolio


    ''' --- CAL PERFORMANCE INDECIES ---

            author: <samana.thetha@gmail.com>
    '''

    def index(func):

        @functools.wraps(func)
        def index_wrapper(self,portfolio,date,index_type,**kwargs):
            """
            Description:
                wrapper function to compute the RSI for the given data
            Attributes:
                same as get_rsi
            Returns:
                self._idxValue
            """
            
            __s_fn_id__ = "function <index_wrapper>"
            _pos_sdf=None
            _neg_sdf=None
            _weights_sdf=None

            try:
                _ror_data = func(self,portfolio,date,index_type,**kwargs)

                if self._idxType == 'RSI':
                    _weights_df = pd.DataFrame(self.portfolio)
#                     _weights_arr = _weights_sdf['mcap.weight'].to_numpy()
                    _rsi=-1000.0
                    _rsi=Portfolio.calc_rsi(
                        ror_data=_ror_data,
                        val_col=kwargs["LOGCOLNAME"],
                        part_col=kwargs["PARTCOLNAME"],
                        weights_sdf=_weights_df,
#                         weights=_weights_arr,
                        **kwargs,
                    )
                    if _rsi <= 1.0 and _rsi >= 0:
                        self._idxValue = _rsi
                    else:
                        raise ValueError("%s Something went wrong computing %s"
                                         %(__s_fn_id__,self._idxType))

                elif self._idxType == 'DMI':
#                     _weights_sdf = pd.DataFrame(self.portfolio)
#                     _weights_arr = _weights_sdf['mcap.weight'].to_numpy()
                    _dmi_sdf=None
                    _dmi_sdf = Portfolio.calc_dmi(
                        ror_data=_ror_data,
                        val_col=kwargs["LOGCOLNAME"],
                        part_col=kwargs["PARTCOLNAME"],
#                         weights_list=self.portfolio,
                        **kwargs,
                    )
                    self._data=_dmi_sdf
                    self._idxValue = None

                elif self._idxType == 'ADX':
                    _weights_sdf = pd.DataFrame(self.portfolio)
#                     _weights_arr = _weights_sdf['mcap.weight'].to_numpy()
                    _adx=None
                    _adx = Portfolio.calc_adx(
                        ror_data=_ror_data,
                        val_col=kwargs["LOGCOLNAME"],
                        part_col=kwargs["PARTCOLNAME"],
                        weights_list=self.portfolio,
                        **kwargs,
                    )
#                     self._data=_adx_sdf
                    self._idxValue = _adx
                else:
                    raise AttributeError("Unrecognized index type %s or something was wrong"
                                         % self._idxType)

                logger.info("%s computed index value for %s = %0.4f"
                            % (__s_fn_id__,self._idxType,self._idxValue))

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._idxValue
        
        return index_wrapper

    def ror(func):
        
        @functools.wraps(func)
        def ror_wrapper(self,portfolio,date,index_type,**kwargs):
            """
            Description:
                wrapper function to compute the ROR for the given data
            Attributes:
                same as get_rsi
            Returns:
                self._data (DataFrame) with additional ror columns
            """
            
            __s_fn_id__ = "function <ror_wrapper>"
#             _pos_trend=None
#             _neg_trend=None

            try:
                _mcap_data = func(self,portfolio,date,index_type,**kwargs)

                if "VALCOLNAME" not in kwargs.keys():
                    kwargs["VALCOLNAME"] = 'mcap_value'
                if "PARTCOLNAME" not in kwargs.keys():
                    kwargs["PARTCOLNAME"] = 'asset_name'
                if "PREVALCOLNAME" not in kwargs.keys(): 
                    kwargs["PREVALCOLNAME"] = 'mcap_prev_val'
                if "DIFFCOLNAME" not in kwargs.keys():
                    kwargs["DIFFCOLNAME"] = 'mcap_diff'
                if "LOGCOLNAME" not in kwargs.keys():
                    kwargs["LOGCOLNAME"] = 'log_ror'

                _mcap_log_ror, _log_col = clsMPT.get_log_ror(
                    data=_mcap_data,
                    num_col_name=kwargs["VALCOLNAME"],
                    part_column =kwargs["PARTCOLNAME"],
                    **kwargs,
                )
                ''' drop the null values '''
                _mcap_log_ror.na.drop(subset=[kwargs["LOGCOLNAME"]])

                if _mcap_log_ror.count() > 0:
                    self._data = _mcap_log_ror
                    logger.debug("computed ROR for column: mcap_value added %s column with %d rows"
                                 ,_log_col,self._data.count())

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._data
        
        return ror_wrapper
    
    @index
    @ror
    def get_index(
#     def get_rsi(
        self,
        portfolio:list=[],
        date:date=date.today(),
        index_type:str='RSI',
        **kwargs,
    ) -> DataFrame:
        """
        Description:
            Calculates the Relative Strength Index (RSI). It is a momentum estimateion,
            which is the speed and magnitude of the price change. The index will 
            provide a value between 0 and 1 to decide whether it is overvalued (>= 0.7)
            or under valued (<0.7). The returned data will provide a time series
            of the oscilator. It also indicates the trend reversal (uptrend or downtrend).
            Thereby, indicating when to rebalance; i.e. buy/sell.
        Attributes:
            portfolio (list) containing the a dictionary of the 
                _id (ObjectId)  - unique identifier
                date (timestamp)- of the asset evaluation date 
                asset (str)     - defining the name of the asset
                weight (flaot64)- propotion of the asset to the portfolio
                ror (float64)   - logarithmic ratio of return, relative to previous date
                value (float64) - the mcap value of the asset for that date
            kwargs:
                DATETIMEATTR (str)- defines the column name to use in SMA
                WINLENGTH (str)   - defines the window length; typically 14 days
                WINUNIT (str)     - defines the window length is DAY, WEEK, MINUTE, HOUR
                RSITYPE (str)     - STEPONE or STEPTWO first or second step calculation
        returns:
            self._data (DataFrame) comprising the T-day timeseries that defines the RSI
        """

        __s_fn_id__ = "function <get_index>"

        __def_table_name__ = ''
        __def_mcap_val_limit__ = 10000
        __def_dt_attr__ = "mcap_date"
        __def_win_len__ = 14
        __def_len_unit__= "DAY"

        try:
            ''' confirm portfolio data '''
            if len(portfolio)<=0:
                raise AttributeError("Cannot use an empty portfolio to compute RSI")
            _dates_list = [x['date'].split('T')[0] for x in portfolio]
            if str(date) not in _dates_list:
                raise AttributeError("Invalid date %s not in any portfolio dates %s"
                                     % (str(date),str(_dates_list)))
            if len(portfolio)<=0:
                raise AttributeError("Invalid portfolio: %s"
                                     %(str(portfolio)))
            self.portfolio=portfolio
            if index_type.upper() not in self._idxTypeList:
                raise AttributeError("Invalid index_type: %s; must be in %s"
                                     %(index_type,str(self._idxTypeList)))
            self._idxType = index_type.upper()

            if "MCAPLIMIT" not in kwargs.keys():
                kwargs["MCAPLIMIT"]=__def_mcap_val_limit__
            ''' estanlish the window length and unit '''
            if "DATETIMEATTR" not in kwargs.keys():
                kwargs["DATETIMEATTR"]=__def_dt_attr__
            if "WINLENGTH" not in kwargs.keys():
                kwargs["WINLENGTH"]=__def_win_len__
            if "WINUNIT" not in kwargs.keys():
                kwargs["WINUNIT"]=__def_len_unit__
            if "RSITYPE" not in kwargs.keys() or \
                kwargs["RSITYPE"].upper() not in ['STEPONE','STEPTWO']:
                kwargs["RSITYPE"] = 'STEPTWO'

            ''' retrieve mcap log ror for each asset in portfolio for the window length '''
            _assets_in = ','.join(["'{}'".format(x['asset']) for x in portfolio])
            _to_date = str(date)
            ''' get for window length + 1 day '''
            _from_date = str(date - timedelta(days=kwargs["WINLENGTH"]))
            _mcap_upper = kwargs["MCAPLIMIT"]
            _query = "select * from warehouse.mcap_past "+\
                    f"where mcap_date >= '{_from_date}' and mcap_date <= '{_to_date}' "+\
                    f"and asset_name in ({_assets_in}) "+\
                    f"and mcap_value > {_mcap_upper}"

            mcap_sdf = clsMPT.read_n_clean_mcap(query=_query,**kwargs)
            if mcap_sdf.count() > 0:
                self._data = mcap_sdf
                logger.debug("%s loaded %d rows and %d columns for %s computation"
                             ,__s_fn_id__,self._data.count()
                             ,len(self._data.columns),index_type.upper())
            else:
                raise ValueError("No data portfolio asset data received for query %s: " % _query)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' --- RELATIVE STRENGTH INDEX ---

            TODO: (i) verify weights shape, if not same as dataframe create all 1.0 array
                 (ii) if asset_name and value (rorO column not specified in kwargs; try
                    to find the first column with float values
                (iii) do both step 1 and step 2 RSI caculations

            author: <samana.thetha@gmail.com>
    '''

    @staticmethod
    def calc_rsi(
        ror_data:DataFrame=None,
        val_col:str="log_ror",
        part_col:str="asset_name",
#         weights:np.ndarray=None,
        weights_sdf:pd.DataFrame=None,
        **kwargs,
    ) -> float:

        __s_fn_id__ = "@staticmethod <calc_rsi>"

        try:
            if ror_data.count() <=0:
                raise AttributeError("Cannot compute RSI with empty dataframe")
            if "".join(val_col.strip())=="" or \
                val_col not in ror_data.columns or \
                ror_data.select(F.col(val_col)).dtypes[0][1]=='string':
                raise AttributeError("A valid numeric column from %s required" % ror_data.dtypes)

            _pos_sdf = ror_data.groupBy(kwargs['PARTCOLNAME'])\
                            .agg(\
                                 F.sum(F.when(F.col(kwargs["LOGCOLNAME"])>0,\
                                              F.col(kwargs["LOGCOLNAME"])))\
                                 .alias('pos_col')).sort(kwargs['PARTCOLNAME'])

            _neg_sdf = ror_data.groupBy(kwargs['PARTCOLNAME'])\
                            .agg(\
                                 F.sum(F.when(F.col(kwargs["LOGCOLNAME"])<=0,\
                                              F.col(kwargs["LOGCOLNAME"])))\
                                 .alias('neg_col')).sort(kwargs['PARTCOLNAME'])

            _pos_arr=_pos_sdf.toPandas()['pos_col'].to_numpy()
            _neg_arr=_neg_sdf.toPandas()['neg_col'].to_numpy()

#             _pos_arr=ror_data.toPandas()['pos_col'].to_numpy()
#             _neg_arr=ror_data.toPandas()['neg_col'].to_numpy()
            _weights_arr = weights_sdf['mcap.weight'].to_numpy()
            print(_pos_arr)
            print(_neg_arr)
            print(_weights_arr)
            _pos = np.matmul(_pos_arr,_weights_arr)
            _neg = np.matmul(_neg_arr,_weights_arr)
            print(type(_pos),type(_neg),_pos,_neg)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return float(1/(1+abs(_neg/_pos)))
#         return 1/(1+(F.abs(_neg/_pos)))


    ''' --- DIRECTIONAL MOVEMENT INDICATOR ---
    
            author: <samana.thetha@gmail.com>
    '''
#     @staticmethod
    def get_dmi_data(
        self,
        dmi_sdf_:DataFrame=None,
        val_col:str="log_ror",
        part_col:str="asset_name",
#         weights_list:list=[],
        **kwargs,
    ) -> DataFrame:
        """
        Description:
            Directional Movement Indicator (DMI) helps assess whether a trade should be
            taken long or short, or if a trade should be taken at all. It is derived from
            the Directional Index (DI).
            The @staticmethod computes all the +/- Direction Index (DI) 14-day rolling sum, 
            average, and smooth values. Then partitions by the asset_name to multiply the DI 
            by the asset weight to compute a weighted ADX for the entire portfolio.
        Attributes:
            ror_data (DataFrame) the rate of returns dataframe
            val_col (str) defines the numeric column to use; default value = log_ror
            part_col (str) defines the partition column; i.e categories; default value = asset_name
            weights_list (list) contains dictionaries of asset_name and associated weight,
            **kwargs not used
        Returns:
            adx (float) with the single adx number between [0,1]
            adx_sdf (DataFrame) augments the all the DI avg, sum, smooth, adx columns to the
                original DataFrame
        """

        __s_fn_id__ = "@staticmethod <calc_adx>"

        try:
            ''' validate the attribues '''
            if dmi_sdf_.count() <=0:
                raise AttributeError("Cannot compute RSI with empty dataframe")
#             dmi_sdf_=ror_data
            if "".join(val_col.strip())=="" or \
                val_col not in dmi_sdf_.columns or \
                dmi_sdf_.select(F.col(val_col)).dtypes[0][1]=='string':
                raise AttributeError("A valid numeric column from %s required" % dmi_sdf_.dtypes)
            if "".join(part_col.strip())=="":
                raise AttributeError("Invalid partition column, specify one from %s" % dmi_sdf_.dtypes)
#             _asset_count = dmi_sdf_.select(F.col(part_col)).distinct().count()
#             if len(weights_list) != _asset_count:
#                 raise AttributeError("Dimension mismatching weights %d "+ \
#                                      "to that of distinct asset count %d"
#                                      % (len(weights_list),r_asset_count))

            ''' compute the DMI sum, avg, smooth, and DI column values '''
            dmi_sdf_ = dmi_sdf_.withColumn("+DM",
                                           F.when(F.col(val_col) > 0,
                                                  F.abs(F.col(val_col)))
                                           .otherwise(0))
            dmi_sdf_ = dmi_sdf_.withColumn("-DM",
                                           F.when(F.col(val_col) <= 0,
                                                  F.abs(F.col(val_col)))
                                           .otherwise(0))
            ''' Smoothed values '''
            kwargs['RESULTCOL']='sm_sum_+DM'
            dmi_sdf_ = clsStats.simple_moving_stats(
                num_col="+DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="sum",
                data=dmi_sdf_,
                **kwargs,
            )
            kwargs['RESULTCOL']='sm_avg_+DM'
            dmi_sdf_ = clsStats.simple_moving_stats(
                num_col="+DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="avg",
                data=dmi_sdf_,
                **kwargs,
            )
            kwargs['RESULTCOL']='sm_sum_-DM'
            dmi_sdf_ = clsStats.simple_moving_stats(
                num_col="-DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="sum",
                data=dmi_sdf_,
                **kwargs,
            )
            kwargs['RESULTCOL']='sm_avg_-DM'
            dmi_sdf_ = clsStats.simple_moving_stats(
                num_col="-DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="avg",
                data=dmi_sdf_,
                **kwargs,
            )

            ''' shift +DM & -DM column by period=1 '''
            _win = Window.partitionBy(F.col(part_col)).orderBy(F.col("mcap_date").cast('long'))
            dmi_sdf_ = dmi_sdf_.withColumn('shift_+DM',
                                           F.lag(F.col('+DM'),offset=1,default=0)
                                           .over(_win))
            dmi_sdf_ = dmi_sdf_.withColumn('shift_-DM',
                                           F.lag(F.col('-DM'),offset=1,default=0)
                                           .over(_win))
            ''' smoothen the data '''
            dmi_sdf_ = dmi_sdf_.withColumn('smooth_+DM',
                                           (F.col('sm_sum_+DM')
                                            -F.col('sm_avg_+DM')
                                            +F.col('shift_+DM')))
            dmi_sdf_ = dmi_sdf_.withColumn('smooth_-DM',
                                           (F.col('sm_sum_-DM')
                                            -F.col('sm_avg_-DM')
                                            +F.col('shift_-DM')))
            ''' Final Calculations of DI+ & DI- '''
            dmi_sdf_ = dmi_sdf_.withColumn('+DI',(F.col('sm_avg_+DM')/F.col('smooth_+DM')))
            dmi_sdf_ = dmi_sdf_.withColumn('-DI',(F.col('sm_avg_-DM')/F.col('smooth_-DM')))

#             ''' augment asset specific weights to the dataframe '''
#             obj_map = {}
#             for _doc in weights_list:
#                 obj_map[_doc['asset']]=_doc['mcap.weight']
#             mapping_expr = F.create_map([F.lit(x) for x in chain(*obj_map.items())])
#             df1 = _dm_stat_sdf.filter(F.col(part_col).isNull())\
#                                 .withColumn('weight', F.lit(None))
#             df2 = _dm_stat_sdf.filter(F.col(part_col).isNotNull())\
#                                 .withColumn('weight',\
#                                             F.when(F.col(part_col).isNotNull(),\
#                                                  mapping_expr[F.col(part_col)]
#                                                 ))
#             adx_sdf_ = df1.unionAll(df2)
#             adx_sdf_ = adx_sdf_.withColumn('weighted_+DI',(F.col('+DI')*F.col('weight')))
#             adx_sdf_ = adx_sdf_.withColumn('weighted_-DI',(F.col('-DI')*F.col('weight')))
#             adx_sdf_ = adx_sdf_.withColumn('ADX',(F.col('weighted_-DI')-F.col('weighted_+DI'))/
#                                         (F.col('weighted_-DI')+F.col('weighted_+DI')))
#             adx_ = adx_sdf_.select(F.sum(F.col('ADX'))).alias("adx_val")

            if dmi_sdf_.count()>0:
                self._data = dmi_sdf_

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data  #adx_.collect()[0][0],adx_sdf_


    ''' --- AVERAGE DIRECTIONAL INDEX ---
    
            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def calc_adx(
        dmi_data:DataFrame=None,
        val_col:str="log_ror",
        part_col:str="asset_name",
        weights_list:list=[],
        **kwargs,
    ) -> float:
        """
        Description:
            The Average Diractional Index (ADX) commonly includes three separate lines.
            These are used to help assess whether a trade should be taken long or short,
            or if a trade should be taken at all. The method is also refered to as the 
            Directional Movement Indicator (DMI) derived from the Directional Index (DI)
            The @staticmethod computes all the +/- Direction Index (DI) 14-day rolling sum, 
            average, and smooth values. Then partitions by the asset_name to multiply the DI 
            by the asset weight to compute a weighted ADX for the entire portfolio.
        Attributes:
            dmi_data (DataFrame) the directional movement indicator dataframe
            val_col (str) defines the numeric column to use; default value = log_ror
            part_col (str) defines the partition column; i.e categories; default value = asset_name
            weights_list (list) contains dictionaries of asset_name and associated weight,
            **kwargs not used
        Returns:
            adx (float) with the single adx number between [0,1]
        """

        __s_fn_id__ = "@staticmethod <calc_adx>"
        adx_ = None
        adx_sdf_=None
        obj_map = {}

        try:
            if dmi_data.count() <=0:
                raise AttributeError("Cannot compute RSI with empty dataframe")
            if "".join(val_col.strip())=="" or \
                val_col not in ror_data.columns or \
                dmi_data.select(F.col(val_col)).dtypes[0][1]=='string':
                raise AttributeError("A valid numeric column from %s required" % dmi_data.dtypes)
            if "".join(part_col.strip())=="":
                raise AttributeError("Invalid partition column, specify one from %s" % dmi_data.dtypes)
            _asset_count = dmi_data.select(F.col(part_col)).distinct().count()
            if len(weights_list) != _asset_count:
                raise AttributeError("Dimension mismatching weights %d "+ \
                                     "to that of distinct asset count %d"
                                     % (len(weights_list),r_asset_count))


            ''' augment asset specific weights to the dataframe '''
            for _doc in weights_list:
                obj_map[_doc['asset']]=_doc['mcap.weight']
            mapping_expr = F.create_map([F.lit(x) for x in chain(*obj_map.items())])
            df1 = dmi_data.filter(F.col(part_col).isNull())\
                            .withColumn('weight', F.lit(None))
            df2 = dmi_data.filter(F.col(part_col).isNotNull())\
                            .withColumn('weight',\
                                        F.when(F.col(part_col).isNotNull(),\
                                               mapping_expr[F.col(part_col)]
                                              ))
            adx_sdf_ = df1.unionAll(df2)
            adx_sdf_ = adx_sdf_.withColumn('weighted_+DI',(F.col('+DI')*F.col('weight')))
            adx_sdf_ = adx_sdf_.withColumn('weighted_-DI',(F.col('-DI')*F.col('weight')))
            adx_sdf_ = adx_sdf_.withColumn('ADX',(F.col('weighted_-DI')-F.col('weighted_+DI'))/
                                        (F.col('weighted_-DI')+F.col('weighted_+DI')))
            adx_ = adx_sdf_.select(F.sum(F.col('ADX'))).alias("adx_val")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return adx_.collect()[0][0]


    ''' --- DIVERGENCE ---
    
            TODO: replace pandas with pyspark

            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def calc_divergence(
        ror_data:DataFrame=None,
        val_col:str="log_ror",
        part_col:str="asset_name",
        weights_list:list=[],
        **kwargs,
    ) -> float:
        """
        Description:
            Divergence is when the DMI and price disagree, or do not confirm one another.
            An example is when price makes a new high, but the +DMI does not. Divergence
            is generally a warning to manage risk because it signals a change of swing 
            strength and commonly precedes a retracement or reversal.
        Attributes:
            ror_data (DataFrame) the rate of returns dataframe
            val_col (str) defines the numeric column to use; default value = log_ror
            part_col (str) defines the partition column; i.e categories; default value = asset_name
            weights_list (list) contains dictionaries of asset_name and associated weight,
            **kwargs not used
        Returns:
            adx (float) with the single adx number between [0,1]
            adx_sdf (DataFrame) augments the all the DI avg, sum, smooth, adx columns to the
                original DataFrame
        """

        __s_fn_id__ = "@staticmethod <calc_divergence>"

        try:
            if ror_data.count() <=0:
                raise AttributeError("Cannot compute RSI with empty dataframe")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None
