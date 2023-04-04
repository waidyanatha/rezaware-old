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
            'BETA',  # Beta volatility
            'ALPHA', # Alpha performance relative to benchmark
            'SORTINO', # Sortino ratio 
            'SHARP',    # Sharp ratio of all positive
            'MFI',   # Money Flow Index
        ] 
        
        global pkgConf
        global logger
        global clsSDB
        global clsNoSQL
#         global clsSCNR
        global clsStats
#         global clsMPT

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
            from utils.modules.etl.load import sparkDBwls as spark
            clsSDB =spark.SQLWorkLoads(desc=self.__desc__)
            ''' import spark mongo work load utils to read and write data '''
            from utils.modules.etl.load import sparkNoSQLwls as nosql
            clsNoSQL = nosql.NoSQLWorkLoads(desc=self.__desc__)
#             from utils.modules.etl.transform import sparkCleanNRich as sparkCNR
#             clsSCNR = sparkCNR.Transformer(desc=self.__desc__)
            ''' import spark time-series work load utils for rolling mean/variance computations '''
            from utils.modules.ml.timeseries import rollingstats as stats
            clsStats = stats.RollingStats(desc=self.__desc__)
#             from mining.modules.assets.etp import dailyTopN as topN
#             clsMPT =topN.WeightedPortfolio(desc=self.__desc__)

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
                self._portfolio = self.read_portfolio(
                    db_name="tip_daily_mpt",
                    db_coll="",
                    mpt_date=date.today(),
                )
                logger.warning("%s Portfolio is Non-type; retrieved portfolio data for %s "+\
                               "of dtype %s with %d records",
                               __s_fn_id__,str(date.today()),
                               type(self._portfolio),len(self._portfolio))

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
            logger.debug("%s Portfolio class property set with %s of length %d"
                         %(__s_fn_id__,type(self._portfolio),len(self._portfolio)))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio


    @property
    def movement(self) -> list:
        """
        Description:
            movement @property function
        Attributes:
            data in @setter will instantiate self._movement 
        Returns (list) self._movement
        """

        __s_fn_id__ = "function <@property movement>"

        try:
            if self._movement is None:
                logger.warning("%s Movement class property is Non-type",__s_fn_id__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._movement

    @movement.setter
    def movement(self,movement:list=[]) -> list:

        __s_fn_id__ = "function <@setter movement>"

        try:
            if len(movement)<=0:
                raise AttributeError("Invalid movement attribute, must be a non-empy list")
            self._movement = movement
            logger.debug("%s Movement class property set with %s of length %d"
                         %(__s_fn_id__,type(self._portfolio),len(self._portfolio)))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._movement


    ''' --- READ PORTFOLIO ---

            author: <samana.thetha@gmail.com>
    '''

    def read_portfolio(
        self,
        db_name:str="",   # specifies the name of the database; else use default value
        db_coll:list=[],  # specifies the collection name; else default to date postfix
        coll_date:date=date.today(),   # specifies the date of the portfolio
        **kwargs,
    ) -> list:
        """
        Description:
            reads the portfolio from the MongoDB database. If collection is given
            then read directly from the collection; else read all collections with
            a specific date postfix.
            The function is associated with the self.portfolio class property required
            for all Index computations; see function: get_index()
        Attributes:
            db_name (str) specifies the name of the database; else use default value
            db_coll (str) specifies the collection name; else default to date postfix
            mpt_date (date) specifies the date of the portfolio
            **kwargs
        Returns (list) self._portfolio is a list of dictionaries
        Exceptions:
            if database collection list returns None log eventand raise a RuntimeError
        """

        __s_fn_id__ = "function <read_portfolio>"

        __def_db_type__ ='MongoDB'
        __def_db_name__ = "tip-daily-mpt"
#         __def_coll_pref__ = "mpt"
        
        _mpts = []
#         _db_coll_list = []

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

            ''' confirm exists and set collection list '''
            if len(db_coll)>0:
                ''' check if listed collections exist '''
                ''' TODO use set intersection '''
                for _coll in db_coll:
                    if _coll not in _all_colls:
                        raise AttributeError("Invalid collection name: %s was not found in %s" 
                                             % (_coll,_all_colls))
            elif coll_date <= date.today():
                ''' get all collections containing date postfix using the HASINNAME wild card'''
                kwargs["HASINNAME"]=coll_date
#                 clsNoSQL.collections={"HASINNAME":coll_date}
#                 db_coll=clsNoSQL.collections
#                 print(db_coll)
            else:
                raise AttributeError("Either a db_coll list or mpt_date must be specified")

            ''' read all collections into a list '''
            _mpts = clsNoSQL.read_documents(
                as_type="LIST",
                db_name=_db_name,
                db_coll=db_coll,
                doc_find={},
                **kwargs)
            if len(_mpts)>0:
                self._portfolio = _mpts
                logger.info("%s Loaded %d portfolios",__s_fn_id__,len(self._portfolio))
            else:
                raise ValueError("No portfolios found for collections: %s in %s %s"
                                 % (db_coll,clsNoSQL.dbType,clsNoSQL.dbName))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._portfolio


    ''' --- WRITE MOVEMENT (INDEX) ---

            author: <samana.thetha@gmail.com>
    '''

    def write_movement(
        self,
        portfolio:list=[],   #
        movement:list=[], #
#         db_name:str="",   # specifies the name of the database; else use default value
#         db_coll:str="",  # specifies the collection name; else default to date postfix
#         coll_date:date=date.today(),   # specifies the date of the portfolio
        **kwargs,
    ) -> list:
        """
        Description:

        Attributes:
            portfolio (list)
            movement (list)
            db_name (str) specifies the name of the database; else use default value
            db_coll (str) specifies the collection name; else default to date postfix
            mpt_date (date) specifies the date of the portfolio
            **kwargs
        Returns (list) self._portfolio is a list of dictionaries
        Exceptions:
            if database collection list returns None log eventand raise a RuntimeError
        """

        __s_fn_id__ = "function <write_movement>"
        __def_db_type__ ='MongoDB'
        __destin_db_name__ = "tip-portfolio"
        __destin_coll_prefix__="movement"
        __uids__ = ['date']  # portfolio date
#                     'asset']  # asset name

#         _db_coll = []

        try:
            if len(portfolio)>0:
                self._portfolio=portfolio
            if len(movement)>0:
                self._movement=movement
            if not (self.portfolio or self.movement):
                raise AttributeError("Either class property portfolio or movement is Non-type")
            ''' initialize collection vars '''
            _movement_list=[]
            _coll_dict={}
            _movement_dt=max([x['date'] for x in self.portfolio])
            _coll_dict['date']=_movement_dt 
            for _mcap in self._portfolio:
                _coll_dict[".".join(['mcap',_mcap['asset'],'weight'])]=_mcap['mcap.weight']
                _coll_dict[".".join(['mcap',_mcap['asset'],'value'])]=_mcap['mcap.value']
                _coll_dict[".".join(['mcap',_mcap['asset'],'db.fk'])]=_mcap['mcap.db.fk']
                _coll_dict[".".join(['mcap',_mcap['asset'],'db.source'])]=_mcap['mcap.db.source']
            for _idx in self._movement:
                _coll_dict[".".join(['index',_idx])]=self._movement[_idx]
            _coll_dict["audit.mod.by"]=os.environ.get('USERNAME').upper()
            _coll_dict["audit.mod.dt"]=datetime.strftime(datetime.now(),'%Y-%m-%dT%H:%M:%S')
            _coll_dict["audit.mod.proc"]="-".join([self.__name__,__s_fn_id__])
            _movement_list.append(_coll_dict)

            ''' set database name & check exists'''
            _destin_db = __destin_db_name__
            if "DESTINDBNAME" in kwargs.keys():
                _destin_db = kwargs["DESTINDBNAME"]
            if "DBAUTHSOURCE" in kwargs.keys():
                clsNoSQL.connect={'DBAUTHSOURCE':kwargs['DBAUTHSOURCE']}
            else:
                clsNoSQL.connect={'DBAUTHSOURCE':_destin_db}
#             ''' confirm database exists '''
#             if not _destin_db in clsNoSQL.connect.list_database_names():
#                 raise RuntimeError("%s does not exist",_destin_db)

            ''' set collection prefix '''
            _mpt_coll_prefix = __destin_coll_prefix__
            if "COLLPREFIX" in kwargs.keys():
                _mpt_coll_prefix = kwargs["COLLPREFIX"]

            ''' write portfolio to collection '''
#             _colls_list=[]
#             _uniq_dates = set([x['date'] for x in _data])
#             for _date in _uniq_dates:
            _asset_count=len([x['asset'] for x in self._portfolio])
#             _assets = list(filter(lambda d: d['asset'] == _date, _data))
            _destin_coll = '.'.join([
                _mpt_coll_prefix,"top",str(_asset_count),_movement_dt.split('T')[0]
            ])
            _write_coll = clsNoSQL.write_documents(
                db_name=_destin_db,
                db_coll=_destin_coll,
                data=_movement_list,
                uuid_list=__uids__)
#             _colls_list.append(_mpt_coll)
            ''' confirm returned collection counts '''
            if len(_write_coll) > 0:
                logger.debug("%s %d documents written to %s collection in %s %s database",
                             __s_fn_id__,len(_movement_list),
                             _destin_coll,clsNoSQL.dbType,_destin_db)
#             if len(_colls_list) > 0:
#                 self._portfolio = _colls_list
#                 logger.info("Wrote %d mpt collections successfully to %s %s",
#                             len(self._portfolio),clsNoSQL.dbType,_destin_db)
#             else:
#                 raise RuntimeError("Something was wrong with writing mpt to collections in %s %s"
#                                    ,clsNoSQL.dbType,_destin_db)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _write_coll


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
            ''' validate index value w.r.t index type '''
            if self.idxType.upper() in ['RSI','MFI'] and \
                float(index_value)>=0 and float(index_value)<=1.0:
                self._idxValue = float(index_value)
                logger.debug("%s Validated %s value %0.4f set for class property",
                             __s_fn_id__,self.idxType.upper(),self._idxValue)
            elif self.idxType.upper() in ['SORTIONO','SHARP'] and float(index_value)>=0:
                self._idxValue = float(index_value)
                logger.debug("%s Validated %s value  %0.4f set for class property",
                             __s_fn_id__,self.idxType.upper(),self._idxValue)
            elif self.idxType.upper() in ['BETA','ALPHA'] and \
                float(index_value)>=-1.0 and float(index_value)<=1.0:
                self._idxValue = float(index_value)
                logger.debug("%s Validated %s value %0.4f set for class property",
                             __s_fn_id__,self.idxType.upper(),self._idxValue)
            else:
                self._idxValue = index_value
                logger.warning("%s %s index value %0.4f set as class property",
                               __s_fn_id__,self.idxType,self._idxValue)

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
            if self._idxType not in self._idxTypeList:
                raise AttributeError("%s Invalid index type; must be one of %s"
                                 %(self._idxType,self._idxTypeList))
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._idxType.upper()

    @idxType.setter
    def idxType(self,index_type) -> DataFrame:

        __s_fn_id__ = "function <@setter idxType>"

        try:
            if index_type.upper() not in self._idxTypeList:
                raise AttributeError("Invalid index type %s; must be one of %s"
                                 %(index_type.upper(),self._idxTypeList))

            self._idxType=index_type.upper()

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._idxType


    ''' --- CALC PERFORMANCE INDICATORS ---

            author: <samana.thetha@gmail.com>
    '''

    def calc_index(func):

        @functools.wraps(func)
        def index_wrapper(self,portfolio,asset_eval_date,asset_name_col,asset_val_col,
                          asset_date_col,index_type,risk_free_assets,risk_free_name_col,
                          risk_free_val_col,risk_free_date_col,**kwargs):
            """
            Description:
                wrapper function to compute the RSI for the given data
            Attributes:
                same as get_rsi
            Returns:
                self._idxValue
            """
            
            __s_fn_id__ = "function <index_wrapper>"

#             _pos_sdf=None
#             _neg_sdf=None
            _weights_pdf=None
            _ror_data=None
            _rf_data=None
            idx_val_=None

            try:
                _ror_data, _rf_data = func(self,portfolio,asset_eval_date,asset_name_col,
                                           asset_val_col,asset_date_col,index_type,
                                           risk_free_assets,risk_free_name_col,
                                           risk_free_val_col,risk_free_date_col,**kwargs)

                ''' calc values for all index types '''
                idx_val_dict = {}
                for _idx_name in index_type:
                    try:
#                         if self._idxType in ['RSI','MFI']:
                        if _idx_name.upper() in ['RSI','MFI']:
                            _weights_df = pd.DataFrame(self.portfolio)
                            idx_val_=Portfolio.calc_movement(
                                ror_data=_ror_data,
                                val_col=asset_val_col,
                                part_col=asset_name_col,
                                weights_pdf=_weights_df,
                                index_type=_idx_name,
#                                 index_type=self._idxType,
                                **kwargs,
                            )

#                         elif self._idxType == 'DMI':
                        elif _idx_name.upper() == 'DMI':
                            self.data = Portfolio.calc_dmi(
                                ror_data=_ror_data,
                                val_col=asset_val_col,
                                part_col=asset_name_col,
                                **kwargs,
                            )

#                         elif self._idxType == 'ADX':
                        elif _idx_name.upper() == 'ADX':
                            idx_val_,self.data = Portfolio.calc_adx(
                                dmi_sdf=_ror_data,
                                val_col=asset_val_col,
                                part_col=asset_name_col,
                                weights_list=self.portfolio,
                                **kwargs,
                            )

#                         elif self._idxType in ['BETA','SHARP','SORTINO']:
                        elif _idx_name.upper() in ['BETA','SHARP','SORTINO']:
                            idx_val_, self.data = Portfolio.calc_movement_with_risk(
                                ror_sdf=_ror_data,
                                val_col=asset_val_col,
                                part_col=asset_name_col,
                                weights_list=self.portfolio,
                                risk_free_sdf=_rf_data,
                                risk_free_val_col=asset_val_col,
                                risk_free_part_col=asset_name_col,
                                index_type=_idx_name,
#                                 index_type=self._idxType,
                                **kwargs,
                            )

                        else:
                            raise RuntimeError("%s something was wrong" % __s_fn_id__)
                        if idx_val_ is None:
                            raise ValueError("%s returned None type %s value"
                                             ,__s_fn_id__,_idx_name)
                        logger.info("%s computed index value for %s = %0.4f",
                                    __s_fn_id__,_idx_name,idx_val_)
                        idx_val_dict[_idx_name]=idx_val_

                    except Exception as idx_err:
                        logger.warning("%s",idx_err)
                if len(idx_val_dict)>0:
                    self._movement=idx_val_dict

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._movement
#             return idx_val_dict
        
        return index_wrapper

  
    @calc_index
    def get_index(
#     def get_rsi(
        self,
        portfolio:list=[],
        asset_eval_date:date=date.today(),
        asset_name_col:str='asset_name',
        asset_val_col: str='log_ror',
        asset_date_col:str='mcap_date',
#         index_type:str='RSI',
        index_type:list=['RSI'],
        risk_free_assets:list=[],
        risk_free_name_col:str='asset_name',
        risk_free_val_col: str='log_ror',
        risk_free_date_col:str='mcap_date',
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
                ror (float64)   - ratio of return (e.g. log_ror), relative to previous date
                value (float64) - the mcap value of the asset for that date
            asset_eval_date (timestamp)- of the asset evaluation date 
            asset_name_col (str)- defines the column containinf asset name; also used as
                the partitioning column
            asset_val_col (str) - defines the numeric column holding the value
            index_type (str) - defines the type of index computation to perform; see the valid
                list in self._idxTypeLlist
            kwargs:
                VALCOLNAME (str)  - defines the numeric column to use in the index calc
                DATETIMEATTR (str)- defines the datetime column name to use in timeseries SMA
                WINLENGTH (str)   - defines the window length; typically 14 days
                WINUNIT (str)     - defines the window length is DAY, WEEK, MINUTE, HOUR
                RSITYPE (str)     - STEPONE or STEPTWO first or second step calculation
        Returns:
            self._data (DataFrame) comprising the T-day timeseries that defines the RSI
        Exceptions:
            Setting the self.portfolio class property validates the portfolio data; else raises
            an AttributeError exception
            Setting the self.idxType class property with the 
            By returning the data read through sparkDBwls into self.data class property, it
            gets validated; else will raise an AttributeError exception
        """

        __s_fn_id__ = "function <get_index>"

        _tbl_name = 'warehouse.mcap_past'
        _mcap_min_val = 10000
        __def_dt_attr__ = "mcap_date"
        __def_win_len__ = 14
        __def_len_unit__= "DAY"
        
        risk_free_sdf_=None

        try:
            ''' validate and set portfolio data '''
            self.portfolio=portfolio
            _idx_lsit_upper = [x.upper() for x in index_type]
            _inval_idxs = [idx for idx in _idx_lsit_upper 
                                if idx not in self._idxTypeList]
            if len(_inval_idxs)>0:
                raise AttributeError("Invalid index types %s" % str(_inval_idxs))

            _dates_list = [x['date'].split('T')[0] for x in self._portfolio]
            if str(asset_eval_date) not in _dates_list:
                raise AttributeError("Invalid date %s not in any portfolio dates %s"
                                     % (str(asset_eval_date),str(_dates_list)))

            ''' apply a lower bound to select values above the limit '''
            if "MCAPLIMIT" in kwargs.keys():
                _mcap_min_val=kwargs['MCAPLIMIT']
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
            if "TABLENAME" in kwargs.keys():
                _tbl_name = kwargs['TABLENAME']

            ''' retrieve mcap log ror for each asset in portfolio for the window length '''
            _assets_in = ','.join(["'{}'".format(x['asset']) for x in portfolio])
            _to_date = str(asset_eval_date)
            ''' get for window length + 1 day '''
            _from_date = str(asset_eval_date - timedelta(days=kwargs["WINLENGTH"]))
            _query =f"select * from {_tbl_name} "+\
                    f"where mcap_date >= '{_from_date}' and mcap_date <= '{_to_date}' "+\
                    f"and asset_name in ({_assets_in}) "+\
                    f"and mcap_value > {_mcap_min_val}"

            self.data = clsSDB.read_data_from_table(select=_query, **kwargs)
            self._data.na.drop(subset=[asset_val_col])
            if self._data.count() > 0:
                logger.debug("%s loaded %s assets %d rows and %d columns between %s and %s from %s"
                             ,__s_fn_id__,str(_assets_in),self._data.count(),len(self._data.columns),
                             str(_from_date),str(_to_date),_tbl_name.upper())
            else:
                raise ValueError("No data portfolio asset data received for query %s: " % _query)

            ''' to compute sharp, sortion, etc index get risk free data '''
            risk_free_idx_list = list(set(_idx_lsit_upper)\
                                      .intersection(set(['SHARP','SORTINO','BETA'])))
            if len(risk_free_idx_list)>0:
#                 print('get risk free data %s'
#                       % set(_idx_lsit_upper).intersection(set(['SHARP','SORTINO','BETA'])))
#             if index_type.upper() in ['SHARP','SORTINO','BETA']:
                risk_free_sdf_ = Portfolio.get_risk_free_data(
                    asset_name=risk_free_assets,
                    asset_eval_date=asset_eval_date,
                    asset_name_col=risk_free_name_col,
                    asset_date_col=risk_free_date_col,
                    asset_val_col=risk_free_val_col,
                    **kwargs,
                )

                if risk_free_sdf_.count() > 0:
                    logger.debug("%s loaded %d rows and %d columns for %s risk free data assets %s"
                                 ,__s_fn_id__,risk_free_sdf_.count()
                                 ,len(risk_free_sdf_.columns),risk_free_idx_list
                                 ,str(risk_free_assets))
                else:
                    raise ValueError("No data portfolio asset data received for query %s: " % _query)

            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data,risk_free_sdf_


    ''' --- WEIGHTED PORTFOLIO RETURNS --- 
    
    '''
    @staticmethod
    def get_weighted_ror(
        ror_sdf:DataFrame,
        ror_asset_col:str,
        ror_val_col:str,
        weights_list:list,
        weight_asset_col:str,
        weight_val_col:str,
        **kwargs,
    ):

        __s_fn_id__ = "@staticmethod <get_weighted_ror>"

        weighted_data_=None

        try:
            ''' validate input parameters '''
            if ror_sdf.count()<=0 or len(weights_list)<=0:
                raise AttributeError("Either data or weights is an Empty dataframe")
            if "".join(ror_asset_col.split())=="" or \
                "".join(ror_val_col.split())=="" or \
                "".join(weight_asset_col.split())=="" or \
                "".join(weight_val_col.split())=="":
                raise AttributeError("One or more of the essentila parameters is empty "+\
                                     "%s, %s, %s, %s"
                                     % (ror_asset_col,ror_val_col,weight_asset_col,weight_val_col))
            ''' validate asset column values '''
            dist_data_assets =ror_sdf.select(F.collect_set(ror_asset_col)\
                                             .alias(ror_asset_col))\
                                        .first()[ror_asset_col]
            dist_weight_assets=[x[weight_asset_col] for x in weights_list]
            if len(set(dist_data_assets).union(set(dist_weight_assets)))!=len(set(dist_data_assets)):
                raise ValueError("Mismatch of distinct values between % in Data and %s in Weight"
                                 % (ror_asset_col,weight_asset_col))
                
            ''' set the ror weighted value column name from kwargs, if exisits '''
            _ror_weight_val_col="_".join(['weighted',ror_val_col])
            if "RORWEIGHTCOL" in kwargs.keys():
                _ror_weight_val_col=kwargs['RORWEIGHTCOL']

            ''' agument asset specific weights to ror dataframe '''
            obj_map = {}
            for _wt_rec in weights_list:
                obj_map[_wt_rec[weight_asset_col]]=_wt_rec[weight_val_col]
            mapping_expr = F.create_map([F.lit(x) for x in chain(*obj_map.items())])
            ''' set the corresponding weights in dataframe '''
            df1 = ror_sdf.filter(F.col(ror_asset_col).isNull())\
                            .withColumn('weight', F.lit(None))
            df2 = ror_sdf.filter(F.col(ror_asset_col).isNotNull())\
                            .withColumn('weight',\
                                        F.when(F.col(ror_asset_col).isNotNull(),\
                                               mapping_expr[F.col(ror_asset_col)]
                                              ))
            weighted_data_ = df1.unionAll(df2)
            weighted_data_ = weighted_data_.withColumn(_ror_weight_val_col,
                                                       (F.col(ror_val_col)*F.col('weight')))
            ''' check if non-empty dataframe '''
            if weighted_data_.count()<=0:
                raise AttributeError("No rows in merged wieghted dataframe")
            logger.debug("%s Merged dataframes returning %d rows and %d columns",
                         __s_fn_id__,weighted_data_.count(),len(weighted_data_.columns))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return weighted_data_


    ''' --- MOVEMENT INDEXES (NO RISK) ---

            TODO: (i) verify weights shape, if not same as dataframe create all 1.0 array
                 (ii) if asset_name and value (rorO column not specified in kwargs; try
                    to find the first column with float values
                (iii) do both step 1 and step 2 RSI caculations

            author: <samana.thetha@gmail.com>
    '''

    @staticmethod
    def calc_movement(
        ror_data:DataFrame=None,
        val_col:str="log_ror",
        part_col:str="asset_name",
        weights_pdf:pd.DataFrame=None,
        index_type:str="RSI",
        **kwargs,
    ) -> float:
        """
        Description:
        Attributes:
        Returns"
        Exceptions:
        """

        __s_fn_id__ = "@staticmethod <calc_movement>"

        try:
            if ror_data.count() <=0:
                raise AttributeError("Cannot compute %s with empty dataframe" % index_type)
            if "".join(val_col.strip())=="" or \
                val_col not in ror_data.columns or \
                ror_data.select(F.col(val_col)).dtypes[0][1]=='string':
                raise AttributeError("A valid numeric column from %s required" % ror_data.dtypes)

            _pos_sdf = ror_data.groupBy(part_col)\
                            .agg(\
                                 F.sum(F.when(F.col(val_col)>=0,\
                                              F.col(val_col))\
                                      .otherwise(0))\
                                 .alias('pos_col')).sort(part_col)

            _neg_sdf = ror_data.groupBy(part_col)\
                            .agg(\
                                 F.sum(F.when(F.col(val_col)<0,\
                                              F.col(val_col))\
                                      .otherwise(0))\
                                 .alias('neg_col')).sort(part_col)

            _pos_arr=_pos_sdf.toPandas()['pos_col'].astype('float').to_numpy()
            _neg_arr=_neg_sdf.toPandas()['neg_col'].astype('float').to_numpy()

            _weights_arr = weights_pdf['mcap.weight'].to_numpy()
            _pos = np.matmul(_pos_arr,_weights_arr)
            _neg = np.matmul(_neg_arr,_weights_arr)

            ret_idx_val_=None
            if index_type.upper() == 'RSI':
                ret_idx_val_=float(1/(1+abs(_neg/_pos)))
            elif index_type.upper()=='MFI':
                ret_idx_val_=1.0-float(1/(1+abs(_pos/_neg)))
            else:
                raise RuntimeError('Something went wrong')

            ''' validate and return computed index '''
            if ret_idx_val_ is None:
                raise ValueError("% returned invalid None-type %s index computation"
                                 %(__s_fn_id__,index_type))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return ret_idx_val_


    ''' --- DIRECTIONAL MOVEMENT INDICATOR ---
    
            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def get_dmi_data(
#         self,
        ror_sdf_:DataFrame=None,
        val_col:str="log_ror",
        part_col:str="asset_name",
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
            **kwargs not used
        Returns:
            adx (float) with the single adx number between [0,1]
            adx_sdf (DataFrame) augments the all the DI avg, sum, smooth, adx columns to the
                original DataFrame
        """

        __s_fn_id__ = "@staticmethod <get_dmi_data>"

        try:
            ''' validate the attribues '''
            if ror_sdf_.count()<=0:
                raise AttributeError("Cannot compute RSI with empty dataframe")
#             ror_sdf_=ror_data
            if "".join(val_col.strip())=="" or \
                val_col not in ror_sdf_.columns or \
                ror_sdf_.select(F.col(val_col)).dtypes[0][1]=='string':
                raise AttributeError("A valid numeric column from %s required" % ror_sdf_.dtypes)
            if "".join(part_col.strip())=="":
                raise AttributeError("Invalid partition column, specify one from %s" % ror_sdf_.dtypes)

            ''' compute the DMI sum, avg, smooth, and DI column values '''
            ror_sdf_ = ror_sdf_.withColumn("+DM",
                                           F.when(F.col(val_col) > 0,
                                                  F.abs(F.col(val_col)))
                                           .otherwise(0))
            ror_sdf_ = ror_sdf_.withColumn("-DM",
                                           F.when(F.col(val_col) <= 0,
                                                  F.abs(F.col(val_col)))
                                           .otherwise(0))
            ''' Smoothed values '''
            kwargs['RESULTCOL']='sm_sum_+DM'
            ror_sdf_ = clsStats.simple_moving_stats(
                num_col="+DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="sum",
                data=ror_sdf_,
                **kwargs,
            )
            kwargs['RESULTCOL']='sm_avg_+DM'
            ror_sdf_ = clsStats.simple_moving_stats(
                num_col="+DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="avg",
                data=ror_sdf_,
                **kwargs,
            )
            kwargs['RESULTCOL']='sm_sum_-DM'
            ror_sdf_ = clsStats.simple_moving_stats(
                num_col="-DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="sum",
                data=ror_sdf_,
                **kwargs,
            )
            kwargs['RESULTCOL']='sm_avg_-DM'
            ror_sdf_ = clsStats.simple_moving_stats(
                num_col="-DM",
                date_col="mcap_date",
                part_col=part_col,
                stat_op="avg",
                data=ror_sdf_,
                **kwargs,
            )

            ''' shift +DM & -DM column by period=1 '''
            _win = Window.partitionBy(F.col(part_col)).orderBy(F.col("mcap_date").cast('long'))
            ror_sdf_ = ror_sdf_.withColumn('shift_+DM',
                                           F.lag(F.col('+DM'),offset=1,default=0)
                                           .over(_win))
            ror_sdf_ = ror_sdf_.withColumn('shift_-DM',
                                           F.lag(F.col('-DM'),offset=1,default=0)
                                           .over(_win))
            ''' smoothen the data '''
            ror_sdf_ = ror_sdf_.withColumn('smooth_+DM',
                                           (F.col('sm_sum_+DM')
                                            -F.col('sm_avg_+DM')
                                            +F.col('shift_+DM')))
            ror_sdf_ = ror_sdf_.withColumn('smooth_-DM',
                                           (F.col('sm_sum_-DM')
                                            -F.col('sm_avg_-DM')
                                            +F.col('shift_-DM')))
            ''' Final Calculations of DI+ & DI- '''
            ror_sdf_ = ror_sdf_.withColumn('+DI',(F.col('sm_avg_+DM')/F.col('smooth_+DM')))
            ror_sdf_ = ror_sdf_.withColumn('-DI',(F.col('sm_avg_-DM')/F.col('smooth_-DM')))

            if ror_sdf_.count()>0:
                logger.debug("%s computation of DI+ and DI- resulted in %d rows",
                             __s_fn_id__,ror_sdf_.count())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return ror_sdf_ #self._data  #adx_.collect()[0][0],adx_sdf_


    ''' --- AVERAGE DIRECTIONAL INDEX ---
    
            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def calc_adx(
        dmi_sdf:DataFrame=None,
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
            dmi_sdf (DataFrame) the directional movement indicator dataframe
            val_col (str) defines the numeric column to use; default value = log_ror
            part_col (str) defines the partition column; i.e categories; default value = asset_name
            weights_list (list) contains dictionaries of asset_name and associated weight,
            **kwargs not used
        Returns:
            adx (float) with the single adx real number
        """

        __s_fn_id__ = "@staticmethod <calc_adx>"
        adx_ = None
        adx_sdf_=None
        obj_map = {}

        try:
            if dmi_sdf.count() <=0:
                raise AttributeError("Cannot compute ADX with empty dataframe")
            if "".join(val_col.strip())=="" or \
                val_col not in dmi_sdf.columns or \
                dmi_sdf.select(F.col(val_col)).dtypes[0][1]=='string':
                raise AttributeError("A valid numeric column from %s required" % dmi_sdf.dtypes)
            if "".join(part_col.strip())=="":
                raise AttributeError("Invalid partition column, specify one from %s" % dmi_sdf.dtypes)

            ''' check if DI+/DI- are already in dataframe; else compute them first '''
#             if not ("+DICOL" in kwargs.keys() and "-DICOL" in kwargs.keys()): and \
#                 not (kwargs['+DICOL'] in dmi_sdf.columns and kwargs['-DICOL'] in dmi_sdf.columns):
            if "+DI" not in dmi_sdf.columns or "-DI" not in dmi_sdf.columns:
                logger.warning("Could not find +DI or -DI columns; proceeding with DMI calculation")
                dmi_sdf = Portfolio.get_dmi_data(
                    ror_sdf_= dmi_sdf,
                    val_col=val_col,
                    part_col=part_col,
                    **kwargs,
                )
#             ''' check if weights match dataframe shape '''
#             _asset_count = dmi_sdf.select(F.col(part_col)).distinct().count()
#             if len(weights_list) != _asset_count:
#                 raise AttributeError("Dimension mismatching weights %d "+ \
#                                      "to that of distinct asset count %d"
#                                      % (len(weights_list),r_asset_count))

            adx_sdf_=Portfolio.get_weighted_ror(
                ror_sdf=dmi_sdf,
                ror_asset_col=part_col,
                ror_val_col='+DI',
                weights_list=weights_list,
                weight_asset_col='asset',
                weight_val_col='mcap.weight',
                **kwargs,
            )
            adx_sdf_=Portfolio.get_weighted_ror(
                ror_sdf=adx_sdf_,
                ror_asset_col=part_col,
                ror_val_col='-DI',
                weights_list=weights_list,
                weight_asset_col='asset',
                weight_val_col='mcap.weight',
                **kwargs,
            )
            adx_sdf_ = adx_sdf_.withColumn('ADX',(F.col('weighted_-DI')-F.col('weighted_+DI'))/
                                        (F.col('weighted_-DI')+F.col('weighted_+DI')))
            adx_ = adx_sdf_.select(F.sum(F.col('ADX'))).alias("adx_val")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return adx_.collect()[0][0],adx_sdf_


    ''' --- GET RISK FREE ROR ---

            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def get_risk_free_data(
        asset_name:list=['bitcoin'],
        asset_eval_date:date=None,
        asset_name_col:str='asset_name',
        asset_date_col:str='mcap_date',
        asset_val_col:str='mcap_value',
        **kwargs,
    ):
        """
        Description:
        Attributes:
        Returns:
        Exceptions:
        """

        __s_fn_id__ = "@staticmethod <get_risk_free_data>"

        _tbl_name = 'warehouse.mcap_past'
        _mcap_min_val = 0
        __def_dt_attr__ = "mcap_date"
        __def_win_len__ = 14
        __def_len_unit__= "DAY"

        try:
            ''' validate and set portfolio data '''
#             _dates_list = [x['date'].split('T')[0] for x in portfolio]
#             if str(asset_eval_date) not in _dates_list:
#                 raise AttributeError("Invalid date %s not in any portfolio dates %s"
#                                      % (str(asset_eval_date),str(_dates_list)))

            ''' apply a lower bound to select values above the limit '''
            if "MCAPLIMIT" in kwargs.keys():
                _mcap_min_val=kwargs['MCAPLIMIT']
            ''' estanlish the window length and unit '''
            if "DATETIMEATTR" not in kwargs.keys():
                kwargs["DATETIMEATTR"]=__def_dt_attr__
            if "WINLENGTH" not in kwargs.keys():
                kwargs["WINLENGTH"]=__def_win_len__
            if "WINUNIT" not in kwargs.keys():
                kwargs["WINUNIT"]=__def_len_unit__
            if "TABLENAME" in kwargs.keys():
                _tbl_name = kwargs['TABLENAME']

            ''' retrieve mcap log ror for each asset in portfolio for the window length '''
            _assets_in = ','.join(["'{}'".format(x) for x in asset_name])
            _to_date = str(asset_eval_date)
            ''' get for window length + 1 day '''
            _from_date = str(asset_eval_date - timedelta(days=kwargs["WINLENGTH"]))
            _query =f"select * from {_tbl_name} "+\
                    f"where {asset_date_col} >= '{_from_date}' "+\
                    f"and {asset_date_col} <= '{_to_date}' "+\
                    f"and {asset_name_col} in ({_assets_in}) "+\
                    f"and mcap_value > {_mcap_min_val}"

            risk_free_data_ = clsSDB.read_data_from_table(select=_query, **kwargs)
            risk_free_data_.na.drop(subset=[asset_val_col])
            if risk_free_data_.count() > 0:
                logger.debug("%s loaded %d rows and %d columns for %s assets",
                             __s_fn_id__,risk_free_data_.count(),
                             len(risk_free_data_.columns), str(_assets_in))
            else:
                raise ValueError("No asset data received for query %s: " % _query)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return risk_free_data_


    ''' --- MOVEMENT WITH RISK (SHARP, BETA, SORTINO) ---

            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def calc_movement_with_risk(
        ror_sdf:DataFrame=None,
        ror_val_col:str="log_ror",
        ror_part_col:str="asset_name",
        weights_list:list=[],
        risk_free_sdf:DataFrame=None,
        risk_free_val_col:str="log_ror",
        risk_free_part_col:str="asset_name",
        index_type:str="SHARP",
        **kwargs,
    ) -> float:
        """
        Description:
            Sharpe Ratio (or Sharpe Index) gauges the performanceof an investment by adjusting
                for its risk. The higher the ratio, the greater the investment return relative
                to the amount of risk taken, and thus, the better the investment.
                Sharpe Ratio = (Rx – Rf) / StdDev Rx; where Rx = Expected portfolio return, 
                Rf = Risk-free rate of return, StdDev Rx = Standard deviation of portfolio return
                Grading threshold < 1: Bad, 1 – 1.99: Adequate, 2 – 2.99: Very good, > 3: Excellent
            Beta is the volatility of the investment. Along with the Alpha, it is used in the
                Capital Asset Pricing Model (CAPM).
                Beta = Covariance/Variance
        Attributes:
            ror_data (DataFrame) the rate of returns dataframe
            ror_val_col (str) defines the numeric column to use; default value = log_ror
            part_col (str) defines the partition column; i.e categories; default value = asset_name
            weights_list (list) contains dictionaries of asset_name and associated weight,
            **kwargs not used
        Returns:

        Exceptions:

        Resources: understad the stochastic behavior of Beta https://www.jstor.org/stable/2290501

        """

        __s_fn_id__ = "@staticmethod <calc_movement_with_risk>"

        sharp_sdf=None
        obj_map = {}

        try:
            if ror_sdf.count() <=0:
                raise AttributeError("Cannot compute Sharp ratio with empty ror dataframe")
            if risk_free_sdf.count() <=0:
                raise AttributeError("Cannot compute Sharp ratio with empty risk free dataframe")

            ''' agument asset specific weights to ror dataframe '''
            kwargs['RORWEIGHTCOL']='weighted_ror'
            sharp_sdf_=Portfolio.get_weighted_ror(
                ror_sdf=ror_sdf,
                ror_asset_col=ror_part_col,
                ror_val_col=ror_val_col,
                weights_list=weights_list,
                weight_asset_col='asset',
                weight_val_col='mcap.weight',
                **kwargs,
            )

            ret_idx_val_=None
            if index_type.upper() in ['SHARP','SORTINO']:
                rp = float(sharp_sdf_.select(F.mean(F.col('weighted_ror')))\
                                .alias("rp_rate").collect()[0][0])
                rf = float(risk_free_sdf.select(F.mean(F.col(risk_free_val_col)))\
                                .alias("rf_rate").collect()[0][0])
                if index_type.upper()=="SHARP":
                    stdv_p = float(sharp_sdf_.select(F.stddev(F.col('weighted_ror')))\
                                  .alias("rp_stdv").collect()[0][0])
                elif index_type.upper()=="SORTINO":
                    stdv_p = sharp_sdf_.select(F.col('weighted_ror'))\
                                        .filter(F.col('weighted_ror')<0)\
                                        .agg(F.stddev(F.col('weighted_ror'))\
                                             .alias('rp_stdv')).collect()[0][0]
                    if stdv_p==0:
                        raise ValueError("No volatility cannot devide by zero standard deviation")
                else:
                    raise RuntimeError("Something went wrong")
                ret_idx_val_=(rp-rf)/stdv_p

            elif index_type.upper()=="BETA":
                ''' groupBy date to compute ROR sum '''
                _grp_ror_sdf = sharp_sdf_.groupBy(F.col('mcap_date'))\
                                        .agg(F.sum(F.col(kwargs['RORWEIGHTCOL']))\
                                            .alias('ror_sum'))\
                                        .sort(F.col('mcap_date'))
                ''' get the arrays for portfolio and risk free ror'''
                rp_arr=_grp_ror_sdf.toPandas()['ror_sum'].astype('float').to_numpy()
                rf_arr=risk_free_sdf.toPandas()[risk_free_val_col].astype('float').to_numpy()
                if rp_arr.shape != rf_arr.shape:
                    raise AttributeError("Mismatch between porfolio array shape %s "+\
                                         "and risk free array shape %s" 
                                         %(str(rp_arr.shape), str(rf_arr.shape)))
                ret_idx_val_=np.cov(rp_arr,rf_arr,bias=True)[0][1]/np.var(rp_arr)
            else:
                raise AttributeError("Invalid index_type: %s" % index_type)

            if ret_idx_val_ is None:
                raise ValueError("Invalid return index value computed for %s" % index_type)
            logger.debug("%s computed %s index value = %0.4f",__s_fn_id__,index_type,ret_idx_val_)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return ret_idx_val_,sharp_sdf_


    ''' --- R-SQUARED ---

            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def calc_rsquared(
        ror_data:DataFrame=None,
        val_col:str="log_ror",
        part_col:str="asset_name",
        weights_list:list=[],
        **kwargs,
    ) -> float:
        """
        Description:

        Attributes:
            ror_data (DataFrame) the rate of returns dataframe
            val_col (str) defines the numeric column to use; default value = log_ror
            part_col (str) defines the partition column; i.e categories; default value = asset_name
            weights_list (list) contains dictionaries of asset_name and associated weight,
            **kwargs not used
        Returns:

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


    ''' --- DIVERGENCE ---

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


    ''' --- ALPHA ---

            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def calc_alpha(
        ror_data:DataFrame=None, # the rate of returns dataframe
        val_col:str="log_ror",   # defines the numeric column to use
        part_col:str="asset_name", # defines the partition column
        weights_list:list=[],    # contains dictionaries of asset_name and associated weight
        **kwargs,
    ) -> float:
        """
        Description:
            Alpha is a percentage reflects how an investment performed relative to a benchmark
            index. Alpha > 0 means the portfolio overperformed exceeded the benchmark and
            an Alpha < 0 means it underperformed. Along with the Beta, it is used in the
            Capital Asset Pricing Model (CAPM).
            Alpha = R – Rf – beta (Rm-Rf); where R - portfolio return, Rf - risk-free ROR, 
            Beta - portfolio systematic risk, Rm represents the market return, per a benchmark

        Attributes:
            ror_data (DataFrame) the rate of returns dataframe
            val_col (str) defines the numeric column to use; default value = log_ror
            part_col (str) defines the partition column; i.e categories; default value = asset_name
            weights_list (list) contains dictionaries of asset_name and associated weight,
            **kwargs not used
        Returns:
            alpha (float) 
        """

        __s_fn_id__ = "@staticmethod <calc_alpha>"

        try:
            if ror_data.count() <=0:
                raise AttributeError("Cannot compute RSI with empty dataframe")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None

