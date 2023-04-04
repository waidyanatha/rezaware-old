#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "FeatureEngineer"
__module__ = "ml"
__package__ = "dimreduc"
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

    from datetime import datetime, date, timedelta
#     import numpy as np
#     import pandas as pd
#     from sklearn.decomposition import PCA

    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame,Window
    from pyspark.ml.feature import PCA, VectorAssembler #,StringIndexer, OneHotEncoder
    from pyspark.sql.types import *

    print("All packages in %s %s %s %s imported successfully!"
          % (__app__,__module__,__package__,__name__))

except Exception as e:
    print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

class FeatureEngineer():
    """ ***************************************************************************************
    CLASS derives the authogonal prinicipal components for a data set
    
    The functions aim to reduce the dimension of the data set.
    Identify the pinicipal components and the statistical significance of the variables
    defined by each principal components
    Offer a set of KMO measurements to validate the components and their usability
    
    When the class is instantiated, it will inherit properties and methods from packages 
    (1) rezaware - application specific configuration and logging functions
    
    Resources:
    
    contributors:
        * samana.thetha@gmail.com
        
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
        
        self._session=None   # spark session property
        self._data = None    # dataframe property
        self._features=None   # PCA derived components; i.e. PCA1, PCA2, etc
#         self._endDT = None
#         self._dtAttr = None  # timestamp property
#         self._partAttr=None  # partitionBy column name
#         self._winSpecUnits = ['MINUTE','HOUR','DAY']
#         self._winSpec = {
#             'LENGTH':7,   # set default value to 7 days
#             'UNIT':'DAY'  # rolling window
#         }

        global config
        global logger
        global clsSpark
        global clsCNR

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
            from utils.modules.etl.transform import sparkCleanNRich as clnr
            clsCNR = clnr.Transformer(desc=self.__desc__)

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


    ''' Function --- CLASS PROPERTY SETTER/GETTER ---

        author <samana.thetha@gmail.com>
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
        
        __s_fn_id__ = "function <@property data>"
        
        try:
            if self._data is None:
                raise ValueError("Data is of NoneType; cannot be used in any %s %s computations"
                                 % (self.__package__,self.__name__))
            if not isinstance(self._data,DataFrame):
                self._data = self.session.createDataFrame(data)
                logger.debug("Data of dtype %s converted to pyspark DataFrame", type(data))

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


    ''' Function --- FEATURES ---

        author <samana.thetha@gmail.com>
    '''
    @property
    def features(self):
        """
        Description:

        Atributes:

        Returns:
            self._factors ()
        """

        __s_fn_id__ = "function <@property factors>"

        try:
            if self._features is None and self.data.count()>0:
                ''' build the factors '''
                kwargs={}
                self._features = self.get_features(
                    data=self._data,
                    feature_cols=None,
                    **kwargs,
                )
                logger.warning("%s empty feature property set to default with all numeric attributes",
                               __s_fn_id__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._features

    @features.setter
    def features(self,features) -> DataFrame:
        """
        Description:

        Atributes:

        Returns:
            self._factors (DataFrame)
        """

        __s_fn_id__ = "function <@features.setter>"

        try:
            if features is None:
                raise AttributeError("Dataset cannot be empty")

            if not isinstance(features,DataFrame):
                self._features = self.session.createDataFrame(features)
                logger.debug("Data of dtype %s converted to pyspark DataFrame", type(features))
            else:
                self._features = features
                logger.debug("Class property data is a pyspark DataFrame")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._features


    ''' Function --- GET FACTORS ---

        authors: <samana.thetha@gmail.com>
    '''
    def get_features(
        self,
        data=None, # mix of numeric and categorical data
        feature_cols:list=None,   # list of columns; must be numeric data columns
        **kwargs,
    ) -> DataFrame:
        """
        Description:
        Attributes:
        Returns:
            self._factors (DataFrame) - containing the factors and covariance
        Exceptions:
            data must be a pyspark DataFrame or compatible dtype that can be converted
        """

        __s_fn_id__ = "function <get_features>"

        try:
            ''' vaidate and set data and other property values'''
            self.data=data

            _num_cols = [x[0] for x in self._data.dtypes if x[1] not in ['string','date','timestamp']]

            if feature_cols is None or len(feature_cols)==0:
#             if len(feature_cols)==0:
                _cols = _num_cols
                logger.warning("%s No columns defined; defaulting to all numeric columns; %s",
                               __s_fn_id__,_cols)
            else:
                _cols = list(set(feature_cols).intersection(set(_num_cols)))
                logger.debug("%s Working with valid numeric columns %s",__s_fn_id__,_cols)
            if len(_cols) <= 0:
                raise AttributeError("Invalid set of columns defined: %s must be numeric columns: %s"
                                     % (feature_cols,_num_cols))

            if not "COLPREFIX" in kwargs.keys():
                kwargs['COLPREFIX']='scaled'
            if not "STANDARDIZE" in kwargs.keys() and \
                    kwargs['STANDARDIZE'].upper() not in ['MINMAX','STANDARD']:
                    kwargs['STANDARDIZE'] = "standard"

            ''' Normalize the columns between 0 and 1.0 '''
            self._data=clsCNR.scale(
                data=self._data,
                col_list=_cols,
                col_prefix=kwargs['COLPREFIX'],
                scale_method=kwargs['STANDARDIZE'],
            )

            ''' redefining the set of feature columns '''
            _feat_cols = [x for x in self._data.columns if kwargs['COLPREFIX']+"_" in x]
            if self._data.count() <= 0:
                raise RuntimeError("Scaler transformation returned empty dataframe")
            logger.debug("%s scaler transformation successfully returned data in %s columns",
                         __s_fn_id__,str(_feat_cols))

            assembler = VectorAssembler(inputCols=_feat_cols,outputCol='features')
            assembled = assembler.transform(self._data)
            if not "COMPONENTS" in kwargs.keys() or isinstance(kwargs['COMPONENTS'],int):
                kwargs['COMPONENTS']=2
                
            pca = PCA(k=kwargs['COMPONENTS'], inputCol='features', outputCol='pca_features')
            pca_model = pca.fit(assembled)

            self._data=pca_model.transform(assembled)
            if self._data.count()<=0:
                raise RuntimeError("%s empty dataframe with PCA dimension reduction" % __s_fn_id__)
            logger.debug("%s transformed %d rows with PCA dimensionality reduction",
                         __s_fn_id__,self._data.count())
            
            ''' split the features into columns '''
            to_list = lambda v : v.toArray().tolist()
            split_array_to_list = lambda _col : F.udf(to_list, ArrayType(DoubleType()))(_col)
            self._features = self._data.select(split_array_to_list(F.col('pca_features'))\
                                               .alias('PC'))\
                                        .select([F.col("PC")[i] for i in range(kwargs['COMPONENTS'])])
            self._data = self._data.drop('features','pca_features')
            if self._features.count() <= 0:
                raise RuntimeError("%s could not construct PCA feature columns" % __s_fn_id__)
            logger.debug("%s constructed %d feature columns with %d rows",
                         __s_fn_id__,len(self._features.columns),self._features.count())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return self._features
