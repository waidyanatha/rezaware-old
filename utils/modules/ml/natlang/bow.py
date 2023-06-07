#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "BOWwokrkLoads" # bag of words workloads
__package__ = "natlang"    # natural language processing
__module__ = "ml"   # machine learning
__app__ = "utils"   # rezaware utils
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages for rezaware framewor '''
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
    ''' pyspark packages '''
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql import DataFrame

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))


'''
    CLASS runs pyspark corpus and bag of words functions
    
'''

# class Warehouse():
class BOWwokrkLoads():

    ''' Function --- INIT ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="market cap data prep", **kwargs):
        """
        Decription:
            Initializes the ExtractFeatures: class property attributes, app configurations, 
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
        if desc is None or "".join(desc.split())=="":
            self.__desc__ = " ".join([self.__app__,
                                      self.__module__,
                                      self.__package__,
                                      self.__name__])
        else:
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
            from utils.modules.etl.loader import sparkFILEwls as sFile
            clsSFile = sFile.FileWorkLoads(desc=self.__desc__)
            ''' import spark clean-n-rich work load utils to transform the data '''

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


    ''' Function --- CLASS PROPERTIES ---

            author: <nuwan.waidyanatha@rezgateway.com>
                    <ushan.jayasuriya@colombo.rezgateway.com>
    '''
    ''' --- DATA --- '''
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
            if self._data is not None and not isinstance(self._data,DataFrame):
                self._data = clsSFile.session.createDataFrame(self._data)
                logger.debug("%s converted non pyspark data object to %s",
                             __s_fn_id__,type(self._data))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):

        __s_fn_id__ = f"{self.__name__} function <@setter data>"

        try:
            if data is None:
                raise AttributeError("Invalid data attribute, must be a valid pyspark dataframe")
            if not isinstance(data,DataFrame):
                self._data = clsSFile.session.createDataFrame(data)
                logger.debug("%s converted %s object to %s",
                             __s_fn_id__,type(data),type(self._data))
            else:
                self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' --- BOW --- '''
    @property
    def bow(self):
        """
        Description:
            bow @property and @setter functions. make sure it is a valid dict
        Attributes:
            bow in @setter will instantiate self._bow  
        Returns (dict) self._bow
        """

        __s_fn_id__ = f"{self.__name__} function <@property bow>"

        try:
            if not isinstance(self._bow,dict):
                raise AttributeError("bow is not a valid dict, % unacceptable" % type(self._bow))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._bow

    @bow.setter
    def bow(self,_bow):

        __s_fn_id__ = f"{self.__name__} function <@setter bow>"

        try:
            if bow is None or bot isinstance(bow,dict):
                raise AttributeError("Cannot set bow class property with %s" % type(bow))

            self._bow = bow

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._bow


    ''' --- CORPUS --- '''
    @property
    def corpus(self):
        """
        Description:
            corpus @property and @setter functions. make sure it is a valid list
        Attributes:
            corpus in @setter will instantiate self._corpus    
        Returns (dataframe) self._data
        """

        __s_fn_id__ = f"{self.__name__} function <@property corpus>"

        try:
            if self._corpus is not None and not isinstance(self._corpus,list):
                raise AttributeError("corpus is not a valid list, % unacceptable" % type(self._corpus))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._corpus

    @corpus.setter
    def corpus(self,corpus):

        __s_fn_id__ = f"{self.__name__} function <@setter corpus>"

        try:
            if corpus is None and not is instance(corpus,list):
                raise AttributeError("Cannot set bow class property with %s" % type(corpus))
            self._corpus = corpus

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._corpus


    ''' Function --- READ BOW FILE ---

            author: <ushan.jayasuriya@colombo.rezgateway.com>
    '''

    def read_bow_from_file(
        self,
        file_name: str,
        file_path: str,
        **kwargs):
        """
        Description:

        Arguments:

        Returns: self._data (dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <read_bow_from_file>"

        ''' declare variables & default values '''

        try:
            ''' validate input attributes '''
            
            ''' read bow from file '''
            self._bow = {"hello" : "world"}
            
            ''' << ADD THE LOGIC HERE >> '''

            ''' check if function produced a valid return object '''
            if self._bow is None or len(self._bow) <= 0:
                raise RuntimeError("retuned an empty %s bow dict",
                                   type(self._bow))
            logger.debug("%s loaded corpus with %d rows",__s_fn_id__,len(self._bow))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._bow

    ''' Function --- READ BOW FILE ---

            author: <ushan.jayasuriya@colombo.rezgateway.com>
    '''

    def write_bow_to_file(
        self,
        bow : dict,
        file_name: str,
        file_path: str,
        **kwargs):
        """
        Description:

        Arguments:

        Returns: self._bow (dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <write_bow_to_file>"

        ''' declare variables & default values '''

        try:
            ''' validate input attributes '''
            
            ''' << ADD THE LOGIC HERE >> '''

            ''' write bow from file '''
            self._bow = {"hello" : "world"}
            
            ''' check if function produced a valid return object '''
            if self._bow is None or len(self._bow) <= 0:
                raise RuntimeError("retuned an empty %s bow dict",
                                   type(self._bow))
            logger.debug("%s loaded corpus with %d rows",__s_fn_id__,len(self._bow))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._bow


    ''' Function --- READ BOW FILE ---

            author: <ushan.jayasuriya@colombo.rezgateway.com>
    '''

    def update_bow(
        self,
        corpus: list,
        bow : dict,
        file_name: str,
        file_path: str,
        **kwargs):
        """
        Description:

        Arguments:

        Returns: self._bow (dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <update_bow>"

        ''' declare variables & default values '''

        try:
            ''' validate input attributes '''
            self.corpus = corpus
            
            ''' << ADD THE LOGIC HERE >> '''

            ''' update bow from file '''
            self._bow = {"hello" : "world"}
            
            ''' check if function produced a valid return object '''
            if self._bow is None or len(self._bow) <= 0:
                raise RuntimeError("retuned an empty %s bow dict",
                                   type(self._bow))
            logger.debug("%s loaded corpus with %d rows",__s_fn_id__,len(self._bow))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._bow
