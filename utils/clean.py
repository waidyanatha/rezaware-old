#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "clean"
__package__ = "DataClensing"
# __root_dir__ = "/home/nuwan/workspace/rezgate/wrangler/"
__utils_dir__ = '/home/nuwan/workspace/rezgate/utils/'
#__data_dir__ = 'data/hospitality/bookings/scraper/'
__conf_fname__ = 'app.cfg'
__logs_dir__ = 'logs/'
__log_fname__ = 'app.log'

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    from numpy import isin
    import numpy as np
    from datetime import datetime, timedelta, date, timezone
    import pandas as pd
    import traceback

    print("All packages in DataClensing loaded successfully!")

except Exception as e:
    print("Some packages didn't load\n{}".format(e))

'''
    CLASS to clean up the data after extraction and staged in a desired file format:
        1) Remove empty columns and rows
        2) Covert units from a given unit to a desired unit
        3) Clean categorical data by removing leading and tailing spaces and characters
'''
class DataClensing():
    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author(s): nuwan.waidyanatha@rezgateway.com
    '''
    def __init__(self,
                 desc : str="cleansing workloads",   # identifier for the instances
                 **clean:dict):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__desc__ = desc
        _s_fn_id = "Function <__init__>"

        try:
            ''' initiate to load app.cfg data '''
            global confUtil
            confUtil = configparser.ConfigParser()

            ''' Set the utils root directory, it can be changed using kwargs '''
            self.utilsDir = __utils_dir__
            if "UTILS_DIR" in kwargs.keys():
                self.utilsDir = kwargs['UTILS_DIR']
            ''' load the utils config env vars '''
            self.utilConfFPath = os.path.join(self.utilsDir, __conf_fname__)
            confUtil.read(self.utilConfFPath)

            ''' get the file and path for the logger '''
            self.logDir = os.path.join(self.utilsDir,confUtil.get('LOGGING','LOGPATH'))
            if not os.path.exists(self.logDir):
                os.makedirs(self.logDir)
            self.logFPath = os.path.join(self.logDir,confUtil.get('LOGGING','LOGFILE'))
            ''' innitialize the logger '''
            global logger
            logger = logging.getLogger(__package__)
            logger.setLevel(logging.DEBUG)
            if (logger.hasHandlers()):
                logger.handlers.clear()
            # create file handler which logs even debug messages
            fh = logging.FileHandler(self.logFPath, confUtil.get('LOGGING','LOGMODE'))
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info(__name__)
            logger.info('Utils Path = %s', self.utilsDir)

            ''' set tmp storage location from app.cfg '''
            self.tmpDIR = os.path.join(self.utilsDir,confUtil.get('STORES','TMPDATA'))
            ''' override if defined in kwargs '''
            if "TMP_DIR" in kwargs.keys():
                self.tmpDIR = kwargs['TMP_DIR']
            if not os.path.exists(self.tmpDIR):
                os.makedirs(self.tmpDIR)

            ''' list of possible cleaning processes '''
            self._clean_procs = ["all",               # apply all cleaning processes and will ignore other flags
                                 "no_empty_rows",     # remove rows if all values are NULL
                                 "no_empty_cols",     # remove columns if all values are NULL
                                 "no_double_quotes",  # remove double quotes from from strings
                                 "no_head_spaces",    # remove leading spaces in data element
                                 "no_tail_spaces",    # remove end spaces in data element
                                 "impute",            # impute empty cells with synthesized data
                                ]

            logger.info("Connection complete! ready to load data.")
            print("Initialing %s class for %s with instance %s"
                  % (self.__package__, self.__name__, self.__desc__))
            print("Logging %s info, warnings, and error to %s" % (self.__package__, self.logFPath))

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None

    ''' Function - 
            name: convert_currency
            procedure: 

            return DataFrame

    '''
    def get_cleaned_data(self,
                         df,
                         clean_scope, # : str= self._clean_procs,
                         **kwargs):
        pass

    def drop_empty_cols(self,df,**kwargs):
        ''' Drop empty columns and column with all zeros '''
        stats_df = data_df.copy()
        stats_df.replace("", np.nan, inplace=True)
        stats_df.dropna(how='all', axis=1, inplace=True)
        stat_keys = list(stats_df.columns)

        pass

    ''' Function - 
            name: drop_empty_rows
            procedure: 

            return DataFrame

    '''
    def drop_empty_rows(self,df,**kwargs):
        pass

    ''' Function
            name: drop_duplicates
            parameters:
                df - dataframe with all the data
                kwargs - define whether to drop duplocate coloums or rows
                        and the list of rows to consider to validate duplication
            return dataframe (_return_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def drop_duplicates(self,df,**kwargs):

        _return_df = pd.DataFrame()   # initialize return var
        _col_list = []   # initialize column list
        _row_index_list = []   # initiatlize row index list

        try:
            if df.shape[0] <= 0:
                raise ValueError("No data in the dataframe")
            _return_df = df.copy()
            
            ''' if columns are listed, drop them '''
            if "columns" in kwargs.keys():
                _col_list = kwargs['columns']
            else:
                _col_list = _return_df.columns
            ''' verify columns are in dataframe '''
            if (set(_col_list).issubset(set(_return_df.columns))):
                _return_df.drop_duplicates(_col_list, inplace=True, ignore_index=True)
            else:
                raise ValueError("List of columns don't match the dataframe columns")

            ''' if row index is given drop them '''
            if "row_index" in kwargs.keys():
                _row_index_list = kwargs['row_index']
                # COMPLETE THIS
        
        except Exception as err:
            _s_fn_id = "Class <DataClensing> Function <drop_duplicates>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _return_df

    ''' Function
            name: drop_duplicates
            parameters:
                df - dataframe with all the data
                kwargs - define whether to drop duplocate coloums or rows
                        and the list of rows to consider to validate duplication
            return dataframe (_return_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def remove_strings(self,df, remove_list:list, **kwargs):
        except Exception as err:
            _s_fn_id = "Class <DataClensing> Function <drop_duplicates>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _return_df