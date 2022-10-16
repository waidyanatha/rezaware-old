#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
#    from numpy import isin
#    import numpy as np
#    from datetime import datetime, timedelta, date, timezone
    import pandas as pd
    from pandas.api.types import is_datetime64_any_dtype as is_datetime
    import calendar
    import traceback

    print("All packages in OTAWebScraper loaded successfully!")

except Exception as e:
    print("Some packages didn't load\n{}".format(e))

'''
    CLASS to enrich the data after the data has been extracted and cleaned:
        1) 

    Contributors:
        * nuwan.waidyanatha@rezgateway.com
'''
class DataEnrichment():
    ''' Function
            name: __init__
            parameters:
                    @name (str)
                    @enrich (dict)
            procedure: 

            return DataFrame

    '''
    def __init__(self, name : str="data", **kwargs:dict):

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

        ''' Dictionary of column augmentations '''
        self._aug_dict = { "DateTime" :
                          ["ALL",     # apply all cleaning processes and will ignore other flags
                           "DOW",     # add a day of week column
                           "YYYY",    # add a cloumn with the year only
                           "YY",      # add a cloumn with the year only
                           "MMM",     # add a column with three letter month name
                           "MM",      # add a column with two digit month number
                           "MMM-DD",  # add a column with three letter month and two digit day
                           "HH-mm",   # add a column with two digit hour and two digit minute
                          ]
                         }

        ''' default datetime column name when applying date and time related column augmentations '''
        self._date_col_name = "DateTime"
        self._cols_to_augment_dict = { "DateTime" : ["ALL"] }
#        self._l_dt_aug_cols = ["ALL"]
        self._kwargs = {"dt_col_name" : "DateTime",   # identifies the datetime column in the dataframe
                        "pad_lead_zeros" : False,     # pad the numeric datetime values with leading zeros (e.g. Jan = 01)
                        "abbreviate" : False,         # truncates the string to give 3 character names for year, month, dow 
                        "sorted_by" : ["YYYY","MM","DD"]   # sort dataframe by Year, Month, and Day
                       }

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
            name: get_enriched_data
            procedure: 

            return DataFrame

    '''
    def get_enriched_data(self,
                          df,
                          col_augment_dict : dict,
                          **kwargs):

#        import traceback
#        import pandas as pd
#        from pandas.api.types import is_datetime64_any_dtype as is_datetime
        
        ''' Exception text header '''
        _s_fn_id = "Class <DataEnrichment> Function <get_enriched_data>"

        ''' Initialize the return DataFrame '''
        _dt_aug_df = pd.DataFrame([])

        _enrich_action_map = {'DateTime': self.get_dt_augmentations,
                              #'Text': self.get_str_augmentations,
                             }


        try:
            if not isinstance(df, pd.DataFrame):
                raise ValueError("data is an invalid pandas DataFrame")

            if not isinstance(col_augment_dict,dict):
                print("[WARNING] {} Unreconized column_augment_list {}"
                      .format(_s_fn_id, err,column_augment_list))
                print("Using default value {}".format(self._cols_to_augment_dict))
            else:
                self._cols_to_augment_dict = col_augment_dict

            if isinstance(kwargs, dict):
                self._kwargs = kwargs

            if "dt_col_name" in self._kwargs.keys():
                self._date_col_name = self._kwargs["dt_col_name"]
                if not is_datetime(df[self._date_col_name]):
                    raise ValueError("{} column is an invalid datetime".format(df[self._date_col_name]))

            ''' loop through dictionary of column augmentations '''
            for key in list(self._cols_to_augment_dict.keys()):
                if "DateTime" in key:
                    _dt_aug_df = self.get_dt_augmentations(df[[self._date_col_name]],                # pass only the datetime col 
                                                           self._cols_to_augment_dict["DateTime"],   # list of dt columns
                                                           **self._kwargs)
                if "Text" in key:
                    pass

            print("Columns to augment to dataframe:\n",self._cols_to_augment_dict)

            ''' if sorted **kwarg is provided then sort by the given columns 
                if the columns are not in dataframe the aument them and remove before returning dataframe '''

        except Exception as err:
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _dt_aug_df


    ''' Function
            name: get_YYYY
            parameters:
                _dt_df - 
                kwargs - 
            return dataframe (mm_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    @staticmethod
    def get_YYYY(self,_dt_df, **kwargs):

#        import pandas as pd

        ''' single column dataframe with datetime values'''
        yyyy_df = pd.DataFrame([])
        dt_col_name =  _dt_df.columns[0]
        yyyy_df["YYYY"] = _dt_df[dt_col_name].dt.year

        return yyyy_df


    ''' Function
            name: get_MM
            parameters:
                _dt_df - 
                kwargs - 
            return dataframe (mm_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    @staticmethod
    def get_MM(self,_dt_df, **kwargs):

#        import pandas as pd

        ''' single column dataframe with datetime values'''
        mm_df = pd.DataFrame([])
        dt_col_name =  _dt_df.columns[0]
        mm_df["MM"] = _dt_df[dt_col_name].dt.month
        ''' pad with leading zero to support sorting and formatting '''
        if kwargs["pad_lead_zeros"]:
            mm_df["MM"] = mm_df["MM"].apply(lambda x: '{0:0>2}'.format(x))

        return mm_df


    ''' Function - 
            name: get_MMM
            procedure: Get both the Month name in full or the abbreviation to 3 characters

            return DataFrame

    '''
    @staticmethod
    def get_MMM(self,_dt_df, **kwargs):

#        import pandas as pd
#        import calendar

        ''' single column dataframe with datetime values'''
        mmm_df = pd.DataFrame([])
        dt_col_name =  _dt_df.columns[0]
        mmm_df["MM"] = self.get_MM(self,_dt_df,**{"pad_lead_zeros" : False})
        if kwargs["abbreviate"]:
            mmm_df["MMM"] = mmm_df['MM'].apply(lambda x: calendar.month_abbr[x]).replace(inplace=False)
        else:
            mmm_df["MMM"] = mmm_df['MM'].apply(lambda x: calendar.month_name[x]).replace(inplace=False)

        return mmm_df["MMM"]


    ''' Function - 
            name: get_DD
            procedure: 

            return DataFrame

    '''
    @staticmethod
    def get_DD(self,_dt_df, **kwargs):

#        import pandas as pd

        ''' single column dataframe with datetime values'''
        dd_df = pd.DataFrame([])
        dt_col_name =  _dt_df.columns[0]
        dd_df["DD"] = _dt_df[dt_col_name].dt.day

        if kwargs["pad_lead_zeros"]:
            dd_df["DD"] = dd_df["DD"].apply(lambda x: '{0:0>2}'.format(x))

        return dd_df["DD"]


    ''' Function - 
            name: get_MMM_DD
            procedure: 

            return DataFrame

    '''
    @staticmethod
    def get_MMM_DD(self,_dt_df, **kwargs):

#        import pandas as pd

        ''' single column dataframe with datetime values'''
        mmm_dd_df = pd.DataFrame([])
        dt_col_name =  _dt_df.columns[0]
        mmm_dd_df["DD"] = self.get_DD(self,_dt_df,**{"pad_lead_zeros" : True})
        mmm_dd_df["MM"] = self.get_MM(self,_dt_df,**{"pad_lead_zeros" : True})
        mmm_dd_df["MMM"] = self.get_MMM(self,_dt_df,**{"abbreviate" : True})
        mmm_dd_df["MMM-DD"] = "["+mmm_dd_df["MM"]+"]"+mmm_dd_df["MMM"]+"-"+mmm_dd_df["DD"]
        
        return mmm_dd_df["MMM-DD"]


    ''' Function
            name: get_DOW
            parameters:
                _dt_df - 
                kwargs - 
            return dataframe (dow_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    @staticmethod
    def get_DOW(self,_dt_df, **kwargs):

#        import pandas as pd

        ''' single column dataframe with datetime values'''
        dow_df = pd.DataFrame([])
        dt_col_name =  _dt_df.columns[0]
        dow_df["DOW"] = _dt_df[dt_col_name].dt.day_name()

        ''' if abbreviation is true truncate to first three letters '''
        if kwargs["abbreviate"]:
            dow_df["DOW"] = dow_df["DOW"].str[:3]
        
        return dow_df


    ''' Function
            name: get_HH_mm
            parameters:
                _dt_df - 
                kwargs - 
            return dataframe (hhmm_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    @staticmethod
    def get_HH_mm(self,_dt_df, **kwargs):

#        import pandas as pd

        ''' single column dataframe with datetime values'''
        hhmm_df = pd.DataFrame([])
        dt_col_name =  _dt_df.columns[0]
        hhmm_df["HH:m"] = _dt_df[dt_col_name].dt.strftime('%H:%M')

        return hhmm_df


    ''' Function - 
            name: get_dt_augmentations
            procedure: 

            return DataFrame

    '''
    def get_dt_augmentations(self,
                             _dt_df,
                             dt_list: list,
                             **kwargs):

        _dt_action_map = {'DOW': self.get_DOW,
                          'DD' : self.get_DD,
                          'YYYY': self.get_YYYY,
                          "MMM" : self.get_MMM,
                          "MM" : self.get_MM,
                          "MMM-DD" : self.get_MMM_DD,
                          "HH-mm" : self.get_HH_mm,
                         }

        _augmented_dt_df = _dt_df.copy()
        
        ''' single column dataframe with datetime values'''
        results = {}

        for action in dt_list:
            results[action] = _dt_action_map[action](self,_dt_df,**kwargs)
            _augmented_dt_df[action]=results[action]
            
        return _augmented_dt_df


    ''' Function - 
            name: get_unit_converted_data (public)
            procedure: specify the 

            return DataFrame

    '''
    def get_unit_converted_data(self,
                                df,
                                data_catalog, 
                                cols, # : list=["all"],
                                units, # : dict= self.convertions,
                                **kwargs):
        pass

    ''' Function - 
            name: convert_currency
            procedure: 

            return DataFrame

    '''
    def convert_currency(self,df,
                         from_currency, # : str= self.to_currency,
                         to_currency, # : str= self.to_currency,
                         **kwargs):
        pass

    ''' Function - 
            name: convert_imperial_to_metric
            procedure: 

            return DataFrame

    '''
    def convert_imperial_to_metric(self,
                                   df,
                                   from_unit_type, #: str= self.from_unit,
                                   to_unity_type, # : str= self.to_unit,
                                   **kwargs):
        pass

    ''' Function - 
            name: drop_empty_cols
            procedure: 

            return DataFrame

    '''
    ''' Function - 
            name: set_multi_currency
            procedure: 

            return DataFrame

    '''
    def set_multi_currency(self,df,
                           currency_list : list,
                           **kwargs):
        pass


    ''' Function - 
            name: set_imputation
            procedure: 

            return DataFrame

     '''
    def set_imputation(self,
                       df,
                       method : str,
                       **kwargs):
        pass