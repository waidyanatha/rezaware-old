#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

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

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def __init__(self, name : str="data", **clean:dict):

        ''' list of possible cleaning processes '''
        self._clean_procs = ["all",               # apply all cleaning processes and will ignore other flags
                             "no_empty_rows",     # remove rows if all values are NULL
                             "no_empty_cols",     # remove columns if all values are NULL
                             "no_double_quotes",  # remove double quotes from from strings
                             "no_head_spaces",    # remove leading spaces in data element
                             "no_tail_spaces",    # remove end spaces in data element
                             "impute",            # impute empty cells with synthesized data
                            ]

        self.name = name
#        self._clean_procs = ["all"]

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
