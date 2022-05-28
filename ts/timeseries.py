#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS with essential timeseries properties and methods:
        1) Remove empty columns and rows
        2) Covert units from a given unit to a desired unit
        3) Clean categorical data by removing leading and tailing spaces and characters
'''
class TimeSeries():
    ''' Function
            name: __init__
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 

            return DataFrame

    '''
    def __init__(self, name : str="data", **ts_param:dict):

        self.name = name

        print(params)
        return None

    def get_data_types(self,
                       df     # valid pandas dataframe
                      ):
        import traceback
        import pandas as pd
        
        ''' Initialize the return dict '''
        stat_dict = {}

        try:
            if not isinstance(df,pd.DataFrame):
                raise ValueError("data is an invalid pandas DataFrame")

            ''' Central Tendency mode, median, and mean'''
            stats_df =  df.copy()
            stat_keys = list(stats_df.columns)
            var_vals = list(stats_df.dtypes)
            var_vals = [str(v) for v in var_vals]
            stat_dict.update({"Data Types" : dict(zip(stat_keys,var_vals))})

        except Exception as err:
            _s_fn_id = "Class <DataStatistics> Function <get_data_types>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())
        return stat_dict
