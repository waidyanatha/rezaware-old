#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

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
                    @name (str)
                    @clean (dict)
            procedure: 

            return DataFrame

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
        self._clean_procs = ["all"]

        print(params)
        return None


    ''' Function - 
            name: convert_currency
            procedure: 

            return DataFrame

    '''
    def get_cleaned_data(self,
                         df,
                         clean_scope : str= self.clean_scope,
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
                                cols : list=["all"]
                                units : dict= self.convertions,
                                **kwargs):
        pass

    ''' Function - 
            name: convert_currency
            procedure: 

            return DataFrame

    '''
    def convert_currency(self,df,
                         from_currency : str= self.to_currency,
                         to_currency : str= self.to_currency,
                         **kwargs):
        pass

    ''' Function - 
            name: convert_impertial_to_metric
            procedure: 

            return DataFrame

    '''
    def convert_impertial_to_metric(self,
                                    df,
                                    from_unit_type: str= self.from_unit,
                                    to_unity_type : str= self.to_unit,
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
