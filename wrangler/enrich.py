#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS to enrich the data after the data has been extracted and cleaned:
        1) Parse date time stamps to add new columns with YYYY, QTR, MMM, etc
        2) Map location data to Lx administrative boundaries and Points of Interest (PoC)
        3) Impute data to fill in empty cells based on a given algorithm
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
    def __init__(self, name : str="data", **clean:dict):

        ''' list of possible cleaning processes '''
        self._entich_procs = ["all",     # apply all cleaning processes and will ignore other flags
                              "dow",     # add a day of week column
                              "yyyy",    # add a cloumn with the year only
                              "mmm",     # add a column with three letter month name
                              "mm",      # add a column with two digit month number
                            ]

        self.name = name
        self._clean_procs = ["all"]

        print(params)
        return None


    ''' Function - 
            name: get_enriched_data
            procedure: 

            return DataFrame

    '''
    def get_enriched_data(self,
                          df,
                          enrich_scope : str= self.clean_scope,
                          **kwargs):
        pass

    ''' Function - 
            name: set_multi_currency
            procedure: 

            return DataFrame

    '''
    def set_multi_currency(self,df,
                           currency_list : list = self._multi_currencies,
                           **kwargs):
        pass




    ''' Function - 
            name: set_imputation
            procedure: 

            return DataFrame

     '''
    def set_imputation(self,
                       df,
                       method : str = self._impute_method,
                       **kwargs):
        pass