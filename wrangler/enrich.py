#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS to enrich the data after the data has been extracted and cleaned:
        1) 
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

        ''' Dictionary of column aumnetations '''
        self._aug_dict = { "DateTime" :
                          ["ALL",     # apply all cleaning processes and will ignore other flags
                           "DOW",     # add a day of week column
                           "YYYY",    # add a cloumn with the year only
                           "YY",      # add a cloumn with the year only
                           "MMM",     # add a column with three letter month name
                           "MM",      # add a column with two digit month number
                           "MMM-DD",  # add a column with three letter month and two digit day
                          ]
                         }

#        ''' list of datetime column augmentations '''
#        self._aug_col_dt = ["ALL",     # apply all cleaning processes and will ignore other flags
#                            "DOW",     # add a day of week column
#                            "YYYY",    # add a cloumn with the year only
#                            "YY",      # add a cloumn with the year only
#                            "MMM",     # add a column with three letter month name
#                            "MM",      # add a column with two digit month number
#                            "MMM-DD",  # add a column with three letter month and two digit day
#                           ]

        self.name = name
        self._cols_to_augment_dict = { "DateTime" : ["ALL"] }
#        self._l_dt_aug_cols = ["ALL"]

        print("Initialing DataEnrichment class for ",self.name)
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

        import traceback
        import pandas as pd
        
        ''' Exception text header '''
        _s_fn_id = "Class <DataEnrichment> Function <get_enriched_data>"

        ''' Initialize the return DataFrame '''
        _return_df = pd.DataFrame([])

        try:
            if not isinstance(df,pd.DataFrame):
                raise ValueError("data is an invalid pandas DataFrame")

            if not isinstance(col_augment_dict,dict):
                print("[WARNING] {} Unreconized column_augment_list {}"
                      .format(_s_fn_id, err,column_augment_list))
                print("Using default value {}".format(self._cols_to_augment_dict))
            else:
                self._cols_to_augment_dict = col_augment_dict

            aug_keys = list(self._cols_to_augment_dict.keys())
            for key in aug_keys:
                if "DateTime" in key:
                    _l_dt_augs = self._cols_to_augment_dict["DateTime"]
                    _dt_aug_df = self.get_dt_augmentations(df,_l_dt_augs)
                elif "Text" in key:
                    pass
            else:
                raise ValueError("Nothing to do")
            print(self._cols_to_augment_dict)

        except Exception as err:
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _return_df

    ''' Function - 
            name: get_dt_augmentations
            procedure: 

            return DataFrame

    '''
    def get_dt_augmentations(self,
                             df,
                             dt_list: list,
                             **kwargs):
        pass


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