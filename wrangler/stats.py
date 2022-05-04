#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS to evaluate the statistical characteristics in the data before data discovery and exploratory data analysis
    it will allow for looking at the basis statistical properties such as:
        1) Data types of all the data attributes
        2) Existence of NaN (NULL) values and empty columns
        3) Central tendancy: median, mode, mean, standrd deviation
        4) Uniformity test (e.g. kolmogorov smirnov test)
'''
class DataStatistics():
    ''' Function
            name: __init__
            parameters:
                    @name
                    @params
            procedure: 

            return DataFrame

    '''
    def __init__(self, name : str="data", **params:dict):

        self._l_dup_methods = ["rows","columns"]
        self._dist_types = ['norm',        # bell curve that occurs naturally in many situations
                            'genextreme',  #
                            'expon',       #
                            'gamma',       # models the amount of time before the k-th event in a Poisson process,
                            'pareto',      #
                            'lognorm',     # normally distributed logarithm
                            'dweibull'     # 
                            'beta',        # way to represent outcomes for percentages or proportions
                            'burr',        # a unimodal family of distributions
                            't',           #
                            'uniform',
                          ]
        
        self.name = name
        self._dup_method = ["rows"]
        self._distributions = ["popular"]
        
        return None


    ''' Function - 
            name: get_data_types
            procedure:

            return DataFrame

            TODO: enhance to give a list of variables to get their respective data types
    '''
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

    ''' Function - 
            name: get_null_counts
            procedure: 

            return DataFrame

            __TODO__
            1. Return the number of Null values for a list of Columns
            2. Check for other Null possibilities like empty strings or control characters (e.g. /t or /n)
            3. Return the row index and column name or indext with Null values

    '''
    def count_nulls(self,
                    df     # valid pandas dataframe
                   ):
        import traceback
        import pandas as pd

        ''' Initialize the return dict '''
        stat_dict = {}

        try:
            if not isinstance(df,pd.DataFrame):
                raise ValueError("data is an invalid pandas DataFrame")

            ''' Count number of NaN values for each column '''
            stats_df =  df.copy()
            stat_keys = list(stats_df.columns)
            var_vals = list(stats_df.isna().sum())
            stat_dict.update({"NaN Counts" : dict(zip(stat_keys,var_vals))})

        except Exception as err:
            _s_fn_id = "Class <DataStatistics> Function <count_nulls>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return stat_dict

    ''' Function - 
            name: count_duplicates
            procedure: 

            return DataFrame

        __TODO__
        1. Complete returning the number of emply columns
        2. Return number of duplicate rows for selected columns
        3. Return number of duplicate columns for selected rows
        4. Return the row index of duplicate rows
        5. Return the column names or index of empty duplicate columns
    '''
    def count_duplicates(self,
                         df,     # valid pandas dataframe
                         _l_method = ['rows'],
                         #_l_col_name
                        ):
        import traceback
        import pandas as pd

        ''' Initialize return dictionary ''' 
        stat_dict = {}
        _dup_count = 0

        try:
            if not isinstance(df,pd.DataFrame):
                raise ValueError("data is an invalid pandas DataFrame")
            if isinstance(_l_method,list) and [i for i in _l_method if i not in self._l_dup_methods]:
                raise ValueError("Duplicate check method invalid; should be",self._l_dup_methods)

            stats_df =  df.copy()

            for meth_val in _l_method:
                print(meth_val)
                if meth_val == "rows":
                    ''' Count duplicates for all rows '''
                    _dup_count = len(stats_df)-len(stats_df.drop_duplicates())
                    stat_dict.update({"Number of Duplicate Rows" : _dup_count})
                elif meth_val == "columns":
                    stat_dict.update({"Number of Duplicate columns" : _dup_count})
                else:
                    raise ValueError("Method is invalid. it must be",_l_method)

        except Exception as err:
            _s_fn_id = "Class <DataStatistics> Function <count_duplicates>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return stat_dict

    ''' Function - 
            name: get_central_tendency
            procedure: 

            return DataFrame

    '''
    def get_central_tendency(self,
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
            var_vals = list(stats_df.mode(axis=0, numeric_only=True, dropna=True))
            stat_dict.update({"Mode" : dict(zip(stat_keys,var_vals))})
            var_vals = list(round(stats_df.median(axis=0, numeric_only=True, skipna=True),4))
            stat_dict.update({"Median" : dict(zip(stat_keys,stat_keys))})
            var_vals = list(round(stats_df.mean(axis=0, numeric_only=True, skipna=True),4))
            stat_dict.update({"Mean" : dict(zip(stat_keys,var_vals))})
            var_vals = list(round(stats_df.var(axis=0, numeric_only=True, skipna=True),4))
            stat_dict.update({"Variance" : dict(zip(stat_keys,var_vals))})
            var_vals = list(round(stats_df.std(axis=0, numeric_only=True, skipna=True),4))
            stat_dict.update({"Standard Deviation" : dict(zip(stat_keys,var_vals))})

        except Exception as err:
            _s_fn_id = "Class <DataStatistics> Function <get_central_tendency>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return stat_dict

    ''' Function - 
            name: get_uniformity
            procedure:

            return DataFrame

    '''
    def test_uniformity(self,
                        df,     # valid pandas dataframe
                        test_method = "KS-test",
                       ):
        import traceback
        import pandas as pd
        import numpy as np
        from scipy import stats
        
        ''' Initialize the return dict '''
        stat_dict = {}

        try:
            if not isinstance(df,pd.DataFrame):
                raise ValueError("data is an invalid pandas DataFrame")

            ''' kolmogorov smirnov uniformity test for each column '''
            stats_df =  df.copy()
            stat_keys = list(stats_df.columns)
            _l_key = []
            _l_val_s = []
            _l_val_p = []
            _l_num_cols=stats_df.select_dtypes(include=np.number).columns.tolist()
            for col in _l_num_cols:
                _stat, _p_val = stats.kstest(stats_df[col].values, 'norm')
                _l_key.append(col)
                _l_val_s.append(round(_stat,4))
                _l_val_p.append(round(_p_val,4))
                stat_dict.update({"KS Test Statistic" : dict(zip(_l_key,_l_val_s))})
                stat_dict.update({"KS Test P-value" : dict(zip(_l_key,_l_val_p))})

        except Exception as err:
            _s_fn_id = "Class <DataStatistics> Function <eval_uniformity>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return stat_dict

    ''' Function - 
            name: get_distributions
            procedure: will apply the PyPi distfit function to evaluate the Probability distributions of every
                numerical attribute column.
                    method
                    RSS
                    shape
                    loc
            Ref: https://www.statisticshowto.com/probability-and-statistics/statistics-definitions/probability-distribution/

            return DataFrame of the 

    '''
    def fit_distributions(self,
                          df,     # valid pandas dataframe
                          dist_type_list="popular",
                         ):
        import traceback
        import pandas as pd
        import numpy as np
        from distfit import distfit

        disfit_summary_df = pd.DataFrame([])
        try:
            if not isinstance(df,pd.DataFrame):
                raise ValueError("data is an invalid pandas DataFrame")
                
            elif "full" in dist_type_list:
                ''' use all and ignore the rest '''
                dist = distfit(distr='full')
            elif "popular" in dist_type_list:
                ''' use popular and ignore the rest '''
                dist = distfit(distr='popular')
            elif isinstance([i for i in dist_type_list if i in self._dist_types],list):
                dist = distfit(distr=dist_type_list)
            else:
                raise ValueError("Parameter dist_type_list is an invalid. compare with", self._dist_types)

            ''' Initialize dataframe '''
            stats_df =  df.copy()
            stats_df.replace("", np.nan, inplace=True)
            stats_df.dropna(how='all', axis=1, inplace=True)

            ''' list of all the numerical data columns '''
            _l_num_cols=stats_df.select_dtypes(include=np.number).columns.tolist()
            for col in _l_num_cols:
                _col_vals = stats_df[col].values
                ''' initialize distfit'''
                f = dist.fit_transform(_col_vals)
                ''' fliter the summary from the dictionary '''
                filtered_dict = {k:v for k,v in f.items() if "summary" in k}
                for dist_model, summary_info in filtered_dict.items():
                    summary_info["column name"]= col
                    ''' add the summary dataframe for each data column '''
                    disfit_summary_df=disfit_summary_df.append(summary_info)

        except Exception as err:
            _s_fn_id = "Class <DataStatistics> Function <get_distributions>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return disfit_summary_df
