#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS with essential timeseries evaluation properties and methods:
        1) 
'''
class TSEvaluation():
    ''' Function
            name: __init__
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 

            return DataFrame

    '''
    def __init__(self, name : str="data"):

        self.name = name
        ''' Paramenter default values '''
        self.days_offset = 0     # start window at minimum date point
        self.window_length = 7   # window length set to 7 days
        self.p_val = 1.0         # default null hypothesis testing & returns all results < p_val cutt off
        self.evaluation_methods = ["fisher-exact-test",   # 
                                   "chi-sqaure-test",     #  
                                   "granger-causality-test",   #  
                                  ]

        return None

    ''' Function
            name: get_fisher_exact
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 

            return DataFrame

    '''
    def get_fisher_exact(self,
                         curr_inner_df : list,
                         base_inner_df : list,
                         curr_outer_df : list,
                         base_outer_df : list):

        from scipy.stats import fisher_exact, chisquare
        from statistics import mean

        lu_sum = sum(curr_inner_df)
        lu_mean = mean(curr_inner_df)
        ll_sum = sum(base_inner_df)
        ll_mean = mean(base_inner_df)
        ru_sum = sum(curr_outer_df)
        ru_mean = mean(curr_outer_df)
        rl_sum = sum(base_outer_df)
        rl_mean = mean(base_outer_df)
#        lu_sum = curr_inner_df["RoomNights"].sum()
#        lu_mean = curr_inner_df["RoomNights"].mean()
#        ll_sum = base_inner_df["RoomNights"].sum()
#        ll_mean = base_inner_df["RoomNights"].mean()
#       ru_sum = curr_outer_df["RoomNights"].sum()
#        ru_mean = curr_outer_df["RoomNights"].mean()
#        rl_sum = base_outer_df["RoomNights"].sum()
#        rl_mean = base_outer_df["RoomNights"].mean()

#        oddsr, p = fisher_exact([[lu_sum,ru_sum],[ll_sum,rl_sum]], alternative='two-sided')

        oddsr, p = fisher_exact([[lu_mean,ru_mean],[ll_mean,rl_mean]], alternative='two-sided')

        fisher_stats_dict = {"stat" : oddsr, "p_value" : p}

        return fisher_stats_dict

    ''' Function
            name: get_ts_eval_stats
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 

            return DataFrame

    '''
    def get_ts_eval_stats(self,
#                          df,        # valid pandas dataframe with baseline and current dates & counts
                          baseline_df,   # valid pandas dataframe with baseline retrospective data
                          prospect_df,   # valid pandas dataframe with post-baseline prospective data
#                          base_yr,   # baseline year
#                          curr_yr,   # current year
                          counts_col, # column name containing the time series counts
                          eval_meth,  # contains the evaluation methods
                          **eval_params,
                         ):
        import traceback
        import pandas as pd
        import datetime
        from datetime import timedelta, date
        
        ''' Initialize the return dict '''
        columns = ["date","evaluation"]
        stats_df = pd.DataFrame([],columns = columns)

        try:
#            if not isinstance(df,pd.DataFrame):
#                raise ValueError("Data is an invalid pandas DataFrame")

            if not isinstance(baseline_df,pd.DataFrame) or not baseline_df.shape[0] > 0:
                raise ValueError("Baseline data is an invalid pandas DataFrame")

            if not baseline_df.shape[0] > 0:
                raise ValueError("There is no baseline data")

            if not isinstance(prospect_df,pd.DataFrame):
                raise ValueError("Post-baseline data is an invalid pandas DataFrame")

            if not prospect_df.shape[0] > 0:
                raise ValueError("There is no post-baseline data")

            if baseline_df.shape[0] != prospect_df.shape[0]:
                raise ValueError("Baseline %d and Post-baseline %d have mismatching data sizes"
                                 % (baseline_df.shape[0],prospect_df.shape[0]))

            if eval_meth not in self.evaluation_methods:
                raise ValueError("Invalid evaluation method")

            ''' if dict value not specified revert to deafulat value set in __init()__ '''
            if isinstance(eval_params, dict):
                ''' set the number of days to offset from the begining of the time line'''
                if "days_offset" in eval_params.keys():
                    self.days_offset = eval_params.days_offset
                ''' set the window length in number of days'''
                if "window_length" in eval_params.keys():
                    self.window_length = eval_params.window_length
                ''' set the whether or not to return statistically significant values '''
                if "p_val_cutoff" in eval_params.keys():
                    self.p_val = eval_params.p_val_cutoff

            ''' store test results in list '''
            _l_rows=[]
            for row_idx in range(0,prospect_df.shape[0]-self.window_length):
#            for date in prospect_df["Date"]:
#                print(row_idx)
                ''' get the date value from the current datframe'''
                pros_day = prospect_df.iloc[row_idx]["Date"]
                base_day = baseline_df.iloc[row_idx]["Date"]

                ''' Set the left and right side date of the sliding window '''
                pros_win_left_dt = pros_day + timedelta(days=self.days_offset)
                pros_win_right_dt = pros_win_left_dt + timedelta(days=self.window_length-1)

                ''' set the base window left date same as current window month and day but base year '''
#                base_win_left_dt = datetime.datetime(base_yr, curr_win_left_dt.month, curr_win_left_dt.day)
                base_win_left_dt = base_day + timedelta(days=self.days_offset)
                base_win_right_dt = base_win_left_dt + timedelta(days=self.window_length-1)

                mask = (baseline_df['Date'] >= base_win_left_dt) & (baseline_df['Date'] <= base_win_right_dt)
                base_inner_df = baseline_df[mask]

                mask = (prospect_df['Date'] >= pros_win_left_dt) & (prospect_df['Date'] <= pros_win_right_dt)
                pros_inner_df = prospect_df[mask]

                mask = (baseline_df['Date'] < base_win_left_dt) | (baseline_df['Date'] > base_win_right_dt)
                base_outer_df = baseline_df[mask]

                mask = (prospect_df['Date'] < pros_win_left_dt) | (prospect_df['Date'] > pros_win_right_dt)
                pros_outer_df = prospect_df[mask]

                ''' apply fisher exact test '''
                stats_dict = self.get_fisher_exact(list(pros_inner_df[counts_col]),
                                                   list(base_inner_df[counts_col]),
                                                   list(pros_outer_df[counts_col]),
                                                   list(base_outer_df[counts_col]))
                if stats_dict["p_value"] < self.p_val:
                    _l_rows.append({"date" : pros_win_right_dt, "evaluation" : stats_dict})

            stats_df = pd.DataFrame(_l_rows)

        except Exception as err:
            _s_fn_id = "Class <TimeSeries> Function <get_ts_eval_stats>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return stats_df
