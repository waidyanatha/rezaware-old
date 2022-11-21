#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
### The Index Project (TIP) Coingeko market-cap daily extracts

COPY THIS FILE INTO airflow home dags folder

The dag is setup to pull daily market-cap data

1. execute the api to request and store data in MongoDB

author(s): <nuwan.waidyanatha@rezgateway.com>
"""

import json
from textwrap import dedent

### The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

### Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta

### import standard python classes
import os
import sys
from datetime import datetime, date, timedelta
import traceback
import logging

prop_kwargs = {"WRITE_TO_FILE":False,
              }
sys.path.insert(1,"/home/nuwan/workspace/rezaware/")
import rezaware as reza
from wrangler.modules.assets.etl import cryptoMCExtractor as crypto
from utils.modules.etl.load import noSQLwls as nosql
clsMC = crypto.CryptoMarkets(
    desc='get crypto macket capitalization data', **prop_kwargs)

__data_source__ = 'coinmarketcap'

'''
### Define and start the DAG
    There are four PythonOperator tasks:
    (1) parameterize_urls_task - generate a list of urls with parameters to scrape data
    (2) init_storage_task - make the directory to hold the scraped data csv file
    (3) extract_prices_task - run the extraction process and save to a set of csv files
    (4) transform_task - read the folder with csv files to clean and transform for loading
    (5) load_task - write the data to the postgres database
    (6) clean_task - cleanup the tmp files used for exchaning data between tasks

'''
with DAG(
    'tip_'+__data_source__+'_daily_marketcap',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['samana.thetha@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'start_date': datetime(2022, 11, 18),
        'retries': 3,
        'retry_delay': timedelta(minutes=30),
        'schedule_interval': '@hourly',
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },

    description='mainly retrieve daily market-cap value and circulating asset volumes for crypto assets',
#     schedule_interval=timedelta(hours=24),
#     schedule_interval:'@daily',
    #start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     start_date=datetime(2022, 11, 18),
    catchup=False,
    tags=['wrangler','assets','etl','market-cap','stage', 'MongoDB'],
) as dag:
    
    dag.doc_md = __doc__

    ''' Function
            name: extract
            parameters:
                kwargs - to use with passing values between tasks
            procedure: requests market cap data and stores in mongodb collection
            xcom: collection name (str) pushed

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def extract(**kwargs):

#         _source = __data_source__
        _data, _coll_name = clsMC.get_daily_mc_data(data_owner=__data_source__)
        ti = kwargs['ti']
        ti.xcom_push(key='collection', value=_coll_name)
        logging.info("[function extract] read and write % data to MongoDB", __data_source__)

    ''' Function
            name: store
            parameters:
                kwargs - to use with passing values between tasks
            procedure: read marketcap data from DB collection and 
                        archive in google cloud store
            xcom: collection name (str) pulled

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def store(**kwargs):

        _db_name = "tip-marketcap"
        ti = kwargs['ti']
        _coll_name=ti.xcom_pull(key='collection')
        _fname = str(date.today())+".json"

        archived_data = clsMC.cold_store_daily_mc(
            from_db_name=_db_name,
            from_db_coll=_coll_name,
            to_file_name=_fname,
            to_folder_path=__data_source__,
        )

    ''' task to execute the parameterize_urls function '''
    read_n_write_task = PythonOperator(
        task_id='get_'+__data_source__+'_data',
        python_callable=extract,
    )
    read_n_write_task.doc_md = dedent(
        """\
    #### Read and Write Task
    
    Reads the JSON market-cap data, extracts essential data elements, and writes into the MongoDB. 
    """
    )

    ''' task to execute the parameterize_urls function '''
    cloud_store_task = PythonOperator(
        task_id='cold_store_'+__data_source__+'_data',
        python_callable=store,
    )
    cloud_store_task.doc_md = dedent(
        """\
    #### Cold store marketcap data
    
    Archive the extracted market-cap data in MongoDB in Google Cloud Storage. 
    """
    )


    ''' get url and storage location to pass as inputs to the ETL '''
    read_n_write_task >> cloud_store_task
