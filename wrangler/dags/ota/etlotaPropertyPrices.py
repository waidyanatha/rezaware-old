#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
### rezAWARE OTA Web Scraper ETL

COPY THIS FILE INTO airflow home dags folder

The dag is setup to scrape hotel prices from online travel agency websites

1. Build the scraping URLs with paramet values inserted into place holder
2. Execute the scraper and save the CSV files in the folder
3. Read the files from the folder to import into the stage database

author(s): <nuwan.waidyanatha@rezgateway.com>
modified : 2022-09-24
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

### environment variables to load inhouse libraries
ROOT_DIR = "/home/nuwan/workspace/rezgate/wrangler"
#UTILS_PATH = os.path.join(ROOT_DIR, 'utils/')
UTILS_PATH = "/home/nuwan/workspace/rezgate/utils/"
MODULE_DIR = os.path.join(ROOT_DIR, 'modules/ota/')
DATA_DIR = os.path.join(ROOT_DIR, 'data/hospitality/bookings/scraper')
#DATA_PATH = os.path.join(ROOT_DIR, 'data/')
#kwargs = {"ROOT_DIR":ROOT_DIR}
prop_kwargs = {
    "ROOT_DIR":ROOT_DIR,   # absolute path to the wrangler dir
    "MODULE_DIR":MODULE_DIR,   # absolute path to the ota module
    "DATA_DIR":DATA_DIR, # absolute path to the scraper data dir
}

### inhouse libraries to initialize, extract, transform, and load
sys.path.insert(1,MODULE_DIR)
import propertyScrapers as ps
#import otaWebScraper as otaws
sys.path.insert(1, UTILS_PATH)
import sparkWorkLoads as sparkwl
### pyspark libraries for the transform task
#clsScraper = otaws.OTAWebScraper(name="ota prices", **prop_kwargs)
clsScraper = ps.PropertyScraper(desc='scrape hotel prices data from OTAs', **prop_kwargs)
clsSparkWL = sparkwl.SparkWorkLoads(desc="airflow ETL property room prices", **prop_kwargs)
### must be declared after importing sparkWorkLoads
from pyspark.sql.functions import substring,lit,col
from pyspark.sql.types import StringType,BooleanType,DateType,DecimalType,FloatType, IntegerType,LongType, ShortType, TimestampType


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
    'rezaware_ota_property_prices',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['admin.rezaware@rezgateway.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
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

    description='scrape ota property web data and stage in database',
    schedule_interval=timedelta(hours=1),
    #start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    start_date=datetime(2022, 9, 23),
    catchup=False,
    tags=['wrangler','ota','properties','scrape', 'stage'],
) as dag:
    
    dag.doc_md = __doc__

    ''' Function
            name: parameterize_urls
            parameters:
                kwargs - to use with passing values between tasks
            procedure: sets the start & end date and scrape criteria paramenters
                        to execute the function for generating the list of URLs
            xcom: url_list_file (string) with tmp storage of list of URLs

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def parameterize_urls(**kwargs):

        ''' Initialize args to start parameterizing urls '''
        file = "otaInputURLs.json"
        start_date = date.today()
        end_date = start_date + timedelta(days=1)
        
        urls_kwargs = {"pageOffset":10,
              "pageUpperLimit":550,
              "startDate": start_date,
              "endDate" : end_date,
             }
        _fname, _ota_url_parameterized_list = clsScraper.build_scrape_url_list(
            file_name=file,  # mandatory to give the inputs json file
            dir_path=None,   # optional to be used iff required
            **urls_kwargs
        )
        ''' define XCOM parameters for downstream tasks '''
        ti = kwargs['ti']
        ti.xcom_push(key='url_list_file', value=_fname)
        logging.info("[function parameterize_urls] push url stored file %s to xcom", _fname)


    ''' Function
            name: init_storage
            parameters:
                kwargs - to use with passing values between tasks
            procedure: creates a folder to store the scraped data of each url as a csv file
            xcom: storage_location (string) is pushed with folder path
                  search_datetime (datatime) is pushed basically todays date

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def init_storage(**kwargs):

        from datetime import datetime, timezone

        dirPath = None
        _search_dt = datetime.now()
        ''' round the search datatime to nearest 15 min '''
        _search_dt = _search_dt + (datetime.min - _search_dt) % timedelta(minutes=15)
        ''' include the timezone '''
        _search_dt = (_search_dt.replace(tzinfo=timezone.utc)).isoformat()
        ''' '''
#         fnargs = {'searchDateTime': _search_dt,
#                   'storageLocation': "local",   # values can be "local" or "AWS_S3"
#                  }
        storage_kwargs = {
#             'SEARCH_DATETIME': _search_dt,
            'STORAGE_METHOD': "local",   # values can be "local" or "AWS_S3"
        }

#         ''' include the timezone '''
#         _search_dt = (_search_dt.replace(tzinfo=timezone.utc)).isoformat()

        _search_data_store = clsScraper.make_storage_dir(**storage_kwargs)
        ti = kwargs['ti']
        ti.xcom_push(key='storage_location', value=_search_data_store)
        logging.info("[function init_storage]push storage location %s to xcom", _search_data_store)
        ti.xcom_push(key='search_datetime', value=_search_dt)
        logging.info("[function init_storage] push search datetime %s to xcom", str(_search_dt))

    ''' Function
            name: extract
            parameters:
                kwargs - to use with passing values between tasks
            procedure: Runs the scraping process for each and evey URL to the
                        store the data in the csv files at the given directory path
            xcom: url_list_file (string) is pulled to give as an input to the scraper function
                    search_datetime (datetime) is pulled to give as an input to the scraper function
                    storage_location (string) is pulled to give as an input to the scraper function

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def extract(**kwargs):
        
        import pandas as pd

        ti = kwargs['ti']
        _otaURLfilePath=ti.xcom_pull(key='url_list_file')
        logging.info("[function extract] xcom pull file path to parameterized urls %s", _otaURLfilePath)
        _search_dt=ti.xcom_pull(key='search_datetime')
        logging.info("[function extract] xcom pull search datetime %s", str(_search_dt))
        _save_dir_path = ti.xcom_pull(key='storage_location')
        logging.info("[function extract] xcom pull folder path to storage location %s", _save_dir_path)
        
#        if _otaURLfilePath:
        urlDF = pd.read_csv(_otaURLfilePath, sep=",")
        _otaURLParamDictList = urlDF.to_dict('records')

        _l_saved_files = clsScraper.scrape_url_list(
            otaURLlist =_otaURLParamDictList,
            searchDT = _search_dt,
            data_store_dir = _save_dir_path)
        logging.info("%d csv files with scraped property data saved to %s", len(_l_saved_files), _save_dir_path)


    ''' Function
            name: transform
            parameters:
                kwargs - to use with passing values between tasks
            procedure: runs the function to read the individual csv file to clean and transform the data
            xcom: storage_location (string) is pulled to give as an input of the file directory locations

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def transform(**kwargs):

        ti = kwargs['ti']
        _saved_ota_prices_fpath = ti.xcom_pull(key='storage_location')
#        _saved_ota_prices_fpath = "/home/nuwan/workspace/rezgate/wrangler/data/hospitality/bookings/scraper/rates/2022-9-14-21-0/"
        logging.info("[function extract] xcom pull folder path to storage location %s", _saved_ota_prices_fpath)
        _search_sdf = clsSparkWL.read_csv_to_sdf(filesPath=_saved_ota_prices_fpath)
#        _search_sdf = _search_sdf.distinct()
        logging.info("Spark loaded %d rows", _search_sdf.count())

        ''' enrich the data with: 
            (1) matching city names to the codes 
            (2) categorizing the room types based on the taxonomy
            (3) setting the data types of the columns
            using a transform function in the properties class '''
        _search_sdf=_search_sdf.withColumn("currency", lit("US$"))
        _search_sdf=_search_sdf.withColumn('room_rate', substring('room_rate', 4,10))

        ''' reset data types to match table '''
        _search_sdf = _search_sdf.withColumn("destination_id",col("destination_id").cast(StringType())) \
                        .withColumn("room_rate",col("room_rate").cast(FloatType()))

        logging.info("Split and Extraction complete!")
        
        ''' Get destination id dictionary '''
        destfilesPath = os.path.join(DATA_DIR, 'destinations/')
#        destfilesPath = "../../data/hospitality/bookings/scraper/destinations/"
        destinations_sdf = clsSparkWL.read_csv_to_sdf(filesPath=destfilesPath)
        logging.info("Loaded %d rows from %s to replace destination ids with names", destinations_sdf.count(), destfilesPath)
        ''' Lookup & replace destination name '''
        destinations_sdf = destinations_sdf.selectExpr("city as destination_name", \
                                                       "destinationID as destination_id")
        ''' set data types '''
#         destinations_sdf = destinations_sdf.withColumn("destination_name",col("destination_name").cast(StringType())) \
#                                             .withColumn("destination_id",col("destination_id").cast(StringType()))
        logging.info("Set data types for: destination_name, destination_id")
        ''' augment destination id in dataframe ''' 
#         aug_search_sdf = destinations_sdf.join(_search_sdf,"destination_id")
        aug_search_sdf = _search_sdf.join(destinations_sdf,
                                          _search_sdf.destination_id == destinations_sdf.destination_id,
                                          how='leftouter').drop(_search_sdf.destination_id)
        logging.info("Destination names & id augmented to dataframe.")
        ''' save data to tmp file '''
        _tmp_fname = clsSparkWL.save_sdf_to_csv(aug_search_sdf)
        logging.info("Cleaned data saved to %s",_tmp_fname)
        ti.xcom_push(key='tmp_data_file', value=_tmp_fname)
        logging.info("[transform] xcom push file name to %s", ti) 

    ''' Function
            name: load
            parameters:
                kwargs - to use with passing values between tasks
            procedure: loads the transfromed data from the tmp file to call the function
                        for inserting the data into the database
            xcom: tmp_data_file (string) is pulled to give as an input of the file path

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def load(**kwargs):
        
        ''' pull the tmp file path from xcom '''
        ti = kwargs['ti']
        _tmp_fname = ti.xcom_pull(key='tmp_data_file')
        ''' retrieve the data into a dataframe '''
        _get_tmp_sdf = clsSparkWL.read_csv_to_sdf(filesPath=_tmp_fname)
        logging.info("Retrieved %d records from %s", _get_tmp_sdf.count(), _tmp_fname)
        ''' save data to table '''
        _s_tbl_name = "ota_property_prices"
        count = clsSparkWL.insert_sdf_into_table(save_sdf=_get_tmp_sdf, dbTable=_s_tbl_name)
        logging.info("%d records saved to %s", count, _s_tbl_name)


    ''' Function (TODO)
            name: clear_tmp_files
            parameters:
                kwargs - to use with passing values between tasks
            procedure: calls the funtion to remove the tmp files created by the sessions
                        for inserting the data into the database
            xcom: tmp_data_file (string) is pulled to give as an input of the file path

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def clear_tmp_files(**kwargs):
#        ti = kwargs['ti']
#        total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
#        total_order_value = json.loads(total_value_string)

        print("TODO")

    ''' task to execute the parameterize_urls function '''
    get_urls_task = PythonOperator(
        task_id='get_urls',
        python_callable=parameterize_urls,
    )
    get_urls_task.doc_md = dedent(
        """\
    #### Parameterize URLs Task
    
    Reads the JSON input file with the OTA specific URL templates with parameter placeholders.
    Generates a series of URLs with the parametric combinations for checkin date, destination,
    page display, and so on. 
    """
    )

    ''' task to execute the init_storage_task function '''
    init_storage_task = PythonOperator(
        task_id='init_storage',
        python_callable=init_storage,
    )
    init_storage_task.doc_md = dedent(
        """\
    #### Initialize the Folder to store the data Task
    
    Create a new folder with the date and time for this scrape data. Then returns the directory
    path for the downstream extract task
    """
    )

    extract_task = PythonOperator(
        task_id='extract_ota_prices',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    
    The extract process will use inputs from the get_urls_task and init_storage_task. Then execute
    the extract function to scrape the date into a set of csv files
    """
    )

    transform_task = PythonOperator(
        task_id='transform_for_db',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    
    Reads the aved csv files into a dataframe to clean and enhance the data as a precursor to saving in DB.
    """
    )

    load_task = PythonOperator(
        task_id='load_into_stageDB',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    
    Calls the load function to save the scraped and cleaned data in the database.
    """
    )

    ''' get url and storage location to pass as inputs to the ETL '''
    [get_urls_task, init_storage_task] >> extract_task >> transform_task >> load_task
