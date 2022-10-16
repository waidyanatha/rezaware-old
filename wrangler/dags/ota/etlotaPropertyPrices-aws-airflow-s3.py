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

resource(s): https://www.astronomer.io/guides/airflow-sagemaker/
"""
### utility packages
import json
from textwrap import dedent

### The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
### Operators; we need this to operate!
from airflow.operators.python import PythonOperator
### aws 
# from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sagemaker_transform import SageMakerTransformOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

### import standard python classes
import os
import sys
from datetime import datetime, date, timedelta
import traceback
import logging

########## set the parameters before starting the runs ##############

__search_window_days__ = 1   # set the search window length 
__page_offset__ = 1         # set the page offset for building urls
__page_upper_limit__ = 3   # maximum number of pages for building urls
__schedule_interval__ = 5   # number of hours between runs 

########## set environment directory path variables before runs #####

ROOT_DIR = "/home/ec2-user/SageMaker/rezgate/wrangler"
UTILS_DIR = "/home/ec2-user/SageMaker/rezgate/utils/"
MODULE_DIR = os.path.join(ROOT_DIR, 'modules/ota/')
DATA_DIR = os.path.join(ROOT_DIR, 'data/hospitality/bookings/scraper')
prop_kwargs = {
    "ROOT_DIR":ROOT_DIR,   # absolute path to the wrangler dir
    "MODULE_DIR":MODULE_DIR,   # absolute path to the ota module
    "DATA_DIR":DATA_DIR, # absolute path to the scraper data dir
}

#####################################################################

### inhouse libraries to initialize, extract, transform, and load
sys.path.insert(1,MODULE_DIR)
import propertyScrapers as ps
sys.path.insert(1, UTILS_DIR)
import sparkwls as spark
''' executing the property scraper '''
clsScraper = ps.PropertyScraper(desc='scrape hotel prices data from OTAs', **prop_kwargs)
clsSparkWL = spark.SparkWorkLoads(desc="airflow ETL property room prices", **prop_kwargs)
### must be declared after importing sparkWorkLoads
# from pyspark.sql.functions import substring,lit,col
# from pyspark.sql.types import StringType,BooleanType,DateType,DecimalType,FloatType, IntegerType,LongType, ShortType, TimestampType


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
        'depends_on_past': True,
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
    schedule_interval=timedelta(hours=__schedule_interval__),
    #start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    start_date=datetime(2022, 10, 5),
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
        end_date = start_date + timedelta(days=__search_window_days__)
        
        urls_kwargs = {"pageOffset":__page_offset__,
              "pageUpperLimit":__page_upper_limit__,
              "startDate": start_date,
              "endDate" : end_date,
             }
        logging.debug("[parameterize_urls] building url list for %s start datetime and %d days search window"\
                      ,str(start_date),__search_window_days__)
        _fname, _ota_url_parameterized_list = clsScraper.build_scrape_url_list(
            file_name=file,  # mandatory to give the inputs json file
            dir_path=None,   # optional to be used iff required
            **urls_kwargs
        )
        ''' define XCOM parameters for downstream tasks '''
        ti = kwargs['ti']
        ti.xcom_push(key='url_list_file', value=_fname)
        logging.debug("[parameterize_urls] push url stored file %s to xcom", _fname)


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
        logging.debug("[initi_storage] search datetime set to %s", str(_search_dt))

        storage_kwargs = {
#             'SEARCH_DATETIME': _search_dt,
            'STORAGE_METHOD': "local",   # values can be "local" or "AWS_S3"
        }

        _search_data_store = clsScraper.make_storage_dir(**storage_kwargs)
        ti = kwargs['ti']
        ti.xcom_push(key='storage_location', value=_search_data_store)
        logging.debug("[init_storage]push storage location %s to xcom", _search_data_store)
        ti.xcom_push(key='search_datetime', value=_search_dt)
        logging.debug("[init_storage] push search datetime %s to xcom", str(_search_dt))

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
        logging.debug("[extract] xcom pull file path to parameterized urls %s", _otaURLfilePath)
        _search_dt=ti.xcom_pull(key='search_datetime')
        logging.debug("[extract] xcom pull search datetime %s", str(_search_dt))
        _save_dir_path = ti.xcom_pull(key='storage_location')
        logging.debug("[extract] xcom pull folder path to storage location %s", _save_dir_path)
        
        urlDF = pd.read_csv(_otaURLfilePath, sep=",")
        _otaURLParamDictList = urlDF.to_dict('records')

        _l_saved_files = clsScraper.scrape_url_list(
            otaURLlist =_otaURLParamDictList,
            searchDT = _search_dt,
            data_store_dir = _save_dir_path)
        logging.info("[extract] %d csv files with scraped property data saved to %s", len(_l_saved_files), _save_dir_path)


    ''' Function
            name: transform
            parameters:
                kwargs - to use with passing values between tasks
            procedure: runs the function to read the individual csv file to clean and transform the data
            xcom: storage_location (string) is pulled to give as an input of the file directory locations

            author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def transform(**kwargs):

        ''' to be used for testing only and should get file name from kwargs'''
#          _saved_ota_prices_fpath = os.path.join(DATA_DIR, "rates/2022-10-5-3-0/")
        ''' for normal runs get storage location directory from xcom '''
        ti = kwargs['ti']
        _saved_ota_prices_fpath = ti.xcom_pull(key='storage_location')
        logging.info("[transform] xcom pull folder path to storage location %s", _saved_ota_prices_fpath)
        spark_kwargs = {"TO_PANDAS":True,   # change spark dataframe to pandas
                        "IS_FOLDER":True,   # if folder then check if folder is empty
                       }
        _search_sdf, traceback = clsSparkWL.read_csv_to_sdf(filesPath=_saved_ota_prices_fpath, **spark_kwargs)
        if not traceback:
            logging.info("[transform] Spark loaded %d rows", _search_sdf.shape[0])
        else:
            logging.info("No data loaded by spark; process failed!")

        ''' enrich the data with: 
            (1) matching city names to the codes 
            (2) categorizing the room types based on the taxonomy
            (3) setting the data types of the columns
            using a transform function in the properties class '''

        ''' define room price column to extrac number'''
        logging.debug("[transform] Begin extracting room prices for %d rows", _search_sdf.shape[0])
        rate_col_name = "room_rate"
        aug_col_name = "room_price"
        ''' extract the price value from room rate'''
        _search_sdf = clsScraper.extract_room_rate(_search_sdf,rate_col_name,aug_col_name)
        logging.debug("[transform] Room prices extraction complete for %d rows", _search_sdf.shape[0])
        
        ''' categorize the room types '''
        logging.debug("[transform] Begin categorizing room types for %d rows", _search_sdf.shape[0])
        emb_kwargs = {
            'LOWER':True,
            'NO_STOP_WORDS':False,
            'METRIC':"COSIN",
            'MAX_SCORES':2,
            'TOLERANCE':0.7,
            'ROOM_CATE_FNAME':"room_descriptions.csv",
        }
        _categorized_room_df = clsScraper.merge_similar_room_cate(_search_sdf,emb_kwargs)
        _room_cate_count = len(_categorized_room_df.room_cate.unique())
        logging.debug("[transform] Assigned %d room categories", _room_cate_count)

        ''' Lookup & replace destination name '''
        logging.debug("[transform] Begin lookup and assign destination Lx")
        _aug_dest_df = clsScraper.assign_lx_name(data_df=_categorized_room_df)
        _room_dest_count = len(_aug_dest_df.dest_lx_name.unique())
        logging.info("[transform] Merged %d rows with %d destination name and type"\
                     , _aug_dest_df.shap[0], _room_dest_count)
        ''' save data to tmp file '''
        _tmp_fname = clsSparkWL.save_sdf_to_csv(_aug_dest_df)
        logging.info("[transform] Cleaned data saved to %s",_tmp_fname)
        ti.xcom_push(key='tmp_data_file', value=_tmp_fname)
        logging.debug("[transform] xcom push file name to %s", ti) 

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
        logging.debug("[load] pulled tmp_fname from xcom %s", ti) 
        ''' retrieve the data into a dataframe '''
        _get_tmp_sdf = clsSparkWL.read_csv_to_sdf(filesPath=_tmp_fname)
        logging.info("[load] Retrieved %d records from %s", _get_tmp_sdf.count(), _tmp_fname)
        ''' save data to table '''
        _s_tbl_name = "ota_property_prices"
        count, saved_df = clsScraper.save_to_db(data_df=_aug_dest_df,table_name = _tbl_name)
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
#     transform_task >> load_task