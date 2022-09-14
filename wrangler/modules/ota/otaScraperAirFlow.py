#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
### OTA Web Scraper ETL DAGS

COPY THIS FILE INTO airflow home dags folder

1. Build the scraping URLs with paramet values inserted into place holder
2. Execute the scraper and save the CSV files in the folder
3. Read the files from the folder to import into the stage database
"""

import json
from textwrap import dedent

#import pendulum

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
MODULE_PATH = os.path.join(ROOT_DIR, 'modules/ota/')
UTILS_PATH = os.path.join(ROOT_DIR, 'utils/')
kwargs = {"ROOT_DIR":ROOT_DIR}

### inhouse libraries to initialize, extract, transform, and load
sys.path.insert(1,MODULE_PATH)
import otaWebScraper as otaws
sys.path.insert(1, UTILS_PATH)
import sparkWorkLoads as spark
clsScraper = otaws.OTAWebScraper(name="ota prices", **kwargs)
clsSparkWL = spark.SparkWorkLoads(name="ota prices", **kwargs)

### pyspark libraries for the transform task
from pyspark.sql.functions import substring,lit,col
from pyspark.sql.types import StringType,BooleanType,DateType,DecimalType,FloatType, IntegerType,LongType, ShortType, TimestampType

'''
### Define and start the DAG
    There are four PythonOperator tasks:
    (1) parameterize_urls_task
    (2) init_storage_task
    (3) extract_prices_task
    (4) transform_task
    (5) 

'''
with DAG(
    'zscrape_ota_property_prices',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['rezaware@rezgateway.com'],
        'email_on_failure': True,
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
    # [END default_args]
    description='scrape ota property web data and stage in database',
    schedule_interval=timedelta(hours=1),
    #start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    start_date=datetime(2022, 9, 12),
    catchup=False,
    tags=['wrangler','ota','scrape','stage'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START parameterize_urls]
    def parameterize_urls(**kwargs):

#        import os
#        import sys
#        from datetime import datetime, date, timedelta
#        import traceback

#        ROOT_DIR = "/home/nuwan/workspace/rezgate/wrangler"
#        MODULE_PATH = os.path.join(ROOT_DIR, 'modules/ota/')
#        kwargs = {"ROOT_DIR":ROOT_DIR}
        
#        sys.path.insert(1,MODULE_PATH)
#        import otaWebScraper as otaws
#        clsScraper = otaws.OTAWebScraper(**kwargs)

        ''' Initialize args to start parameterizing urls '''
        file = "otaInputURLs.json"
        start_date = date.today()
        end_date = start_date + timedelta(days=1)
        
        scrape_crietia_dict = {"pageOffset":10,
                               "pageUpperLimit":550,
                               "startDate": start_date,
                               "endDate" : end_date,
                              }
        _fname, _ota_url_parameterized_list  = clsScraper.build_scrape_url_list(fileName=file,
                                                                           dirPath=None,
                                                                           **scrape_crietia_dict)
        ''' define XCOM parameters for downstream tasks '''
        logging.info("[function parameterize_urls] push url stored file %s to xcom", _fname)
        ti = kwargs['ti']
        ti.xcom_push(key='url_list_file', value=_fname)
        
    # [END prepare_urls_function]

    # [START preppare_storate_location]
    def init_storage(**kwargs):

        from datetime import datetime, timezone

        dirPath = None
        _search_dt = datetime.now()
        fnargs = {'searchDateTime': _search_dt,
                  'storageLocation': "local",   # values can be "local" or "AWS_S3"
                 }
        ''' include the timezone '''
        _search_dt = (_search_dt.replace(tzinfo=timezone.utc)).isoformat()

        _search_data_store = clsScraper.get_search_data_dir_path(dirPath, **fnargs)
        ti = kwargs['ti']
        logging.info("[function init_storage]push storage location %s to xcom", _search_data_store)
        ti.xcom_push(key='storage_location', value=_search_data_store)
        logging.info("[function init_storage] push search datetime %s to xcom", str(_search_dt))
        ti.xcom_push(key='search_datetime', value=_search_dt)
    
    # [END preppare_storage_location]

    # [START extract_function]
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
#            otaURLfile = None,
            searchDT = _search_dt,
            dirPath = _save_dir_path)


    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):

        ti = kwargs['ti']
        extract_data_string = ti.xcom_pull(task_ids='extract', key='order_data')
        order_data = json.loads(extract_data_string)

        total_order_value = 0
        for value in order_data.values():
            total_order_value += value

        total_value = {"total_order_value": total_order_value}
        total_value_json_string = json.dumps(total_value)
        ti.xcom_push('total_order_value', total_value_json_string)

        _save_dir_path = ti.xcom_pull(key='storage_location')
        logging.info("[function extract] xcom pull folder path to storage location %s", _save_dir_path)
        _search_sdf = clsSparkWL.read_csv_to_sdf(filesPath=_save_dir_path)
#        _search_sdf = _search_sdf.distinct()


_search_sdf=_search_sdf.withColumn("currency", lit("US$"))
_search_sdf=_search_sdf.withColumn('room_rate', substring('room_rate', 4,10))

''' reset data types to match table '''
_search_sdf = _search_sdf.withColumn("destination_id",col("destination_id").cast(StringType())) \
    .withColumn("room_rate",col("room_rate").cast(FloatType()))
#    .withColumn("search_datetime",col("search_datetime").cast(DateType()))

_search_sdf.printSchema()
#_search_sdf.show(n=2, vertical=True, truncate=False)
print("Split and Extraction complete!")
    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        ti = kwargs['ti']
        total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
        total_order_value = json.loads(total_value_string)

        print(total_order_value)

    # [END load_function]

    # [START clear_tmp_files]
    def clear_tmp_files(**kwargs):
        ti = kwargs['ti']
        total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
        total_order_value = json.loads(total_value_string)

        print(total_order_value)

    # [END load_function]
    # [START main_flow]
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

    init_storage_task = PythonOperator(
        task_id='init_storage',
        python_callable=init_storage,
    )
    init_storage_task.doc_md = dedent(
        """\
    #### Initialize the Folder to store the data Task
    Create a new folder with the date and time for this scrape data
    """
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    The extract process will use inputs from the get_urls_task and init_storage_task
    """
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    [get_urls_task, init_storage_task] >> extract_task >> transform_task >> load_task

# [END main_flow]

# [END tutorial]