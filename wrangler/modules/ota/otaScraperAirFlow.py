#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
### OTA Web Scraper ETL DAGS

COPY THIS FILE INTO airflow home dags folder

1. Build the scraping URLs with paramet values inserted into place holder
2. Execute the scraper and save the CSV files in the folder
3. Read the files from the folder to import into the stage database
"""
# [START ETL]
# [START import_module]


import json
from textwrap import dedent

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta

# [END import_module]

# [START instantiate_dag]
with DAG(
    'zscrape_ota_property_prices',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['rezaware@rezgateway.com'],
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
    # [END default_args]
    description='scrape ota web data and stage in database',
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

        import os
        import sys
        from datetime import datetime, date, timedelta
        import traceback

        ROOT_DIR = "/home/nuwan/workspace/rezgate/wrangler"
        MODULE_PATH = os.path.join(ROOT_DIR, 'modules/ota/')
        kwargs = {"ROOT_DIR":ROOT_DIR}
        
        sys.path.insert(1,MODULE_PATH)
        import otaWebScraper as otaws
        clsScraper = otaws.OTAWebScraper(**kwargs)

        ''' Initialize args to start parameterizing urls '''
        file = "otaInputURLs.json"
        start_date = date.today()
        end_date = start_date + timedelta(days=1)
        
        scrape_crietia_dict = {"pageOffset":10,
                               "pageUpperLimit":550,
                               "startDate": start_date,
                               "endDate" : end_date,
                              }
        _, _ota_url_parameterized_list  = clsScraper.build_scrape_url_list(fileName=file,
                                                                           dirPath=None,
                                                                           **scrape_crietia_dict)

    # [END prepare_urls_function]

    # [START preppare_storate_location]
    def init_storage(**kwargs):

        from datetime import datetime, timezone

        dirPath = None
        _search_dt = datetime.now()
        kwargs = {'search_datetime': _search_dt}
        ''' include the timezone '''
        _search_dt = (_search_dt.replace(tzinfo=timezone.utc)).isoformat()

        _current_search_data_dir = clsScraper.get_search_data_dir_path(dirPath, **kwargs)
        print("Extracting data into %s for search datetime: %s" % (_current_search_data_dir,str(_search_dt)))
    
    # [END preppare_storate_location]
    # [START extract_function]
    def extract(**kwargs):
#        ti = kwargs['ti']
#        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
#        ti.xcom_push('order_data', data_string)

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



    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        ti = kwargs['ti']
        total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
        total_order_value = json.loads(total_value_string)

        print(total_order_value)

    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
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

    extract_task >> transform_task >> load_task

# [END main_flow]

# [END tutorial]