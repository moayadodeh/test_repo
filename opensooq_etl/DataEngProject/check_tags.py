from airflow.models import DAG
import pandas as pd
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.postgres_hook import PostgresHook
from io import StringIO
import math
from airflow.models import Variable


from scraper import LandsScraper_opensooq
from checker import Checker
from settings import BASE_URL
from settings import HEADERS
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import os
import shutil
my_scraper = LandsScraper_opensooq(BASE_URL, HEADERS)
my_checker = Checker()

def check_connection(ti):
    try:
        start_time = datetime.now()
        ti.xcom_push(key='start_time_',value = start_time)
        url = f'https://jo.opensooq.com/ar/عقارات-للبيع/أراضي-للبيع?page=1'
        content = my_scraper.fetch(url_name=url,max_req=1)
        print(content)
        Variable.update(key='_test', value=content)
        Variable.update(key='_error', value='pass')
    except:
        print('FAILLLLL')
        Variable.update(key='_test', value=content)
        Variable.update(key='_error', value='error')

def branch_connection(ti):
    if(Variable.get('_error') == 'error'):
        return 'error_email_task_'
    else:
        return 'check_info_tags_'

def check_info_tags(ti):
    content = Variable.get('_test')
    soup0 = BeautifulSoup(content, "lxml")
    df = my_scraper.scrape_id_url(1, 2)
    print(df)
    numbers_list = Variable.get('opensooq_number_list_full')
    misses = my_checker.check_tags_info(df,soup0)
    print(misses)
    ti.xcom_push(key='info_tags_misses', value=misses)

def check_numbers_tags(ti):
    check = False
    ti.xcom_push(key='skipped', value=check)
    if(check):
        numbers_list = Variable.get('opensooq_number_list_full')
        misses = my_checker.check_tags_numbers(numbers_list)
        print(misses)
        ti.xcom_push(key='numbers_tags_misses', value='list of misses:' + str(misses))

default_args = {
    'owner': 'moayad',
    'email': ['altlawy19@gmail.com'],
    'email_on_failure': True
}

with DAG(
        dag_id='_check_tags_opensooq_v01',
        schedule_interval=timedelta(days=100),
        # start_date=datetime(2023,12,30,10,0)
        start_date=datetime(2024, 1, 1, 21, 5),
        default_args=default_args,
        catchup=False
) as dag:
    connection_task = PythonOperator(
        task_id='check_connection_',
        python_callable=check_connection,
        provide_context=True
    )
    branching = BranchPythonOperator(
        task_id='branch_connection_',
        python_callable=branch_connection,
        provide_context=True
    )
    error = EmailOperator(
        task_id = 'error_email_task_',
        to='altlawy19@gmail.com',
        subject='Connection field',
        html_content = f"""Error while connecting to parse (info) data</p>
        <p>happend at {{{{ts}}}}</p>
        <p>### Error description  ####</p>
        <p>{{{{ti.xcom_pull(key='content',task_ids="check_connection_"))}}}}</p>
        """
    )
    check_info = PythonOperator(
        task_id='check_info_tags_',
        python_callable=check_info_tags,
        provide_context=True
    )
    check_numbers = PythonOperator(
        task_id='check_numbers_tags_',
        python_callable=check_numbers_tags,
        provide_context=True
    )
    final_report = EmailOperator(
        task_id='final_report_email_task_',
        to='altlawy19@gmail.com',
        subject='Check Tags Report',
        html_content=f"""Tags was successfully checked</p>
            
            <p>Info Tags missed: {{{{ti.xcom_pull(key='info_tags_misses',task_ids="check_info_tags_")}}}}</p>
            <p>Numbers Tags missed: Check tags?? {{{{ti.xcom_pull(key='skipped',task_ids="branch_check_numbers_")}}}},  
                    {{{{ti.xcom_pull(key='numbers_tags_misses',task_ids="check_numbers_tags_")}}}}</p>
            <p>Started at: {{{{ti.xcom_pull(key='start_time_',task_ids="check_connection_")}}}}</p>
            <p>Finished at: {{{{ts}}}}</p>
            """
    )

connection_task >> branching >> error
branching >> check_info >> check_numbers >> final_report
#branch_check_numbers >> final_report