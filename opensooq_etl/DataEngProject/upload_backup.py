from operator import index

import pandas as pd
from airflow.models import DAG
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.postgres_hook import PostgresHook
from io import StringIO
import math
from airflow.models import Variable
import concurrent
from concurrent.futures import  ThreadPoolExecutor
from functools import partial
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import os
import shutil

def extract(ti):
    dir = f"{Variable.get('s3_images_dir')}"
    if os.path.exists(dir):
        pass
    else:
        os.mkdir(dir)

    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()
    start_time = datetime.now()
    ti.xcom_push(key='start_time_', value=start_time)
    current_date = start_time.date()

    one_days = timedelta(days=1)
    six_days = timedelta(days=7)
    yesterday = current_date - one_days
    seven_days_ago = current_date - six_days
    print(seven_days_ago)
    with conn.cursor() as c:

        c.execute(
            f"""Select * from {Variable.get('opensooq_info_normal_table')} Where Date("وقت السحب") >= '{seven_days_ago}' and Date("وقت السحب") <= '{yesterday}'""")
        full_data = c.fetchall()
        f_column_names = [desc[0] for desc in c.description]

    full_data_df = pd.DataFrame(data=full_data, columns=f_column_names)
    full_data_df.to_csv(f"{Variable.get('s3_backup_dir')}/{seven_days_ago}.{yesterday}.csv",index=None, encoding='utf-8', sep=',', quotechar='"',
                   quoting=csv.QUOTE_NONNUMERIC)


def upload_backup(bucket_name,ti):
    hook = S3Hook('_opensooq_s3')
    #local_directory = '/home/hadoop/opensooq/backup/'
    local_directory = f"{Variable.get('s3_backup_dir')}"
    counter = 0
    for root, dirs, files in os.walk(local_directory):
        print(dirs,counter)
        #s3_key_prefix=scrape_list[counter]
        for filename in files:
            local_file_path = os.path.join(root, filename)
            s3_key = os.path.join(os.path.relpath(local_file_path, local_directory))
            #print(s3_key)
            hook.load_file(filename = local_file_path,key = s3_key, bucket_name=bucket_name,replace=True)
    if os.path.exists(local_directory):
        # Remove the directory

#        shutil.rmtree(local_directory)
        print(f"Directory '{local_directory}' removed successfully.")
    else:
        print(f"Directory '{local_directory}' does not exist.")


default_args = {
    'owner': 'moayad',
    'email': ['altlawy19@gmail.com'],
    'email_on_failure': True
}

with DAG(
        dag_id='_opensooq_backup_v02',
        schedule_interval=timedelta(days=7),
        # start_date=datetime(2023,12,30,10,0)
        start_date=datetime(2024, 8, 19, 21),
        default_args=default_args,
        catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id='_extract',
        python_callable=extract,
        retries=3,
        retry_delay=timedelta(minutes=4),
        provide_context=True
    )
    upload_s3 = PythonOperator(
        task_id='_upload_s3',
        python_callable=upload_backup,
        op_kwargs={
            'bucket_name': 'opensooq-backups'
        },
        retries=3,
        retry_delay=timedelta(minutes=4),
        provide_context=True
    )
    final_report = EmailOperator(
        task_id='_Backup_upload_email',
        to='altlawy19@gmail.com',
        subject='Backup Uploadt',
        html_content=f"""Backup was successfully uploaded</p>
            <p>Started at: {{{{ti.xcom_pull(key='start_time_',task_ids="_extract")}}}}</p>
            <p>Finished at: {{{{ts}}}}</p>
            """
    )


extract_task>>upload_s3>>final_report