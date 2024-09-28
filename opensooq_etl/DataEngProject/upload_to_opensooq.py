from unittest.mock import inplace

import pandas as pd
import numpy as np
from airflow.models import DAG
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.postgres_hook import PostgresHook
from io import StringIO
import math
from airflow.models import Variable
import concurrent
from concurrent.futures import  ThreadPoolExecutor
from functools import partial
from scraper import LandsScraper_opensooq
from settings import BASE_URL
from settings import HEADERS
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import os
import shutil
from common_helpers import create_driver
my_scraper = LandsScraper_opensooq(BASE_URL, HEADERS)

## for s3
# n5ThwQN16nDW+l5yxLsbFjHbVYg4Gb5tuewoN4Jv

info_posts_table = "info_posts"
info_date_table = "info_date"

def check_connection(ti):
    try:
        start_time = datetime.now()
        ti.xcom_push(key='start_time_',value = start_time)
        url = f'https://jo.opensooq.com/ar/عقارات-للبيع/أراضي-للبيع'
        content = my_scraper.fetch(url_name=url)
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
        return 'scrape_urls_'

def scrape_urls_list(ti):
    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()
    with conn.cursor() as c:
        c.execute(f"""Delete from urls_list2""")
        conn.commit()
    content = Variable.get('_test')
        ##make email of problem
    soup = BeautifulSoup(content, "lxml")
    count_str = soup.find('span',{'class':"countNo font-22"}).text
    count = int(count_str[1:-1].replace(',',''))
    #count = 2
    print('COUNT', count)
    numOfWorkers = 8
    with ThreadPoolExecutor(max_workers=numOfWorkers) as ex:
        list_len = math.ceil((count /30) / numOfWorkers)
        start_len = [1,list_len+1, list_len*2+1, list_len*3+1,list_len*4+1,list_len*5+1, list_len*6+1, list_len*7+1]
        f = [ex.submit(my_scraper.scrape_id_url, start,start+list_len) for start in start_len]
        results = [f.result() for f in concurrent.futures.as_completed(f)]
        for result_df in results:
            upload_urls(result_df,conn)

def prepare_scrape_list(ti):
    dir = f"{Variable.get('s3_images_dir')}"
    if os.path.exists(dir):
        pass
    else:
        os.mkdir(dir)

    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()

    #current_date = datetime.now().date()
    #two_months = timedelta(days=60)
    #two_months_before_date = current_date - two_months
    #print(two_months_before_date)

    with conn.cursor() as c:
        c.execute(f""" SELECT *
                    FROM urls_list2 t1
                    LEFT JOIN info_posts t2 ON t1.poster_id= t2."إعلان رقم" 
                    WHERE t2."إعلان رقم" IS NULL; 
                """)
        scrape_data = c.fetchall()
        scrape_data_names = [desc[0] for desc in c.description]

        c.execute(f""" SELECT *
                            FROM urls_list2 t1
                            LEFT JOIN info_posts t2 ON t1.poster_id= t2."إعلان رقم"
                            WHERE t2."إعلان رقم" IS NOT NULL;
                        """)
        dup_data = c.fetchall()
        dup_data_names = [desc[0] for desc in c.description]
        conn.commit()


    scrape_df = pd.DataFrame(data=scrape_data, columns=scrape_data_names)
    dup_df = pd.DataFrame(data=dup_data, columns=dup_data_names)
    print('before drop::', 'scrape:',scrape_df.shape, 'dup_df:', dup_df.shape)
    scrape_df.drop_duplicates(subset=['poster_id'], inplace=True)
    dup_df = pd.concat([scrape_df, dup_df], axis=0, ignore_index=True)
    dup_df.drop_duplicates(subset=['poster_id'], inplace=True)
    print('final_df count:', dup_df.shape, 'final_urls_df_scrape count:', scrape_df.shape)
    print(dup_df.info())
    ti.xcom_push(key='list_len', value=scrape_df.shape[0])
    ti.xcom_push(key='scrape_df', value=scrape_df.to_json())
    ti.xcom_push(key='duplicate_df', value=dup_df.to_json())


def fetch(ti):
    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()
    df_scrape = ti.xcom_pull(key='scrape_df', task_ids='prepare_scrape_list_')
    df_duplicate = ti.xcom_pull(key='duplicate_df', task_ids='prepare_scrape_list_')
    df_scrape = pd.read_json(df_scrape)
    df_duplicate =pd.read_json(df_duplicate)

    print('df_scrape size', df_scrape.shape, 'df_duplicate size', df_duplicate.shape)
    numOfWorkers =8
    #print('sub_dup count', sub_dup_df.shape, 'merged_df count', merged_df.shape,'urls_to_scrape count',urls_to_scrape_df.shape)
    #merged_df = pd.concat([merged_df, sub_dup_df])

    #df_scrape=df_scrape.iloc[:100,:]
    #l = [[10,20,30],[40,50,60]]
    with ThreadPoolExecutor(max_workers=numOfWorkers) as ex:
        t = 1
        futures = []
        for df in df_scrape.values:
            #my_driver = create_driver()
            f = ex.submit(my_scraper.main_scraper,scrape_df=df,driver=None,counter=t)
            t+=1
            #print('fff', f.result())
            futures.append(f)
            #my_driver.quit()
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
        df_normals = pd.DataFrame()
        df_errors = pd.DataFrame()
        #print('results: ',results)
        for result_df in results:

            #df_normal,df_error = result_df
            df_normal = result_df
            df_normals = pd.concat([df_normals,df_normal],axis=0,ignore_index=True)

            #df_errors = pd.concat([df_errors,df_error],axis=0,ignore_index=True)
            #df,_ = result_df
        print('df_normals concat', df_normals.shape)
        print('fetch df_normals concat', df_normals.info())
        temp = df_normals.copy()
        ## temp
        df_errors = None
        #df_duplicate = pd.concat([temp, df_duplicate], axis=0, ignore_index=True)
        df_normals.drop(df_normals.columns[0], axis=1,inplace=True)
        print('fetch df normals',df_normals.info())
        upload_info_new(df_normals,conn,df_duplicate,dup=True)

def fetch_numbers(ti):
    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()
    df_scrape = ti.xcom_pull(key='scrape_df', task_ids='prepare_scrape_list_')
    numOfWorkers = 1
    #l = [df.iloc[:100,:],df.iloc[100:200,:]]

    list_len = math.ceil(ti.xcom_pull(key='list_len', task_ids='prepare_scrape_list_') / 30)
    numbers_list = Variable.get('opensooq_number_list_full')
    df_normal, df_error,pages_error = my_scraper.scrape_numbers_pages(my_numbers = numbers_list, pageStart = 1, pageEnd = list_len)
    upload_numbers(df_normal, df_error, conn)

    ti.xcom_push(key='pages_error', value=pages_error)

    #with ThreadPoolExecutor(max_workers=numOfWorkers) as ex:
    #    t = 1
    #    futures = []
    #    numbers_list = Variable.get('opensooq_number_list_full')
    #    #print(numbers_list)
    #    numbers_pool = numbers_list.split(',')
    #    print(df_scrape)
#
    #    list_len = ti.xcom_pull(key='list_len',task_ids='scrape_urls_') * 8
    #    print('before','num',t)
    #    f = ex.submit(my_scraper.scrape_numbers_pages, my_numbers=numbers_list, pageStart=1, pageEnd=list_len)
    #    print('after','num',t)
    #    t += 1
    #    futures.append(f)
    #    results = [f.result() for f in concurrent.futures.as_completed(futures)]
    #    df_normals = pd.DataFrame()
    #    df_errors = pd.DataFrame()
    #    for result_df in results:
    #        df_normal, df_error = result_df
    #        df_normals = pd.concat([df_normals, df_normal], axis=0, ignore_index=True)
    #        df_errors = pd.concat([df_errors, df_error], axis=0, ignore_index=True)
    #    print('first',results)
    #    print('second',df_normal.shape)
#
    #    print('rowssss count:',df_normals.shape)
    #    upload_numbers(df_normals, df_errors, conn)

def upload_info_new(df_normal,conn,dup_df=None,dup=False):
    with conn.cursor() as c:
        sio = StringIO()
        #df_normal = df_normal.loc[:,['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18','19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34','35', '36','37']]
        #c.execute(f"SELECT id FROM {Variable.get('opensooq_info_normal_table')} ORDER BY id DESC LIMIT 1;")
        #id = c.fetchall()[0][0]
        #print('id', id)
        #id =0
        #array_ids = np.arange(1, df_normal.shape[0] + 1) + id
        #df_normal.insert(0, 'id', id)

        df_normal.to_csv(sio, index=None, header= None,encoding='utf-8',sep=',',quotechar='"',quoting=csv.QUOTE_NONNUMERIC)
        print('df_normal size: ', df_normal.info())
        sio.seek(0)
        #c.copy_expert(sql=f"COPY {Variable.get('opensooq_info_normal_table')} From STDIN WITH CSV DELIMITER ','",file=sio)

        print(sio.seek(0))
        sio.seek(0)
        #c.copy_expert(sql=f"COPY {info_posts_table}_temp From STDIN WITH CSV DELIMITER ','", file=sio)

        c.copy_expert(sql=f"COPY {info_posts_table} From STDIN WITH CSV DELIMITER ','", file=sio)
        # c.execute(f""" SELECT *
        #                     FROM info_posts_temp t1
        #                     LEFT JOIN info_posts t2 ON t1."إعلان رقم" = t2."إعلان رقم"  -- Assuming `id` is the common column
        #                     WHERE t2."إعلان رقم" IS NULL;
        #                 """)
        # test_data = c.fetchall()
        # test_data_names = [desc[0] for desc in c.description]
        # conn.commit()
        # test_df = pd.DataFrame(data=test_data, columns=test_data_names)
        # print("test_df",test_df.shape)
        #sio = StringIO()
        #df_error.to_csv(sio, index=None, header=None,encoding='utf-8',sep=',',quotechar='"',quoting=csv.QUOTE_NONNUMERIC)
        #sio.seek(0)
        #c.copy_expert(sql="COPY info_error From STDIN WITH CSV DELIMITER ','",file=sio)

        if(dup):
            sio = StringIO()
            #dup_df['وقت السحب'] = datetime.now()
            dup_df = dup_df[['إعلان رقم']]
            dup_df.dropna(axis=0, inplace=True)
            dup_df.reset_index(drop=True,inplace=True)
            dup_df['إعلان رقم'] = dup_df['إعلان رقم'].astype(int)
            dup_df.insert(1, 'وقت السحب', datetime.now())
            #dup_df.insert(0,'وقت السحب',datetime.strptime('2024-05-10', '%Y-%m-%d').date())
            #c.execute(f"SELECT id FROM {Variable.get('opensooq_info_normal_table')} ORDER BY id DESC LIMIT 1;")
            #id = c.fetchall()[0][0]
            #print('id', id)
            #id =0
            #array_ids = np.arange(1, dup_df.shape[0] +1)+id
            #dup_df['id'] = array_ids
            #dup_df.insert(0, 'id', array_ids)
            print('duplicate', dup_df.info())


            dup_df.to_csv(sio, index=None, header=None, encoding='utf-8', sep=',', quotechar='"',
                             quoting=csv.QUOTE_NONNUMERIC)
            sio.seek(0)
            c.copy_expert(sql=f"COPY {info_date_table} From STDIN WITH CSV DELIMITER ','", file=sio)

        conn.commit()


def upload_numbers(df_normal, df_error, conn):
    with conn.cursor() as c:
        # upload numbers_normal
        sio = StringIO()
        df_normal.to_csv(sio, index=None, header=None, encoding='utf-8', sep=',', quotechar='"',
                         quoting=csv.QUOTE_NONNUMERIC)
        sio.seek(0)
        c.copy_expert(sql=f"COPY numbers_normal From STDIN WITH CSV DELIMITER ','", file=sio)
        print(df_normal.shape[0],'rows where uploaded')
        # upload numbers_error
        if(len(df_error) > 0):
            sio = StringIO()
            df_error = df_error.loc[:,'الرابط']
            df_error.to_csv(sio, index=None, header=None, encoding='utf-8', sep=',', quotechar='"',
                            quoting=csv.QUOTE_NONNUMERIC)
            sio.seek(0)
            c.copy_expert(sql="COPY numbers_error From STDIN WITH CSV DELIMITER ','", file=sio)

        conn.commit()

def upload_urls(result_df,conn):
    with conn.cursor() as c:
        sio = StringIO()

        result_df.to_csv(sio, index=None, header= None,encoding='utf-8',sep=',',quotechar='"',quoting=csv.QUOTE_NONNUMERIC)
        print(result_df.shape)
        sio.seek(0)
        c.copy_expert(sql="COPY urls_list2 From STDIN WITH CSV DELIMITER ','",file=sio)
        conn.commit()

def edit(ti):

    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()
    table_name = 'info_error'
    with conn.cursor() as c:
        c.execute(f"""Select * From info_error""")
        urls = c.fetchall()
    result = pd.DataFrame(data=urls)

    count= result.shape[1]

    if(count > 0):
        numOfWorkers = 8
        print('work')

        with ThreadPoolExecutor(max_workers=numOfWorkers) as ex:
            t = 1
            futures = []
            for df in result.values:
                f = ex.submit(my_scraper.main_scraper, errors=True, scrape_df=df, counter=t)
                t += 1
                futures.append(f)
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
            df_normals = pd.DataFrame()
            df_errors = pd.DataFrame()
            for result_df in results:
                df_normal, df_error = result_df
                df_normals = pd.concat([df_normals, df_normal], axis=0, ignore_index=True)
                df_errors = pd.concat([df_errors, df_error], axis=0, ignore_index=True)
            upload_info(df_normals, df_errors, conn)
        with conn.cursor() as c:
            c.execute(f"""Select * from {table_name}""")
            data_df = c.fetchall()
            sio = StringIO()
            data_df = pd.DataFrame(data_df)
            data_df['وقت السحب'] = datetime.now()
            data_df.to_csv(sio, index=None, header=None, encoding='utf-8', sep=',', quotechar='"',
                             quoting=csv.QUOTE_NONNUMERIC)
            print(data_df.shape)
            ti.xcom_push(key='rep_lost_data', value=data_df.shape[0])
            sio.seek(0)
            c.copy_expert(sql=f"COPY info_lost From STDIN WITH CSV DELIMITER ','", file=sio)
            #conn.commit()
            c.execute(f"Delete from {table_name}")

            current_date = datetime.now().date()
            one_day = timedelta(days=1)
            yesterday_date = current_date - one_day
            #c.execute(f"""SELECT id FROM {Variable.get('opensooq_info_normal_table')} Where ("وقت السحب") = '{yesterday_date}' ORDER BY id DESC LIMIT 1;""")
            #id = c.fetchall()[0][0]
            #id=0
            c.execute(f"""SELECT * FROM {Variable.get('opensooq_info_normal_table')} Where ("وقت السحب") = '{current_date}' """)
            data_df = c.fetchall()
            u_column_names = [desc[0] for desc in c.description]
            data_df = pd.DataFrame(data_df, columns=u_column_names)
            #array_ids = np.arange(1, data_df.shape[0] +1) +id
            #data_df['id'] = array_ids
            ti.xcom_push(key='rep_uploaded_data',value=data_df.shape[0])
            sio = StringIO()
            data_df.to_csv(sio, index=None, header=None, encoding='utf-8', sep=',', quotechar='"',
                           quoting=csv.QUOTE_NONNUMERIC)
            sio.seek(0)
            print(data_df,data_df.info())
            # c.execute(f"""Delete from info_normal """)
            c.execute(f"""Delete FROM {Variable.get('opensooq_info_normal_table')} Where ("وقت السحب") = '{current_date}' """)
            c.copy_expert(sql=f"COPY {Variable.get('opensooq_info_normal_table')} From STDIN WITH CSV DELIMITER ','", file=sio)
            conn.commit()
        end_time = datetime.now()
        time_taken = end_time - ti.xcom_pull(key='start_time_',task_ids="check_connection_")
        ti.xcom_push(key='time_taken', value=time_taken)

def edit_numbers(ti):
    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()
    df_scrape = ti.xcom_pull(key='scrape_df', task_ids='prepare_scrape_list_')
    numOfWorkers = 1
    # l = [df.iloc[:100,:],df.iloc[100:200,:]]

    list_len = math.ceil(ti.xcom_pull(key='list_len', task_ids='prepare_scrape_list_') / 30)
    numbers_list = Variable.get('opensooq_number_list_full')
    pages_error = ti.xcom_pull(key='pages_error', task_ids='fetch_numbers_')
    df_normal, df_error, pages_error = my_scraper.scrape_numbers_pages(my_numbers=numbers_list, pageStart=1,
                                                                       pageEnd=list_len,pages_error=pages_error,edit=True)
    upload_numbers(df_normal, df_error, conn)


    #with conn.cursor() as c:
    #    c.execute("select * from numbers_error")
    #    df_error = pd.Series(c.fetchall())
    #    conn.commit()
#
    #numOfWorkers=8
    #with ThreadPoolExecutor(max_workers=numOfWorkers) as ex:
    #    t = 1
    #    futures = []
    #    numbers_list = str(Variable.get('opensooq_number_list_full')).split()
   #     numbers_pool = numbers_list
    #    for df in df_error.values:
    #        if(numbers_pool == 0):
    #            numbers_pool = numbers_list
    #        f = ex.submit(my_scraper.scrape_numbers_url, df, my_number=numbers_pool[0], counter=t)
    #        numbers_pool.pop(0)
    #        t += 1
    #        futures.append(f)
    #    results = [f.result() for f in concurrent.futures.as_completed(futures)]
    #    df_normals = pd.DataFrame()
     #   df_errors = pd.DataFrame()
     #   for result_df in results:
    #        df_normal, df_error = result_df
    #        df_normals = pd.concat([df_normals, df_normal], axis=0, ignore_index=True)
    #        df_errors = pd.concat([df_errors, df_error], axis=0, ignore_index=True)
#
    #    upload_numbers(df_normals, df_error, conn)

def upload_s3(bucket_name,ti):
    hook = S3Hook('_opensooq_s3')
    #scrape_list= ti.xcom_pull(key='scrape_list', task_ids='fetch_')
    #print(scrape_list)
    #print(scrape_df['poster_id'])
    s3_key_prefix ='2325646564/'
    #local_directory = '/home/hadoop/opensooq/images/'
    local_directory = f"{Variable.get('s3_images_dir')}"
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
        shutil.rmtree(local_directory)
        print(f"Directory '{local_directory}' removed successfully.")
    else:
        print(f"Directory '{local_directory}' does not exist.")

default_args = {
    'owner': 'moayad',
    'email': ['altlawy19@gmail.com'],
    'email_on_failure': True
}

with DAG(
        dag_id='_opensooq_upload_data_v03',
        schedule_interval=timedelta(days=100),
        # start_date=datetime(2023,12,30,10,0)
        start_date=datetime(2024, 1, 1, 21, 5),
        default_args=default_args,
        catchup=False
) as dag:
    connection_task = PythonOperator(
        task_id='check_connection_',
        python_callable=check_connection,
        retries=3,
        retry_delay=timedelta(minutes=4),
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
    scrape_urls_task = PythonOperator(
        task_id='scrape_urls_',
        python_callable=scrape_urls_list,
        provide_context=True
    )
    prepare_scrape_list = PythonOperator(
        task_id='prepare_scrape_list_',
        python_callable=prepare_scrape_list,
        provide_context=True
    )
    #trigger_numbers_dag = TriggerDagRunOperator(
    #    task_id='trigger_numbers_dag_',
    #    trigger_dag_id='_opensooq_upload_numbers_v01',
    #    conf={"notice": "Hello DAG!"}
    #)
    fetch_numbers = PythonOperator(
        task_id='fetch_numbers_',
        python_callable=fetch_numbers,
        provide_context=True
    )
    edit_numbers = PythonOperator(
        task_id='edit_numbers_',
        python_callable=edit_numbers,
        provide_context=True
    )
    fetch_task = PythonOperator(
        task_id='fetch_',
        python_callable=fetch,
        provide_context=True
    )
    # retreive_error_data = PythonOperator(
    #     task_id='edit_',
    #     python_callable=edit,
    #     retries=3,
    #     retry_delay=timedelta(minutes=4),
    #     provide_context=True
    # )
    final_report = EmailOperator(
        task_id='final_report_email_task_',
        to='altlawy19@gmail.com',
        subject='Final Report',
        html_content=f"""Data was successfully uploaded</p>
            <p>happend at {{{{ts}}}}</p>
            <p>Uploaded rows: {{{{ti.xcom_pull(key='rep_uploaded_data',task_ids="edit_")}}}}</p>
            <p>Lost rows: {{{{ti.xcom_pull(key='rep_lost_data',task_ids="edit_")}}}}</p>
            <p>Time taken: {{{{ti.xcom_pull(key='time_taken',task_ids="edit_")}}}}</p>
            """
    )
    upload_s3 = PythonOperator(
        task_id='_upload_s3',
        python_callable=upload_s3,
        op_kwargs={
            'bucket_name': 'opensooq-bucket'
        },
        retries=3,
        retry_delay=timedelta(minutes=4),
        provide_context=True
    )
    #make_edit = PythonOperator(
    #    task_id='make_edit_',
    #    python_callable=make_edit,
    #    provide_context=True
    #)
connection_task >> branching >> error
branching >> scrape_urls_task >> prepare_scrape_list >> fetch_task  >> upload_s3 >> final_report
prepare_scrape_list>> fetch_numbers >> edit_numbers >> final_report
