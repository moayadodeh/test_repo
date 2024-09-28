from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def init():
    postgres = PostgresHook(postgres_conn_id="_opensooq_db")
    conn = postgres.get_conn()
    with conn.cursor() as c:
        with open('/home/hadoop/airflow/dags/DataEngProject/info_normal_table.sql','r') as table:
            table_n = table.read()
            info_n=table_n.format(table_name='info_normal')
            info_n_temp = table_n.format(table_name='info_normal_temp')
        print(info_n_temp)
        c.execute(info_n)
        c.execute(info_n_temp)
        conn.commit()

default_args = {
    'owner': 'moayad',
    'email': ['altlawy19@gmail.com'],
    'email_on_failure': True
}


with DAG(
        dag_id='_opensooq_init_v01',
        # start_date=datetime(2023,12,30,10,0)
        start_date=datetime(2024, 1, 1, 21, 5),
        default_args=default_args,
        catchup=False
) as dag:
    fetch_task = PythonOperator(
        task_id='init_tables',
        python_callable=init,
        provide_context=True
    )