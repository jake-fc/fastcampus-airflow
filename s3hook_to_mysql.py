from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

default_args = {
    'owner': 'jake',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 10),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_to_mysql_v2',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

s3_bucket_name = 'jake-api'
s3_key = 'api/upbit-api/year=2023/month=04/day=13/hour=12/ls.s3.0674f7c7-8b73-4ec5-81c2-2b991ed4ca31.2023-04-13T12.29.part13.txt'
mysql_table_name = 'upbit_api'
mysql_conn_id = 'mysql_conn'
s3_conn_id = 'aws_default'

def load_s3_file_to_mysql(**context):
    # S3 hook를 사용하여 S3 파일을 읽어들인다.
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_object = s3_hook.get_key(s3_key, s3_bucket_name)

    # S3 파일을 pandas dataframe으로 읽어들인다.
    s3_file = s3_object.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(pd.compat.StringIO(s3_file), delimiter=',')

    # MySQL hook를 사용하여 RDS MySQL에 데이터를 적재한다.
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    mysql_hook.run(f"TRUNCATE TABLE {mysql_table_name}")
    mysql_hook.insert_rows(table=mysql_table_name, rows=df.to_records(index=False).tolist())

with dag:
    # S3 파일을 MySQL에 적재하는 PythonOperator
    load_s3_file_to_mysql_task = PythonOperator(
        task_id='load_s3_file_to_mysql',
        python_callable=load_s3_file_to_mysql,
        queue="celery",
        provide_context=True
    )

load_s3_file_to_mysql_task
