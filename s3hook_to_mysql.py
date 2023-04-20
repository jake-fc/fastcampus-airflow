from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import io

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


    
def insert_s3_data_bulk(**context):
    s3_files = s3_hook.list_keys(s3_bucket_name)

    cleaned_s3_files = []
    for s3_file in s3_files :
        if "year=2023" in s3_file:
            cleaned_s3_files.append(s3_file)

    print("Done 1")

    for cleaned_s3_file in cleaned_s3_files:
        s3_object = s3_hook.get_key(cleaned_s3_file, s3_bucket_name)
        s3_file = s3_object.get()['Body'].read().decode('utf-8')
        json_lists = [json.loads(json_str) for json_str in s3_file.strip().split('\n')]

    print("Done 2")    

    for json_list in json_lists:
        for del_column in del_columns:
            json_list.pop(del_column)

    print("Done 3")

    for json_list in json_lists:
        df = pd.DataFrame.from_dict([json_list])
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        mysql_hook.insert_rows(table=mysql_table_name, replace=True, rows=df.values.tolist())

with dag:
    # S3 파일을 MySQL에 적재하는 PythonOperator
    load_s3_file_to_mysql_task = PythonOperator(
        task_id='load_s3_file_to_mysql',
        python_callable=insert_s3_data_bulk,
        queue="celery",
        provide_context=True
    )

insert_s3_data_bulk
