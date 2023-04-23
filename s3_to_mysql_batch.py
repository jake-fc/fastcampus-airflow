from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

import pandas as pd
import io
import json

default_args = {
    'owner': 'jake',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 10),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_to_mysql_batch',
    description='S3에서 데이터를 로드한 후 MySQL로 데이터를 인서트한다',
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=True
)

s3_bucket_name = 'jake-api'
mysql_table_name = 'upbit_api'
mysql_conn_id = 'mysql_conn'
s3_conn_id = 'aws_default'

    
def insert_s3_data_bulk(execution_date, **context):
    ds_nodash = execution_date.strftime('%Y%m%d%H')
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_prefix = f'api/upbit-api/year={ds_nodash[:4]}/month={ds_nodash[4:6]}/day={ds_nodash[6:8]}/hour={ds_nodash[8:10]}/'
    s3_files = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix = s3_prefix)
    
    print(f'Today is {ds_nodash}')

    print(s3_prefix)
    print(s3_files)
    
    json_lists = []
    cleaned_s3_files = []
    for s3_file in s3_files :
        if "year=2023" in s3_file:
            cleaned_s3_files.append(s3_file)

    print("Done 1")

    for cleaned_s3_file in cleaned_s3_files:
        s3_object = s3_hook.get_key(cleaned_s3_file, s3_bucket_name)
        s3_file = s3_object.get()['Body'].read().decode('utf-8')
        json_lists += [json.loads(json_str) for json_str in s3_file.strip().split('\n')]

    print("Done 2")    
    
    del_columns = ["s3_bucket","idx_name","s3_path","@version","@timestamp"]
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
