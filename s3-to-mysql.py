from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.transfers.s3_to_mysql import S3ToMySqlOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 12),
    'retry_delay': timedelta(minutes=10),
}



dag = DAG(
    's3_to_rds_mysql',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


s3_to_mysql_task = S3ToMySqlOperator(
    task_id='s3_to_mysql',
    s3_bucket='jake-api',
    s3_source_key='api/upbit-api/year={{ ds_nodash[:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:8] }}/hour={{ ds_nodash[8:10] }}/data.txt',
    mysql_conn_id='mysql_conn',
    mysql_table='upbit_api',
    dag=dag,
)

s3_to_mysql_task

