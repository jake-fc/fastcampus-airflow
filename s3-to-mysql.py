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
    aws_conn_id='aws_default',
    s3_source_key='s3://jake-api/api/upbit-api/year=2023/month=04/day=13/hour=12/ls.s3.0674f7c7-8b73-4ec5-81c2-2b991ed4ca31.2023-04-13T12.29.part13.txt',
    mysql_conn_id='mysql_conn',
    mysql_table='upbit_api',
    mysql_duplicate_key_handling='IGNORE',
    mysql_extra_options="""
            FIELDS TERMINATED BY ','
            IGNORE 1 LINES
            """,
    queue="celery",
    dag=dag,
)

s3_to_mysql_task

