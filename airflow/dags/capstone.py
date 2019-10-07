from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'owner': 'Fernando Carneiro',
    'retries': 0, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=60), # Retries happen every 5 minutes
    'start_date': datetime(2019, 10, 1),
    'email_on_retry': False, # Do not email on retry
}

dag = DAG('data_engineering_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@yearly'
        )

start_operator = DummyOperator(task_id='Begin',  dag=dag)

staging_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Immigration_Data',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'data-engineer-capstone',
    s3_prefix = 'immigration.parquet',
    schema_to = 'public',
    table_to = 'staging_immigration',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

staging_country_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Country_Data',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'data-engineer-capstone',
    s3_prefix = 'country.parquet',
    schema_to = 'public',
    table_to = 'staging_country',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

staging_state_to_redshift = StageToRedshiftOperator(
    task_id='Stage_State_Data',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'data-engineer-capstone',
    s3_prefix = 'state.parquet',
    schema_to = 'public',
    table_to = 'staging_state',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

end_operator = DummyOperator(task_id='End',  dag=dag)

start_operator >> staging_immigration_to_redshift
start_operator >> staging_country_to_redshift
start_operator >> staging_state_to_redshift
staging_immigration_to_redshift >> end_operator
staging_country_to_redshift >> end_operator
staging_state_to_redshift >> end_operator
