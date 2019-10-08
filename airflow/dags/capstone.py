from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'owner': 'Fernando Carneiro',
    'retries': 3, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=60), # Retries happen every 60 minutes
    'start_date': datetime(2016, 1, 1),
    'email_on_retry': False, # Do not email on retry
}

dag = DAG('data_engineering_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin',  dag=dag)

immigration_to_redshift = StageToRedshiftOperator(
    task_id='Immigration_Fact_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'data-engineer-capstone',
    s3_prefix = 'immigration.parquet',
    schema_to = 'public',
    table_to = 'immigration',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

country_to_redshift = StageToRedshiftOperator(
    task_id='Country_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'data-engineer-capstone',
    s3_prefix = 'country.parquet',
    schema_to = 'public',
    table_to = 'country',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

state_to_redshift = StageToRedshiftOperator(
    task_id='State_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'data-engineer-capstone',
    s3_prefix = 'state.parquet',
    schema_to = 'public',
    table_to = 'state',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

date_to_redshift = StageToRedshiftOperator(
    task_id='Date_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'data-engineer-capstone',
    s3_prefix = 'date.parquet',
    schema_to = 'public',
    table_to = 'date',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Data_Quality_Checks',
    redshift_conn_id = "redshift",
    tables=['immigration', 'country', 'state', 'date'],
    dag=dag
)

end_operator = DummyOperator(task_id='End',  dag=dag)

start_operator >> immigration_to_redshift
immigration_to_redshift >> country_to_redshift
immigration_to_redshift >> state_to_redshift
immigration_to_redshift >> date_to_redshift
country_to_redshift >> run_quality_checks
state_to_redshift >> run_quality_checks
date_to_redshift >> run_quality_checks
run_quality_checks >> end_operator
