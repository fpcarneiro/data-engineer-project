from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

import datetime
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 aws_conn_id,
                 redshift_conn_id,
                 s3_from,
                 s3_prefix,
                 schema_to,
                 table_to,
                 options,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_from = s3_from
        self.s3_prefix = s3_prefix
        self.schema = schema_to
        self.table = table_to
        self.options = options
        self.autocommit = True
        self.region = 'us-west-2'

    def execute(self, context):
        self.log.info('Initializing COPY procedure...')        
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_options = '\n\t\t\t'.join(self.options)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            IAM_ROLE 'arn:aws:iam::900646315604:role/myRedshiftRole'
            {copy_options};
        """.format(schema=self.schema,
                   table=self.table,
                   s3_bucket=self.s3_from,
                   s3_key=self.s3_prefix,
                   copy_options=copy_options)

        self.log.info(f'Executing COPY command from bucket s3://{self.s3_from}/{self.s3_prefix} to {self.schema}.{self.table} in Redshift')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete!")