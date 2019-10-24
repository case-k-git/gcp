from __future__ import print_function
import datetime
from airflow import models
from airflow.operators import bash_operator

project_id = ''
default_dag_args = {
    'start_date': datetime.datetime(2018, 1, 1),
    'retries': 1
}

with models.DAG(
        'composer_bq',
        schedule_interval='@once',
        default_args=default_dag_args,
        concurrency=1,
        max_active_runs=1) as dag:

    execute_bq = bash_operator.BashOperator(
        task_id='query',
        bash_command="bq query --destination_table [project_id]:tst.dagbq --use_legacy_sql=false 'SELECT word , word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10'",
        dag=dag)
    execute_bq