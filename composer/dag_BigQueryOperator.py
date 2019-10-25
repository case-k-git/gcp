from __future__ import print_function
import datetime
from airflow import models
import codecs
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

"""
#!/bin/bash
BUCKET=''

gcloud composer environments create composer2  --location asia-northeast1

gcloud composer environments storage plugins import \
  --environment composer1  --location asia-northeast1  \
  --source gs://$BUCKET/media/sql/

gcloud composer environments storage dags import \
  --environment composer1  --location asia-northeast1  \
  --source gs://$BUCKET/media/dag_BigQueryOperator.py
"""

project_id = ''
default_dag_args = {
    'start_date': datetime.datetime(2018, 1, 1),
    'retries': 1
}

with codecs.open('/home/airflow/gcs/plugins/sql/data_source_a.sql', 'r', 'utf-8') as f_a:
 data_source_a_query = f_a.read()

with models.DAG(
        'composer_bq',
        schedule_interval='@once',
        default_args=default_dag_args,
        concurrency=1,
        max_active_runs=1) as dag:

	execute_bq = BigQueryOperator(
	   task_id='data_source_a',
	   write_disposition='WRITE_TRUNCATE',
	   create_disposition='CREATE_IF_NEEDED',
	   allow_large_results=True,
	   bql=data_source_a_query,
	   use_legacy_sql=False,
	   destination_dataset_table='{}.tst.data_source_a'.format(project_id),
	   dag=dag
	 )
	execute_bq