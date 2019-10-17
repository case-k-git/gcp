from __future__ import print_function
import datetime
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow import models
#from airflow.operators import bash_operator
#from airflow.operators import python_operator

project_id = ''
default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2018, 1, 1),
    'retries': 1,
    'dataflow_default_options': {
        'project': project_id,
        #'region': 'europe-west1',
        #'zone': 'europe-west1-d',
        #'tempLocation': 'gs://{}/template/custom_template_1008',
    }
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.

template_path = 'gs://{}/template/GCS_TO_GCS_5'.format(project_id)
with models.DAG(
        'composer_dataflowtemplate1016_3',
        schedule_interval='@daily',
        default_args=default_dag_args,
        concurrency=1,
        max_active_runs=1) as dag:

    execute_dataflow_1 = DataflowTemplateOperator(
        task_id='datapflow_example1',
        template=template_path,
        parameters={
            'inputFile': "gs://{}/sample.csv".format(project_id),
            'outputFile': "gs://{}/composer_output/sample_1.csv".format(project_id),
        },
        dag=dag)
    execute_dataflow_2 = DataflowTemplateOperator(
        task_id='datapflow_example2',
        template=template_path,
        parameters={
            'inputFile': "gs://{}/composer_output/sample_1.csv".format(project_id),
            'outputFile': "gs://{}/composer_output/sample_2.csv".format(project_id),
        },
        dag=dag)
    execute_dataflow_3 = DataflowTemplateOperator(
        task_id='datapflow_example3',
        template=template_path,
        parameters={
            'inputFile': "gs://{}/composer_output/sample_2.csv".format(project_id),
            'outputFile': "gs://{}/composer_output/sample_3.csv".format(project_id),
        },
        dag=dag)
    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    execute_dataflow_1 >> execute_dataflow_2 >> execute_dataflow_3