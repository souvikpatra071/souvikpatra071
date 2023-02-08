from __future__ import nested_scopes
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from airflow.operators.bash_operator import BashOperator
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/nallangollajagadeesh/Downloads/jagadeesh-361306-7fc06e4c6471.json'

default_args = {
    'owner':'jagadeesh',
    'start_date' : datetime(2022,9,21),
    'depends_on_past': False
}
dag=DAG('new_struct_method',default_args=default_args,schedule_interval='@daily')
# def nested_table():
#     credentials = service_account.Credentials.from_service_account_file('/Users/nallangollajagadeesh/Downloads/jagadeesh-361306-7fc06e4c6471.json')
#     project_id = 'jagadeesh-361306'
#     client = bigquery.Client(credentials= credentials,project=project_id)
#     client=bigquery.Client()
#     results=query_job.result().to_dataframe()
table_make_Opetrator = BashOperator(
    task_id='make-table',
    bash_command="""
        set -e
            echo "make-table..."
            bq mk --table jagadeesh-361306.data.gutta_hari gs://jagadeesh-361306.appspot.com/jaggu/schema.json
            """,
    dag=dag
)
table_load_operator = BashOperator(
    task_id='load-table',
    bash_command="""
        set -e
            echo "load table..."
            bq load --source_format=NEWLINE_DELIMITER_JSON jagadeesh-361306.data.samples_nest gs://jagadeesh-361306.appspot.com/jaggu/data.json
            """,
    dag=dag
)      
table_make_Opetrator >> table_load_operator



