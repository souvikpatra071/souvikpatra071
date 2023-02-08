from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from umg.util import on_job_finish
from airflow.operators.umg import UMGBigQueryOperator
from umg.lineage.dataset import *
from airflow.operators.bash_operator import BashOperator

config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Gutta Harikrishna',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 1),
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'dataflow_default_options': {
        'region': config.get('df-region'),
        'project': config.get('workflow_project'),
        'tempLocation': '{}shopify/dataflow/tmp/'.format(config.get('temp_bucket')),
        'gcpTempLocation': '{}shopify/dataflow/tmp/'.format(config.get('temp_bucket')),
        'autoscalingAlgorithm': 'THROUGHPUT_BASED',
        'serviceAccount': config.get('df-service-account')
    }
}

with models.DAG(
        config.dag_name('shopify-finance-hourly'),
        catchup=False,
        max_active_runs=2,
        schedule_interval= None if config.get('schedule_interval')['shopify_ecommerce_hourly'] == 'None' else config.get('schedule_interval')['shopify_ecommerce_hourly'],
        default_args=default_args) as dag:
    
    init_promised_dates = UMGBigQueryOperator(
        task_id=config.task_name('init_promised_dates'),
        sql='sql/ecommerce-reporting/init_promised_dates.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('bq_shopify_dataset')},
        dag=dag
    )

    load_promised_dates = UMGBigQueryOperator(
        task_id=config.task_name('load_promised_dates'),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        write_disposition="WRITE_TRUNCATE",
        sql='sql/ecommerce-reporting/load_promised_dates.sql',
        destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
                                    dataset=config.get('bq_shopify_dataset'), table='shopify_promised_dates'),
        allow_large_results=True,
        dag=dag
    )

    init_main_finance = UMGBigQueryOperator(
        task_id=config.task_name('init_main_finance'),
        sql='sql/ecommerce-reporting/init_main_finance.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('bq_shopify_dataset')},
        dag=dag
    )

    load_main_finance = UMGBigQueryOperator(
        task_id=config.task_name('load_main_finance'),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        sql='sql/ecommerce-reporting/load_main_finance.sql',
        destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
                                    dataset=config.get('bq_shopify_dataset'), table='shopify_main_finance'),
        allow_large_results=True,
        dag=dag
    )


    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Ecommerce Hourly"},
                           dag=dag)


    
    init_promised_dates >> load_promised_dates >> finish
    init_main_finance  >> load_main_finance >> finish
    