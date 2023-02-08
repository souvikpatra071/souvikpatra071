from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from umg.util import rest_api_client,on_job_finish, is_file_unavailable_notify
from airflow.operators.umg import UMGBigQueryOperator
from umg.lineage.dataset import *
from umg.mail.mailjet import send_alert
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.umg import UMGDataFlowJavaOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql','.bql')

config = DagConfig('@project.artifactId@', '@dag.version@')
shopify_daily_config = config.get('shopify_daily_download_v2')

default_args = {
    'owner': 'Madhurya Malladi',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 23),
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
        config.dag_name('daily_download_v2'),
        catchup=False,
        max_active_runs=2,
        schedule_interval= None if config.get('schedule_interval')['daily_download_v2'] == 'None' else config.get('schedule_interval')['daily_download_v2'],
        default_args=default_args) as dag:
        

    download_shopify_customers = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-customers'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.DailyV2Scraper',
        options={
            'inputPath' : '',
            'labels': config.labels(),
            'outputPath': shopify_daily_config['gs_raw_bucket'] + shopify_daily_config['outputPath'].format('{{ds}}') + '/batch1/',
            'startDate': '{{ds}}',
            'endDate': '{{tomorrow_ds}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '6',
            'maxNumWorkers': '16',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': shopify_daily_config['gs_raw_bucket'] + shopify_daily_config['reportPath'].format('{{ds}}')+ '/batch1/' + 'report/',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('daily_v2_BQSyncFlag')
        },
        retries=5,
        dag=dag)


    init_customers = UMGBigQueryOperator(
        task_id=config.task_name('init_customers'),
        sql='sql/batch/init_customers_v1.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': shopify_daily_config['bq_project'], 'bq_dataset': shopify_daily_config['bq_dataset'],'customers_table': shopify_daily_config['customers_table']},
        dag=dag
    )


    load_to_bq_customers = BashOperator(
        task_id=config.task_name('load-to-bq-customers'),
        bash_command="""
                    set -e
                    echo "load customers..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{customers_table}\${date} {gs_raw_bucket}{shopify_source_path}/batch1/customers/*
                    """.format(bq_project=shopify_daily_config['bq_project'],
                               bq_dataset=shopify_daily_config['bq_dataset'],
                               customers_table=shopify_daily_config['customers_table'],
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=shopify_daily_config['gs_raw_bucket'],
                               shopify_source_path=shopify_daily_config['outputPath'].format('{{ds}}')),
        dag=dag)

    download_shopify_customers >> init_customers >> load_to_bq_customers
