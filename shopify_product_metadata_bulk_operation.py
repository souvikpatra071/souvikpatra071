from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from umg.util import  rest_api_client,on_job_finish
from airflow.operators.umg import UMGBigQueryOperator
from umg.lineage.dataset import *
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.umg import UMGDataFlowJavaOperator
from airflow.models import Variable
from umg.util import on_job_finish

config = DagConfig('@project.artifactId@', '@dag.version@')


default_args = {
    'owner': 'Ratul',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 23),
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 2,
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
        config.dag_name('product_metadata_bulk_operation'),
        catchup=False,
        max_active_runs=1,
        schedule_interval= None if config.get('schedule_interval')['daily_bulk_operation'] == 'None' else config.get('schedule_interval')['daily_bulk_operation'],
        default_args=default_args) as dag:

    ####
    download_product_metadata = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('shopify-product-metadata-bulk-operation'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ProductMetadataBulkScraper',
        options={
            'labels': config.labels(),
            'inputPath': '',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.bulk.v1_0/report_date={{ds_nodash}}/metadata/bulk_mutation_response/',
            'reportDate': '{{ds}}',
            'workerMachineType': 'n1-standard-1',
            'numWorkers': '2',
            'maxNumWorkers': '3',
            'processType': 'BULK_OPERATION',
            'vaultUrl':config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('productmetadata_bulk_BQSyncFlag')
        },
        # retries=5,
        dag=dag)

    init_product_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_bulk_mutation_response'),
        sql='sql/batch/init_bulk_mutation_response.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    load_to_bq_product_metafields = BashOperator(
        task_id=config.task_name('load-to-bq-bulk_mutation_response'),
        bash_command="""
                    set -e
                    echo "load bulk mutation response ..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.bulk_mutation_response\${date} {gs_raw_bucket}source/shopify.batch.bulk.v1_0/report_date={date}/metadata/bulk_mutation_response/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               ),
        dag=dag)



    trigger_shopify_bulk_download = BashOperator(
        task_id=config.task_name('trigger_product_metadata_downlaod_1'),
        bash_command = "airflow trigger_dag '{dag_id}' -e '{date}'".format(
            dag_id = config.dag_name(data_source='product_metadata_bulk_download'),
            date='{{ ds }}'),
        dag = dag
    )

    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Product Metadata Bulk Operation"},
                           dag=dag)


    download_product_metadata >> init_product_metafields >> load_to_bq_product_metafields >>  trigger_shopify_bulk_download >> finish

