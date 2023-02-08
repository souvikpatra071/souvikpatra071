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
from airflow.operators.dummy_operator import DummyOperator

config = DagConfig('@project.artifactId@', '@dag.version@')


default_args = {
    'owner': 'Ratul',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 17),
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
        config.dag_name('metadata_bulk_operation_all'),
        catchup=False,
        max_active_runs=1,
        schedule_interval= None if config.get('schedule_interval')['bulk_operation_all'] == 'None' else config.get('schedule_interval')['bulk_operation_all'],
        default_args=default_args) as dag:

    ####



    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Metadata Bulk Operation"},
                           dag=dag)

    trigger_shopify_bulk_download = BashOperator(
        task_id=config.task_name('trigger-metadata_bulk_download-all'),
        bash_command = "airflow trigger_dag '{dag_id}' -e '{date}'".format(
            dag_id = config.dag_name(data_source='metadata_bulk_download-all'),
            date = '{{ ds }}'

        ),
        dag = dag
    )



    download_orders_metadata = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('shopify-metadata-bulk-operation', 'orders'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.MetadataBulkScraper',
        options={
            'labels': config.labels(),
            'inputPath': '',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.bulk.v2_0/report_date={{ds_nodash}}/orders/metadata/bulk_mutation_response/',
            'reportDate': '{{ds}}',
            'startDate': '{{ macros.ds_add(ds, -1) }}',
            'resourceType': 'orders',
            'orderMetafieldsApisToFetch': config.get('order_metafields_apis_to_fetch'),
            'workerMachineType': 'n1-standard-1',
            'numWorkers': '2',
            'maxNumWorkers': '3',
            'processType': 'BULK_OPERATION',
            'vaultUrl':config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('bulk_operation_all')
        },
        # retries=5,
        dag=dag)

    init_orders_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_bulk_mutation_response', 'orders'),
        sql='sql/batch/init_bulk_mutation_orders_response.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table': config.get('orders')['bulk_mutation_response_table']},
        dag=dag
    )

    load_to_bq_orders_metafields = BashOperator(
        task_id=config.task_name('load-to-bq-bulk_mutation_response', 'orders'),
        bash_command="""
                    set -e
                    echo "load bulk mutation response ..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table}\${date} {gs_raw_bucket}source/shopify.batch.bulk.v2_0/report_date={date}/{resource_type}/metadata/bulk_mutation_response/*
                    """.format(bq_project=config.get('bq_project'),
                               bq_dataset = 'shopify_source',
                               bq_table = config.get('orders')['bulk_mutation_response_table'],
                               resource_type = 'orders',
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               ),
        dag=dag)

    bulk_operation_completion_orders = DummyOperator(
        task_id=config.task_name('bulk_operation_completion-orders'),
        dag=dag)

    download_customers_metadata = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('shopify-metadata-bulk-operation', 'customers'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.MetadataBulkScraper',
        options={
            'labels': config.labels(),
            'inputPath': '',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.bulk.v2_0/report_date={{ds_nodash}}/customers/metadata/bulk_mutation_response/',
            'reportDate': '{{ds}}',
            'startDate': '{{ macros.ds_add(ds, -1) }}',
            'resourceType': 'customers',
            'workerMachineType': 'n1-standard-1',
            'numWorkers': '2',
            'maxNumWorkers': '3',
            'processType': 'BULK_OPERATION',
            'vaultUrl':config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('bulk_operation_all')
        },
        # retries=5,
        dag=dag)

    init_customers_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_bulk_mutation_response', 'customers'),
        sql='sql/batch/init_bulk_mutation_customers_response.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table': config.get('customers')['bulk_mutation_response_table']},
        dag=dag
    )

    load_to_bq_customers_metafields = BashOperator(
        task_id=config.task_name('load-to-bq-bulk_mutation_response', 'customers'),
        bash_command="""
                    set -e
                    echo "load bulk mutation response ..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table}\${date} {gs_raw_bucket}source/shopify.batch.bulk.v2_0/report_date={date}/{resource_type}/metadata/bulk_mutation_response/*
                    """.format(bq_project=config.get('bq_project'),
                               bq_dataset = 'shopify_source',
                               bq_table = config.get('customers')['bulk_mutation_response_table'],
                               resource_type = 'customers',
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               ),
        dag=dag)

    bulk_operation_completion_customers = DummyOperator(
            task_id=config.task_name('bulk_operation_completion-customers'),
            dag=dag)

    download_collections_metadata = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('shopify-metadata-bulk-operation', 'collections'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.MetadataBulkScraper',
        options={
            'labels': config.labels(),
            'inputPath': '',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.bulk.v2_0/report_date={{ds_nodash}}/collections/metadata/bulk_mutation_response/',
            'reportDate': '{{ds}}',
            'startDate': '{{ macros.ds_add(ds, -1) }}',
            'resourceType': 'collections',
            'workerMachineType': 'n1-standard-1',
            'numWorkers': '2',
            'maxNumWorkers': '3',
            'processType': 'BULK_OPERATION',
            'vaultUrl':config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('bulk_operation_all')
        },
        # retries=5,
        dag=dag)

    init_collections_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_bulk_mutation_response', 'collections'),
        sql='sql/batch/init_bulk_mutation_collections_response.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table': config.get('collections')['bulk_mutation_response_table']},
        dag=dag
    )

    load_to_bq_collections_metafields = BashOperator(
        task_id=config.task_name('load-to-bq-bulk_mutation_response', 'collections'),
        bash_command="""
                    set -e
                    echo "load bulk mutation response ..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table}\${date} {gs_raw_bucket}source/shopify.batch.bulk.v2_0/report_date={date}/{resource_type}/metadata/bulk_mutation_response/*
                    """.format(bq_project=config.get('bq_project'),
                               bq_dataset = 'shopify_source',
                               bq_table = config.get('collections')['bulk_mutation_response_table'],
                               resource_type = 'collections',
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               ),
        dag=dag)

    bulk_operation_completion_collections = DummyOperator(
        task_id=config.task_name('bulk_operation_completion-collections'),
        dag=dag)



    download_orders_metadata >> init_orders_metafields >> load_to_bq_orders_metafields >> bulk_operation_completion_orders

    bulk_operation_completion_orders >> download_customers_metadata >> init_customers_metafields >> load_to_bq_customers_metafields >> bulk_operation_completion_customers

    bulk_operation_completion_customers >> download_collections_metadata >> init_collections_metafields >> load_to_bq_collections_metafields >> bulk_operation_completion_collections

    bulk_operation_completion_collections >> trigger_shopify_bulk_download >> finish

