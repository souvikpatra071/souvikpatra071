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
products_download_config = config.get('shopify_products_download')

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
        config.dag_name('products_download_v2'),
        catchup=False,
        max_active_runs=2,
        schedule_interval= None if config.get('schedule_interval')['products_download'] == 'None' else config.get('schedule_interval')['products_download'],
        default_args=default_args) as dag:
        

    download_shopify_products = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-products'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ProductsScraper',
        options={
            'inputPath' : '',
            'labels': config.labels(),
            'outputPath': products_download_config['gs_raw_bucket'] + products_download_config['outputPath'].format('{{ds}}')+ '/products/',
            'startDate': '{{ds}}',
            'endDate': '{{tomorrow_ds}}',
            'workerMachineType': 'n1-standard-1',
            'numWorkers': '6',
            'maxNumWorkers': '16',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': products_download_config['gs_raw_bucket'] + products_download_config['reportPath'].format('{{ds}}')+ '/report/',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('products_BQSyncFlag')
        },
        retries=5,
        dag=dag)


    init_products = UMGBigQueryOperator(
        task_id=config.task_name('init_products'),
        sql='sql/batch/init_products_download.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': products_download_config['bq_project'], 'bq_dataset': products_download_config['bq_dataset'],'products_table': products_download_config['products_table']},
        dag=dag
    )

    load_to_bq_products = BashOperator(
        task_id=config.task_name('load-to-bq-products'),
        bash_command="""
                    set -e
                    echo "load products..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{products_table}\${date} {gs_raw_bucket}{shopify_source_path}/products/*
                    """.format(bq_project=products_download_config['bq_project'],
                               bq_dataset=products_download_config['bq_dataset'],
                               products_table=products_download_config['products_table'],
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=products_download_config['gs_raw_bucket'],
                               shopify_source_path=products_download_config['outputPath'].format('{{ds}}')),
        dag=dag)

    init_shopify_products_temp = UMGBigQueryOperator(
        task_id='init_products_temp',
        sql='sql/ecommerce-reporting/init_shopify_products.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': products_download_config['bq_project'],'bq_dataset': products_download_config['bq_ecomm_reporting_dataset'],
                'bq_table': products_download_config['bq_ecomm_shopify_products_temp'] },
        dag=dag
    )

    load_shopify_products_temp = BashOperator(
        task_id=config.task_name('load_shopify_products_temp'),
        bash_command="""
                    set -e
                    echo "load products..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table} {gs_raw_bucket}{shopify_source_path}/products/*
                    """.format(bq_project=products_download_config['bq_project'],
                               bq_dataset=products_download_config['bq_ecomm_reporting_dataset'],
                               bq_table=products_download_config['bq_ecomm_shopify_products_temp'],
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=products_download_config['gs_raw_bucket'],
                               shopify_source_path=products_download_config['outputPath'].format('{{ds}}')),
        dag=dag
    )

    init_shopify_products = UMGBigQueryOperator(
        task_id='init_shopify_products',
        sql='sql/ecommerce-reporting/init_shopify_products.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': products_download_config['bq_project'],'bq_dataset': products_download_config['bq_ecomm_reporting_dataset'],
                'bq_table':products_download_config['bq_ecomm_shopify_products']},
        dag=dag
    )

    merge_shopify_products = UMGBigQueryOperator(
        task_id=config.task_name('merge_shopify_products_temp'),
        sql='sql/ecommerce-reporting/shopify_products.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': products_download_config['bq_project'],
                'bq_dataset': products_download_config['bq_ecomm_reporting_dataset'],
                'bq_source_table': products_download_config['bq_ecomm_shopify_products_temp'],
                'bq_table': products_download_config['bq_ecomm_shopify_products']

                },
        dag=dag
    )

    delete_products_temp = BashOperator(
        task_id=config.task_name('remove-temp-products'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:{bq_dataset}.{bq_table}
                     """.format(bq_project=products_download_config['bq_project'],
                                bq_dataset=products_download_config['bq_ecomm_reporting_dataset'],
                                bq_table=products_download_config['bq_ecomm_shopify_products_temp']),
        dag=dag)



    finish = on_job_finish(subscribers=products_download_config['notification_email'],
                           variables={"service_impacted": "Shopify Products Download Daily"},
                           dag=dag)


    download_shopify_products >> init_products >> load_to_bq_products >> finish
    download_shopify_products >> init_shopify_products_temp >> load_shopify_products_temp >> init_shopify_products >> merge_shopify_products >> delete_products_temp >> finish