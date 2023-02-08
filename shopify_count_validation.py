from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from umg.util import rest_api_client,on_job_finish, is_file_unavailable_notify
from airflow.operators.umg import UMGBigQueryOperator
from umg.lineage.dataset import *
from airflow.operators.bash_operator import BashOperator


config = DagConfig('@project.artifactId@', '@dag.version@')

count_val_config = config.get('shopify_count_validation')

docs = """
#### DAG Details

**Documentation:** [Wiki Link](https://umusic.atlassian.net/wiki/spaces/DataEngineering/pages/212205613/Shopify+Count+Validation+Pipeline)
"""

default_args = {
    'owner': 'Gautham Nair',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 16),
    'email': count_val_config['notification_email'],
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
        config.dag_name('count-validtn'),
        catchup=False,
        max_active_runs=1,
        schedule_interval=None if config.get('schedule_interval')['count_validation'] == 'None' else config.get('schedule_interval')['count_validation'],
        default_args=default_args) as dag:

    dag.doc_md = docs

    download_shopify_counts = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-counts'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ShopifyCountValidation',
        options={
            'inputPath': '',
            'labels': config.labels(),
            'outputPath': count_val_config['gs_raw_bucket'] + count_val_config['outputPath'].format('{{ds}}') + '/counts/',
            'reportDate': '{{ds}}',
            'reportTime': '{{ts}}',
            'workerMachineType': 'n1-highcpu-4',
            'numWorkers': '4',
            'maxNumWorkers': '6',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'reportPath': count_val_config['gs_raw_bucket'] + count_val_config['reportPath'].format('{{ds}}')+ '/report/',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': count_val_config['BQSyncFlag']
        },
        retries=2,
        dag=dag)

    init_counts = UMGBigQueryOperator(
        task_id=config.task_name('init_counts'),
        sql='sql/batch/init_counts_download.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': count_val_config['bq_project'], 'bq_dataset': count_val_config['bq_dataset'], 'counts_table': count_val_config['counts_table']},
        dag=dag
    )

    load_to_bq_counts = BashOperator(
        task_id=config.task_name('load-to-bq-counts'),
        bash_command="""
                    set -e
                    echo "load shopify counts..."
                    bq load --ignore_unknown_values --replace=false --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{counts_table}\${date} {gs_raw_bucket}{shopify_source_path}/counts/*
                    """.format(bq_project=count_val_config['bq_project'],
                               bq_dataset=count_val_config['bq_dataset'],
                               counts_table=count_val_config['counts_table'],
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=count_val_config['gs_raw_bucket'],
                               shopify_source_path=count_val_config['outputPath'].format('{{ds}}')),
        dag=dag)

    init_count_errors = UMGBigQueryOperator(
        task_id=config.task_name('init_count_errors'),
        sql='sql/batch/init_count_errors.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': count_val_config['bq_project'], 'bq_dataset': count_val_config['bq_dataset'], 'error_table': count_val_config['error_table']},
        dag=dag
    )

    load_to_bq_errors = BashOperator(
        task_id=config.task_name('load-to-bq-errors'),
        bash_command="""
                    set -e
                    echo "load shopify count api errors..."
                    bq load --ignore_unknown_values --replace=false --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{error_table}\${date} {gs_raw_bucket}{shopify_source_path}/report/*
                    """.format(bq_project=count_val_config['bq_project'],
                               bq_dataset=count_val_config['bq_dataset'],
                               error_table=count_val_config['error_table'],
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=count_val_config['gs_raw_bucket'],
                               shopify_source_path=count_val_config['reportPath'].format('{{ds}}')),
        dag=dag)

    finish = on_job_finish(subscribers=count_val_config['notification_email'],
                           variables={"service_impacted": "Shopify Counts Download Daily"},
                           dag=dag)



    download_shopify_counts >> init_counts >> load_to_bq_counts >> init_count_errors >> load_to_bq_errors >> finish
