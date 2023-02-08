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

config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Pei Chen',
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
        config.dag_name('batch_daily_product_metadata'),
        catchup=True,
        max_active_runs=2,
        schedule_interval= None if config.get('schedule_interval')['daily'] == 'None' else config.get('schedule_interval')['daily'],
        default_args=default_args) as dag:


    download_product_metadata = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-product-metadata'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ProductMetadataScraper',
        options={
            'labels': config.labels(),
            'inputPath': '',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v3_0/report_date={{ds_nodash}}/metadata/',
            'reportDate': '{{ds}}',
            'workerMachineType': 'n1-standard-1',
            'numWorkers': '25',
            'maxNumWorkers': '50',
            'vaultUrl':config.get("central_vault_host"),
            'shopsType' : 'soundofvinyl',
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v3_0/report_date={{ds_nodash}}/metadata/report/',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account')
        },
        retries=5,
        dag=dag)
    
    init_product_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_product_metafields'),
        sql='sql/batch/init_product_metafields_v2.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_product_variant_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_product_variant_metafields'),
        sql='sql/batch/init_product_variant_metafields_v2.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )


    init_metadata_error_report = UMGBigQueryOperator(
        task_id=config.task_name('init_metadata_error_report'),
        sql='sql/batch/init_metadata_error_report_v2.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )


    load_to_bq_product_metafields = BashOperator(
        task_id=config.task_name('load-to-bq-product-metafields'),
        bash_command="""
                    set -e
                    echo "load product-metafields..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.product_metafields_batch_v2\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/metadata/product_metadata/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v3')),
        dag=dag)
    
    
    load_to_bq_product_variant_metafields = BashOperator(
        task_id=config.task_name('load-to-bq-product-variant-metafields'),
        bash_command="""
                    set -e
                    echo "load product-metafields..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.product_variant_metafields_batch_v2\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/metadata/product_variant_metadata/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v3')),
        dag=dag)

    load_metadata_temp_error_report = BashOperator(
        task_id=config.task_name('load-metadata-temp-error-report'),
        bash_command="""
                    set -e
                    echo "load error report..."
                    bq load --ignore_unknown_values --replace --source_format=CSV {bq_project}:shopify_source.metadata_temp_error_report_v2 {gs_raw_bucket}{shopify_source_path}report_date={date}/metadata/report/*
                    """.format(bq_project=config.get('bq_project'),
                                dag_dir=config.dag_dir(),
                                date='{{ds_nodash}}',
                                gs_raw_bucket=config.get('gs_raw_bucket'),
                                shopify_source_path=config.get('shopify_source_path_v3')),
        dag=dag)

    load_metadata_error_report = UMGBigQueryOperator(
        task_id=config.task_name('load-metadata-error-report'),
        sql='''
            SELECT 
            split(string_field_0, ' ')[OFFSET(0)] url, 
            ltrim(split(string_field_0, '/graphql.json')[OFFSET(1)]) error_message, 
            DATE('{{ds}}') umg_report_date  
            FROM `{{params.bq_project}}.shopify_source.metadata_temp_error_report_v2` where string_field_0 like '%https://%'
        ''',
        use_legacy_sql=False,
        params={
            'bq_project': config.get('bq_project')
        },     
        destination_dataset_table = config.get('bq_project') + '.shopify_source.metadata_error_report',
        write_disposition='WRITE_APPEND',
        dag=dag
    )

    remove_temp_reports = BashOperator(
        task_id=config.task_name('remove-temp-reports'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:shopify_source.metadata_temp_error_report_v2
                     """.format(bq_project=config.get('bq_project'),
                                dag_dir=config.dag_dir()),
        dag=dag)
	
    load_reporting_shopify_products_fields = UMGBigQueryOperator(
         task_id=config.task_name('load_reporting_shopify_products_metafield'),
         sql='sql/ecommerce-reporting/shopify_product_metafields.sql',
         destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
                                    dataset=config.get('bq_shopify_dataset'), table='shopify_product_metafields'),
         write_disposition='WRITE_TRUNCATE',
		 params={
            'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),'bq_source_dataset':config.get('dataset')
        },
         allow_large_results=True,
       use_legacy_sql=False,
    
        dag=dag
         )
    
    # load_reporting_jp_shopify_products_fields = UMGBigQueryOperator(
    #      task_id=config.task_name('load_reporting_jp_shopify_products_metafield'),
    #      sql='sql/ecommerce-reporting/shopify_product_metafields_jp.sql',
    #      destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
    #                                 dataset=config.get('bq_shopify_dataset'), table='jp_shopify_metafields'),
    #      write_disposition='WRITE_TRUNCATE',
    #      params={
    #         'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),'bq_source_dataset':config.get('dataset')
    #     },
    #      allow_large_results=True,
    #    use_legacy_sql=False,
    # 
    #     dag=dag
    #      )

    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Daily Product Metadata"},
                           dag=dag)
    
    download_product_metadata >> init_product_metafields >> load_to_bq_product_metafields 
    download_product_metadata >> init_product_variant_metafields >> load_to_bq_product_variant_metafields 
    [load_to_bq_product_metafields, load_to_bq_product_variant_metafields] >> load_reporting_shopify_products_fields >> finish
    # [load_to_bq_product_metafields, load_to_bq_product_variant_metafields] >> load_reporting_jp_shopify_products_fields >> finish
    download_product_metadata >> init_metadata_error_report >> load_metadata_temp_error_report >> load_metadata_error_report >> remove_temp_reports >> finish






