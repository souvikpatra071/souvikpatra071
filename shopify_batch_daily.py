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
        config.dag_name('batch_daily'),
        catchup=False,
        max_active_runs=2,
        schedule_interval= None if config.get('schedule_interval')['daily'] == 'None' else config.get('schedule_interval')['daily'],
        default_args=default_args) as dag:
        
    def skip_check(templates_dict, *args, **kwargs):
      DAY=templates_dict['day']
      if DAY == 'Saturday' or DAY == 'Sunday' :
        return False
      else:
        return True 
        
    skip_check = ShortCircuitOperator(
                      task_id='skip-check-weekend',
                      provide_context=True,
                      python_callable=skip_check,
                      trigger_rule='all_success',
                      templates_dict={
                      'day': '{{ execution_date.format("%A") }}'},
                      dag=dag)

    email_sql = \
        "select notification_emails from `{email_project}.shopify_validation.report_notification_emails` where report_name = '{report_name}'limit 1".format(email_project=config.get('shopify_report_project'
                                                                                                                                                                                     ), report_name=config.get('report_name'))

    download_batch_1 = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-1'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.Scraper',
        options={
            'labels': config.labels(),
            'inputPath': '',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch1/',
            'startDate': '{{ds}}',
            'endDate': '{{tomorrow_ds}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '3',
            'maxNumWorkers': '8',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch1/report/',
            'frequency': 'daily',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_daily_BQSyncFlag')
        },
        retries=5,
        dag=dag)

    download_batch_2 = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-2'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.Scraper',
        options={
            'labels': config.labels(),
            'inputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch1/next_batch/*',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch2/',
            'startDate': '{{ds}}',
            'endDate': '{{tomorrow_ds}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '3',
            'maxNumWorkers': '6',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch2/report/',
            'frequency': 'daily',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_daily_BQSyncFlag')
        },
        retries=5,
        dag=dag)

    download_batch_3 = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-3'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.Scraper',
        options={
            'labels': config.labels(),
            'inputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch2/next_batch/*',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch3/',
            'startDate': '{{ds}}',
            'endDate': '{{tomorrow_ds}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '3',
            'maxNumWorkers': '6',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch3/report/',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_daily_BQSyncFlag')
        },
        retries=5,
        dag=dag)

    download_batch_4 = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-4'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.Scraper',
        options={
            'labels': config.labels(),
            'inputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch1/next_batch4/*',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch4/',
            'startDate': '{{ds}}',
            'endDate': '{{tomorrow_ds}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '3',
            'maxNumWorkers': '6',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/report/batch4/',
            'apiVersion': "{{var.value.shopify_api_version}}",
	          'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_daily_BQSyncFlag')
        },
        retries=5,
        dag=dag)

    download_batch_5 = rest_api_client(
        dataset_type=DatasetType.fact,
        task_id=config.task_name('download-shopify-5'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
        version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.Scraper',
        options={
            'labels': config.labels(),
            'inputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch1/next_batch5/*',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/batch5/',
            'startDate': '{{ds}}',
            'endDate': '{{tomorrow_ds}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '3',
            'maxNumWorkers': '6',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/report/batch5/',
            'apiVersion': "{{var.value.shopify_api_version}}",
	          'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_daily_BQSyncFlag')
        },
        retries=5,
        dag=dag)


    # download_product_metadata = rest_api_client(
    #     dataset_type=DatasetType.fact,
    #     task_id=config.task_name('download-shopify-product-metadata'),
    #     gcp_conn_id='google_cloud_default',
    #     jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
    #         version=config.get('version')),
    #     job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ProductMetadataScraper',
    #     options={
    #         'labels': config.labels(),
    #         'inputPath': '',
    #         'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/metadata/',
    #         'reportDate': '{{ds}}',
    #         'workerMachineType': 'n1-standard-1',
    #         'numWorkers': '25',
    #         'maxNumWorkers': '50',
    #         'vaultUrl': config.get("central_vault_host"),
    #         'vaultRole': config.get("shopify_vault_role"),
    #         'vaultShopifyPath': config.get('shopify_vault_path'),
    #         'shopsType': "nonvinyl",
    #         'apisToSkip': config.get('shopify_apis_to_skip'),
    #         'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0/report_date={{ds_nodash}}/metadata/report/',
    #         'apiVersion': "{{var.value.shopify_api_version}}",
    #         'vaultServiceAccount': config.get('shopify_vault_service_account')
    #     },
    #     retries=5,
    #     dag=dag)

    init_orders_count = UMGBigQueryOperator(
        task_id=config.task_name('init_orders_count'),
        sql='sql/batch/init_orders_count.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_collects = UMGBigQueryOperator(
        task_id=config.task_name('init_collects'),
        sql='sql/batch/init_collects.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_customers = UMGBigQueryOperator(
        task_id=config.task_name('init_customers'),
        sql='sql/batch/init_customers.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_disputes = UMGBigQueryOperator(
        task_id=config.task_name('init_disputes'),
        sql='sql/batch/init_disputes.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_events = UMGBigQueryOperator(
        task_id=config.task_name('init_events'),
        sql='sql/batch/init_events.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_gift_cards = UMGBigQueryOperator(
        task_id=config.task_name('init_gift_cards'),
        sql='sql/batch/init_gift_cards.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_inventory_items = UMGBigQueryOperator(
        task_id=config.task_name('init_inventory_items'),
        sql='sql/batch/init_inventory_items.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_inventory_levels = UMGBigQueryOperator(
        task_id=config.task_name('init_inventory_levels'),
        sql='sql/batch/init_inventory_levels.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_locations = UMGBigQueryOperator(
        task_id=config.task_name('init_locations'),
        sql='sql/batch/init_locations.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_metafields'),
        sql='sql/batch/init_metafields.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_payouts = UMGBigQueryOperator(
        task_id=config.task_name('init_payouts'),
        sql='sql/batch/init_payouts.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_payouts_transactions = UMGBigQueryOperator(
        task_id=config.task_name('init_payouts_transactions'),
        sql='sql/batch/init_payout_transactions.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_price_rules = UMGBigQueryOperator(
        task_id=config.task_name('init_price_rules'),
        sql='sql/batch/init_price_rules.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    #init_products = UMGBigQueryOperator(
    #    task_id=config.task_name('init_products'),
    #    sql='sql/batch/init_products.sql',
     #   create_disposition='CREATE_IF_NEEDED',
      #  use_legacy_sql=False,
       # params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
       # dag=dag
    #)

    init_smart_collection = UMGBigQueryOperator(
        task_id=config.task_name('init_smart_collection'),
        sql='sql/batch/init_smart_collections.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )
    
    init_pages = UMGBigQueryOperator(
        task_id=config.task_name('init_pages'),
        sql='sql/batch/init_pages.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_themes = UMGBigQueryOperator(
        task_id=config.task_name('init_themes'),
        sql='sql/batch/init_themes.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_asset_settings_data = UMGBigQueryOperator(
        task_id=config.task_name('init_asset_settings_data'),
        sql='sql/batch/init_asset_settings_data.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_assets = UMGBigQueryOperator(
        task_id=config.task_name('init_assets'),
        sql='sql/batch/init_assets.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_customer_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_customer_metafields'),
        sql='sql/batch/init_customer_metafields.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('dataset')},
        dag=dag
    )

    init_collection_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_collection_metafields'),
        sql='sql/batch/init_collection_metafields.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('dataset')},
        dag=dag
    )

    init_settings_schema = UMGBigQueryOperator(
        task_id=config.task_name('init_settings_schema'),
        sql='sql/batch/init_settings_schema.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )



    skip_check_load_to_bq_customer_metafields = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-bq-customer-metafields'
                                 ),
        provide_context=True,
        python_callable=is_file_unavailable_notify,
        trigger_rule='all_success',
        templates_dict={
            'email_sql': email_sql,
            'exec_date': '{{ts}}',
            'task_id': '{{ task_instance.task_id }}',
            'dag_id': '{{ dag.dag_id }} ',
            'file_location': config.get('gs_raw_bucket')
                             + config.get('shopify_source_path_v2')
                             + 'report_date={report_date}/batch4/customer_metafields/*'.format(report_date='{{ds_nodash}}'
                                                                                            ),
        },
        dag=dag,
    )


    load_to_bq_customer_metafields = BashOperator(
        task_id=config.task_name('load_to_bq_customer_metafields'),
        bash_command="""
                    set -e
                    echo "load customer_metafields..."
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.customer_metafields_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch4/customer_metafields/*
                    """.format(bq_project=config.get('bq_project'),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    skip_check_load_to_bq_collection_metafields = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-bq-collection-metafields'
                                 ),
        provide_context=True,
        python_callable=is_file_unavailable_notify,
        trigger_rule='all_success',
        templates_dict={
            'email_sql': email_sql,
            'exec_date': '{{ts}}',
            'task_id': '{{ task_instance.task_id }}',
            'dag_id': '{{ dag.dag_id }} ',
            'file_location': config.get('gs_raw_bucket')
                             + config.get('shopify_source_path_v2')
                             + 'report_date={report_date}/batch5/collection_metafields/*'.format(report_date='{{ds_nodash}}'
                                                                                               ),
        },
        dag=dag,
    )


    load_to_bq_collection_metafields = BashOperator(
        task_id=config.task_name('load_to_bq_collection_metafields'),
        bash_command="""
                    set -e
                    echo "load collection_metafields..."
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.collection_metafields_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch5/collection_metafields/*
                    """.format(bq_project=config.get('bq_project'),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)


    init_error_report1 = UMGBigQueryOperator(
        task_id=config.task_name('init_error_report1'),
        sql='sql/batch/init_error_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':'_batch1'},
        dag=dag
    )

    init_error_report2 = UMGBigQueryOperator(
        task_id=config.task_name('init_error_report2'),
        sql='sql/batch/init_error_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':'_batch2'},
        dag=dag
    )

    init_error_report3 = UMGBigQueryOperator(
        task_id=config.task_name('init_error_report3'),
        sql='sql/batch/init_error_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':'_batch3'},
        dag=dag
    )

    init_error_report = UMGBigQueryOperator(
        task_id=config.task_name('init_error_report_main'),
        sql='sql/batch/init_error_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':''},
        dag=dag
    )

    load_to_bq_orders_count = BashOperator(
        task_id=config.task_name('load-to-bq-orders-count'),
        bash_command="""
                    set -e
                    echo "load orders count..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.orders_count_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/orders_count/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_collects = BashOperator(
        task_id=config.task_name('load-to-bq-collects'),
        bash_command="""
                    set -e
                    echo "load collects..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.collects_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/collects/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_customers = BashOperator(
        task_id=config.task_name('load-to-bq-customers'),
        bash_command="""
                    set -e
                    echo "load customers..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.customers_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/customers/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_disputes = BashOperator(
        task_id=config.task_name('load-to-bq-disputes'),
        bash_command="""
                    set -e
                    echo "load disputes..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.disputes_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/disputes/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_events = BashOperator(
        task_id=config.task_name('load-to-bq-events'),
        bash_command="""
                    set -e
                    echo "load events..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.events_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/events/* 
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_gift_cards = BashOperator(
        task_id=config.task_name('load-to-bq-gift-cards'),
        bash_command="""
                    set -e
                    echo "load gift cards..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.gift_cards_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/gift_cards/* 
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_inventory_items = BashOperator(
        task_id=config.task_name('load-to-bq-inventory-items'),
        bash_command="""
                    set -e
                    echo "load inventory items..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.inventory_items_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch3/inventory_items/* 
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_inventory_levels = BashOperator(
        task_id=config.task_name('load-to-bq-inventory-levels'),
        bash_command="""
                    set -e
                    echo "load inventory levels..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.inventory_levels_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch2/inventory_levels/* 
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_locations = BashOperator(
        task_id=config.task_name('load-to-bq-locations'),
        bash_command="""
                    set -e
                    echo "load locations..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.locations_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/locations/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_metafields = BashOperator(
        task_id=config.task_name('load-to-bq-metafields'),
        bash_command="""
                    set -e
                    echo "load metafields..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.metafields_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/metafields/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_payouts = BashOperator(
        task_id=config.task_name('load-to-bq-payouts'),
        bash_command="""
                    set -e
                    echo "load payouts..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.payouts_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/payouts/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_payouts_transactions = BashOperator(
        task_id=config.task_name('load-to-bq-payouts-transactions'),
        bash_command="""
                    set -e
                    echo "load payout transactions..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.payout_transactions_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch2/payout_transactions/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_price_rules = BashOperator(
        task_id=config.task_name('load-to-bq-price-rules'),
        bash_command="""
                    set -e
                    echo "load price rules..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.price_rules_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/price_rules/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    #load_to_bq_products = BashOperator(
     #   task_id=config.task_name('load-to-bq-products'),
      #  bash_command="""
       #             set -e
        #            echo "load products..."
        #            bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.products_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/products/*
        #            """.format(bq_project=config.get('bq_project'),
        #                       dag_dir=config.dag_dir(),
        #                       date='{{ds_nodash}}',
        #                       gs_raw_bucket=config.get('gs_raw_bucket'),
        #                       shopify_source_path=config.get('shopify_source_path_v2')),
        #dag=dag)

    load_to_bq_smart_collection = BashOperator(
        task_id=config.task_name('load-to-bq-smart-collection'),
        bash_command="""
                    set -e
                    echo "load smart collection..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.smart_collections_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/smart_collections/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_pages = BashOperator(
        task_id=config.task_name('load-to-bq-pages'),
        bash_command="""
                    set -e
                    echo "load pages..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.pages_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/pages/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_themes = BashOperator(
        task_id=config.task_name('load-to-bq-themes'),
        bash_command="""
                    set -e
                    echo "load themes..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.online_store_themes_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/themes/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_assets = BashOperator(
        task_id=config.task_name('load-to-bq-assets'),
        bash_command="""
                    set -e
                    echo "load assets..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.online_store_assets_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch2/assets/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_asset_settings_data = BashOperator(
        task_id=config.task_name('load-to-bq-asset-settings-data'),
        bash_command="""
                    set -e
                    echo "load asset settings data..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.online_store_asset_settings_data_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch2/asset/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_setting_schema_data = BashOperator(
        task_id=config.task_name('load-to-bq-settings-schema-data'),
        bash_command="""
                        set -e
                        echo "load settings schema data..."
                        bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.online_store_settings_schema_data_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch2/settings_schema/*
                        """.format(bq_project=config.get('bq_project'),
                                   dag_dir=config.dag_dir(),
                                   date='{{ds_nodash}}',
                                   gs_raw_bucket=config.get('gs_raw_bucket'),
                                   shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_error_report1 = BashOperator(
        task_id=config.task_name('load-to-bq-error-report1'),
        bash_command="""
                    set -e
                    echo "load error report..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.error_report_batch1\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/report/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_error_report2 = BashOperator(
        task_id=config.task_name('load-to-bq-error-report2'),
        bash_command="""
                    set -e
                    echo "load error report..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.error_report_batch2\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch2/report/*
                    """.format(bq_project=config.get('bq_project'),
                                dag_dir=config.dag_dir(),
                                date='{{ds_nodash}}',
                                gs_raw_bucket=config.get('gs_raw_bucket'),
                                shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)

    load_to_bq_error_report3 = BashOperator(
        task_id=config.task_name('load-to-bq-error-report3'),
        bash_command="""
                    set -e
                    echo "load error report..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.error_report_batch3\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch3/report/*
                    """.format(bq_project=config.get('bq_project'),
                                dag_dir=config.dag_dir(),
                                date='{{ds_nodash}}',
                                gs_raw_bucket=config.get('gs_raw_bucket'),
                                shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag)
	
	

    load_to_bq_error_report = UMGBigQueryOperator(
        task_id=config.task_name('load-to-bq-error-report'),
        sql='''
        SELECT *, "batch1" as source from `{{params.bq_project}}.shopify_source.error_report_batch1` UNION ALL
        SELECT *, "batch2" as source from `{{params.bq_project}}.shopify_source.error_report_batch2` UNION ALL
        SELECT *, "batch3" as source from `{{params.bq_project}}.shopify_source.error_report_batch3`
        ''',
        use_legacy_sql=False,
        params={
            'bq_project': config.get('bq_project')
        },
        destination_dataset_table = config.get('bq_project') + '.shopify_source.error_report',
        write_disposition='WRITE_APPEND',
        dag=dag
    )

    remove_temp_reports = BashOperator(
        task_id=config.task_name('remove-temp-reports'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:shopify_source.error_report_batch1
                    bq rm -f -t {bq_project}:shopify_source.error_report_batch2
                    bq rm -f -t {bq_project}:shopify_source.error_report_batch3
                     """.format(bq_project=config.get('bq_project'),
                                dag_dir=config.dag_dir()),
        dag=dag)

    # init_product_metafields = UMGBigQueryOperator(
    #     task_id=config.task_name('init_product_metafields'),
    #     sql='sql/batch/init_product_metafields.sql',
    #     create_disposition='CREATE_IF_NEEDED',
    #     use_legacy_sql=False,
    #     params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
    #     dag=dag
    # )

    # init_product_variant_metafields = UMGBigQueryOperator(
    #     task_id=config.task_name('init_product_variant_metafields'),
    #     sql='sql/batch/init_product_variant_metafields.sql',
    #     create_disposition='CREATE_IF_NEEDED',
    #     use_legacy_sql=False,
    #     params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
    #     dag=dag
    # )


    # init_metadata_error_report = UMGBigQueryOperator(
    #     task_id=config.task_name('init_metadata_error_report'),
    #     sql='sql/batch/init_metadata_error_report.sql',
    #     create_disposition='CREATE_IF_NEEDED',
    #     use_legacy_sql=False,
    #     params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
    #     dag=dag
    # )

    # load_to_bq_product_metafields = BashOperator(
    #     task_id=config.task_name('load-to-bq-product-metafields'),
    #     bash_command="""
    #                 set -e
    #                 echo "load product-metafields..."
    #                 bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.product_metafields_batch_v1\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/metadata/product_metadata/*
    #                 """.format(bq_project=config.get('bq_project'),
    #                            dag_dir=config.dag_dir(),
    #                            date='{{ds_nodash}}',
    #                            gs_raw_bucket=config.get('gs_raw_bucket'),
    #                            shopify_source_path=config.get('shopify_source_path_v2')),
    #     dag=dag)

    # load_to_bq_product_variant_metafields = BashOperator(
    #     task_id=config.task_name('load-to-bq-product-variant-metafields'),
    #     bash_command="""
    #                 set -e
    #                 echo "load product-metafields..."
    #                 bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.product_variant_metafields_batch_v1\${date} {gs_raw_bucket}{shopify_source_path}report_date={date}/metadata/product_variant_metadata/*
    #                 """.format(bq_project=config.get('bq_project'),
    #                            dag_dir=config.dag_dir(),
    #                            date='{{ds_nodash}}',
    #                            gs_raw_bucket=config.get('gs_raw_bucket'),
    #                            shopify_source_path=config.get('shopify_source_path_v2')),
    #     dag=dag)

    # load_metadata_temp_error_report = BashOperator(
    #     task_id=config.task_name('load-metadata-temp-error-report'),
    #     bash_command="""
    #                 set -e
    #                 echo "load error report..."
    #                 bq load --ignore_unknown_values --replace --source_format=CSV {bq_project}:shopify_source.metadata_temp_error_report {gs_raw_bucket}{shopify_source_path}report_date={date}/metadata/report/*
    #                 """.format(bq_project=config.get('bq_project'),
    #                             dag_dir=config.dag_dir(),
    #                             date='{{ds_nodash}}',
    #                             gs_raw_bucket=config.get('gs_raw_bucket'),
    #                             shopify_source_path=config.get('shopify_source_path_v2')),
    #     dag=dag)

    # load_metadata_error_report = UMGBigQueryOperator(
    #     task_id=config.task_name('load-metadata-error-report'),
    #     sql='''
    #         SELECT
    #         split(string_field_0, ' ')[OFFSET(0)] url,
    #         ltrim(split(string_field_0, '/graphql.json')[OFFSET(1)]) error_message,
    #         DATE('{{ds}}') umg_report_date
    #         FROM `{{params.bq_project}}.shopify_source.metadata_temp_error_report` where string_field_0 like '%https://%'
    #     ''',
    #     use_legacy_sql=False,
    #     params={
    #         'bq_project': config.get('bq_project')
    #     },
    #     destination_dataset_table = config.get('bq_project') + '.shopify_source.metadata_error_report',
    #     write_disposition='WRITE_APPEND',
    #     dag=dag
    # )

    # remove_temp_reports_metadata = BashOperator(
    #     task_id=config.task_name('remove-temp-reports-metadata'),
    #     bash_command="""
    #                  #!/usr/bin/env bash
    #                 bq rm -f -t {bq_project}:shopify_source.metadata_temp_error_report
    #                  """.format(bq_project=config.get('bq_project'),
    #                             dag_dir=config.dag_dir()),
    #     dag=dag)
	#
    # load_reporting_shopify_products_fields = UMGBigQueryOperator(
    #      task_id=config.task_name('load_reporting_shopify_products_metafield'),
    #      sql='sql/ecommerce-reporting/shopify_product_metafields.sql',
    #      destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
    #                                 dataset=config.get('bq_shopify_dataset'), table='shopify_product_metafields'),
    #      write_disposition='WRITE_TRUNCATE',
	# 	 params={
    #         'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),'bq_source_dataset':config.get('dataset')
    #     },
    #      allow_large_results=True,
    #    use_legacy_sql=False,
    #
    #     dag=dag
    #      )
    #
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
	
    trigger_ecommerce_reporting = BashOperator(
        task_id=config.task_name('trigger_ecommerce_reporting'),
        bash_command = "airflow trigger_dag 'datalake-shopify-ecommerce-reporting-daily-1.0{version}' -e '{date}'".format(
            version="-SNAPSHOT" if Variable.get("env") == 'dev' else "",

            date='{{ ds }}'),
        dag = dag
    )
    
    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Batch Daily"},
                           dag=dag) 
							
	

    # create tables
    download_batch_1 >> download_batch_2 >> download_batch_3
    download_batch_1 >> init_orders_count >> load_to_bq_orders_count >> finish
    download_batch_1 >> init_collects >> load_to_bq_collects  >> finish
    download_batch_1 >> init_customers >> load_to_bq_customers  >> finish
    download_batch_1 >> init_disputes >> load_to_bq_disputes  >> finish
    download_batch_1 >> init_events >> load_to_bq_events  >> finish
    download_batch_1 >> init_gift_cards >> load_to_bq_gift_cards >> finish
    download_batch_3 >> init_inventory_items >> load_to_bq_inventory_items >> [trigger_ecommerce_reporting] >> finish
    download_batch_2 >> init_inventory_levels >> load_to_bq_inventory_levels  >> finish
    download_batch_1 >> init_locations >> load_to_bq_locations  >> finish
    download_batch_1 >> init_metafields >> load_to_bq_metafields >> finish
    download_batch_1 >> skip_check >> init_payouts >> load_to_bq_payouts >> finish
    download_batch_2 >> skip_check >> init_payouts_transactions >> load_to_bq_payouts_transactions >> finish
    download_batch_1 >> init_price_rules >> load_to_bq_price_rules >> finish
    #download_batch_1 >> init_products >> load_to_bq_products >> finish
    download_batch_1 >> init_smart_collection >> load_to_bq_smart_collection >> finish
    download_batch_1 >> skip_check >> init_pages >> load_to_bq_pages >> finish
    download_batch_1 >> init_themes >> load_to_bq_themes >> finish
    download_batch_2 >> init_assets >> load_to_bq_assets >> finish
    download_batch_2 >> init_asset_settings_data >> load_to_bq_asset_settings_data
    download_batch_2 >> init_settings_schema >> load_to_bq_setting_schema_data
    download_batch_1 >> init_error_report1 >> load_to_bq_error_report1 >> finish
    download_batch_2 >> init_error_report2 >> load_to_bq_error_report2 >> finish
    download_batch_3 >> init_error_report3 >> load_to_bq_error_report3
    # download_product_metadata >> init_product_metafields >> load_to_bq_product_metafields
    # download_product_metadata >> init_product_variant_metafields >> load_to_bq_product_variant_metafields
    # [load_to_bq_product_metafields, load_to_bq_product_variant_metafields] >> load_reporting_shopify_products_fields >> finish
    # [load_to_bq_product_metafields, load_to_bq_product_variant_metafields] >> load_reporting_jp_shopify_products_fields >> finish
    # download_product_metadata >> init_metadata_error_report >> load_metadata_temp_error_report >> load_metadata_error_report >> remove_temp_reports_metadata >> finish
    load_to_bq_error_report3 >> init_error_report >> load_to_bq_error_report >> remove_temp_reports >> finish
    [download_batch_1] >> download_batch_4 >> init_customer_metafields >> skip_check_load_to_bq_customer_metafields >> load_to_bq_customer_metafields >> finish
    [download_batch_1] >> download_batch_5 >> init_collection_metafields >> skip_check_load_to_bq_collection_metafields >> load_to_bq_collection_metafields >> finish
