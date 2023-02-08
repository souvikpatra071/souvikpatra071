from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from umg.util import rest_api_client, on_job_finish, \
    is_file_unavailable_notify
from airflow.operators.umg import UMGBigQueryOperator
from umg.lineage.dataset import *
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.umg import UMGDataFlowJavaOperator
from airflow.models import Variable
from airflow.operators.python_operator import ShortCircuitOperator

config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Pei Chen',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 24, 10, 0),
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
        config.dag_name('batch_hourly'),
        catchup=True,
        max_active_runs=3,
        schedule_interval= None if config.get('schedule_interval')['hourly'] == 'None' else config.get('schedule_interval')['hourly'],
        user_defined_macros={
            'cur_ts': lambda exe_date: (exe_date + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        },
        default_args=default_args) as dag:
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
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/batch1/',
            'startDate': '{{ts}}',
            'endDate': '{{cur_ts(execution_date)}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '100',
            'maxNumWorkers': '200',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'orderMetafieldsApisToFetch': config.get('order_metafields_apis_to_fetch'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/report/batch1/orders/',
            'frequency': 'hourly',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_hourly_BQSyncFlag')
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
            'inputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/batch1/next_batch/*.json',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/batch2/',
            'startDate': '{{ts}}',
            'endDate': '{{cur_ts(execution_date)}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '100',
            'maxNumWorkers': '200',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'orderMetafieldsApisToFetch': config.get('order_metafields_apis_to_fetch'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/report/batch2/order_transactions/',
            'frequency': 'hourly',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_hourly_BQSyncFlag')
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
            'inputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/batch1/next_batch_1/*.json',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/batch3/',
            'startDate': '{{ts}}',
            'endDate': '{{cur_ts(execution_date)}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '10',
            'maxNumWorkers': '10',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'orderMetafieldsApisToFetch': config.get('order_metafields_apis_to_fetch'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/report/batch3/risks/',
            'frequency': 'hourly',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_hourly_BQSyncFlag')
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
            'inputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/batch1/next_batch_2/*.json',
            'outputPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/batch4/',
            'startDate': '{{ts}}',
            'endDate': '{{cur_ts(execution_date)}}',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '10',
            'maxNumWorkers': '10',
            'vaultUrl': config.get("central_vault_host"),
            'vaultRole': config.get("shopify_vault_role"),
            'vaultShopifyPath': config.get('shopify_vault_path'),
            'apisToSkip': config.get('shopify_apis_to_skip'),
            'orderMetafieldsApisToFetch': config.get('order_metafields_apis_to_fetch'),
            'reportPath': config.get('gs_raw_bucket') + 'source/shopify.batch.v2_0_hourly/report_date={{ts_nodash}}/report/batch4/order_metafields/',
            'frequency': 'hourly',
            'apiVersion': "{{var.value.shopify_api_version}}",
            'vaultServiceAccount': config.get('shopify_vault_service_account'),
            'BQSyncFlag': config.get('batch_hourly_BQSyncFlag')
        },
        retries=5,
        dag=dag)


    # init table tasks
    init_orders = UMGBigQueryOperator(
        task_id=config.task_name('init_orders'),
        sql='sql/batch/init_orders.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )
    
    init_orders_staging = UMGBigQueryOperator(
        task_id=config.task_name('init_orders_staging'),
        sql='sql/batch/init_orders_staging.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    order_transform = UMGDataFlowJavaOperator(
        task_id=config.task_name('order_transform'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ShopifyOrdersTransform',
        options={
            'labels': config.labels(),
            'inputPath': config.get('gs_raw_bucket')+config.get('shopify_source_path_v2_hourly')+'report_date={{ts_nodash}}/batch1/orders/*.json',
            'outputPath': config.get('gs_raw_bucket')+config.get('shopify_source_path_v2_hourly')+'report_date={{ts_nodash}}/batch1/orders/transformed/',
            'workerMachineType': 'n1-standard-4',
            'numWorkers': '5'
        },

        dag=dag)

    init_order_transactions = UMGBigQueryOperator(
        task_id=config.task_name('init_order_transactions'),
        sql='sql/batch/init_order_transactions.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_risks = UMGBigQueryOperator(
        task_id=config.task_name('init_risks'),
        sql='sql/batch/init_risks.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_shops = UMGBigQueryOperator(
        task_id=config.task_name('init_shops'),
        sql='sql/batch/init_shops.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source'},
        dag=dag
    )

    init_order_metafields = UMGBigQueryOperator(
        task_id=config.task_name('init_order_metafields'),
        sql='sql/batch/init_order_metafields.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('dataset')},
        dag=dag
    )

    skip_check_load_to_bq_order_metafields = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-bq-order-metafields'
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
                             + config.get('shopify_source_path_v2_hourly')
                             + 'report_date={report_date}/batch4/order_metafields/*'.format(report_date='{{ts_nodash}}'
                                                                                              ),
        },
        dag=dag,
    )

    skip_check_load_to_gcs_order_metafields = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-gcs-order-metafields'
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
                             + config.get('shopify_source_path_v2_hourly')
                             + 'report_date={report_date}/batch1/next_batch_2/*.json'.format(report_date='{{ts_nodash}}'
                                                                                             ),
        },
        dag=dag,
    )

    load_to_bq_order_metafields = BashOperator(
        task_id=config.task_name('load_to_bq_order_metafields'),
        bash_command="""
                    set -e
                    echo "load order_metafields..."
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.order_metafields_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/batch4/order_metafields/*
                    """.format(bq_project=config.get('bq_project'),
                               date='{{ds_nodash}}',
                               datetime='{{ts_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)


    init_error_report1 = UMGBigQueryOperator(
        task_id=config.task_name('init_error_report1'),
        sql='sql/batch/init_error_hourly_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':'_batch1_hourly'},
        dag=dag
    )

    init_error_report2 = UMGBigQueryOperator(
        task_id=config.task_name('init_error_report2'),
        sql='sql/batch/init_error_hourly_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':'_batch2_hourly'},
        dag=dag
    )
    
    init_error_report3 = UMGBigQueryOperator(
        task_id=config.task_name('init_error_repor3'),
        sql='sql/batch/init_error_hourly_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':'_batch3_hourly'},
        dag=dag
    )

    init_error_report = UMGBigQueryOperator(
        task_id=config.task_name('init_error_report_main'),
        sql='sql/batch/init_error_report.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': 'shopify_source', 'bq_table':'_hourly'},
        dag=dag
    )
    

    # Ecommerce reporting - order - load
    skip_check_bq_orders = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-bq-orders'),
        provide_context=True,
        python_callable=is_file_unavailable_notify,
        trigger_rule='all_success',
        templates_dict={
            'email_sql': email_sql,
            'exec_date': '{{ts}}',
            'task_id': '{{ task_instance.task_id }}',
            'dag_id': '{{ dag.dag_id }} ',
            'file_location': config.get('gs_raw_bucket')
                + config.get('shopify_source_path_v2_hourly')
                + 'report_date={report_date}/batch1/orders/transformed/*'.format(report_date='{{ts_nodash}}'
                    ),
            },
        dag=dag,
        )

    load_reporting_shopify_shops_temp = UMGBigQueryOperator(
        task_id=config.task_name('load_reporting_shopify_shops_temp'),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        sql='sql/report/shops_unified.sql',
        allow_large_results=True,
        dag=dag
    )
    
    load_reporting_shopify_shops = UMGBigQueryOperator(
        task_id=config.task_name('load_reporting_shopify_shops'),
        sql='sql/report/shops_unified_final.sql',
        destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
                                dataset=config.get('shopify_unified_dataset'), table=config.get('bq_shopify_shops')),
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        dag=dag
    )

    init_ecomm_reporting_shops = UMGBigQueryOperator(
        task_id=config.task_name('init_ecomm_reporting_shops'),
        sql='sql/ecommerce-reporting/init_ecomm_reporting_shopify_shops.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('shopify_ecomm_reporting_dataset'), 'bq_table':config.get('bq_shopify_shops')},
        dag=dag
    )

    load_ecomm_reporting_shopify_shops = UMGBigQueryOperator(
        task_id=config.task_name('load_ecomm_reporting_shopify_shops'),
        sql='sql/report/load_ecomm_reporting_shopify_shops.sql',
        destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
                                                                       dataset=config.get('shopify_ecomm_reporting_dataset'), table=config.get('bq_shopify_shops')),
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        dag=dag
    )

    # load data tasks
    load_to_bq_orders_staging = BashOperator(
        task_id=config.task_name('load-to-bq-orders_staging'),
        bash_command="""
                    set -e
                    echo "load orders staging..."
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.orders_batch_staging_{datetime} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/batch1/orders/transformed/*
                    bq query --replace --use_legacy_sql=false --allow_large_results --destination_table={bq_project}:shopify_source.orders_batch_staging_{datetime} "Select * except(x_email),cast(FARM_FINGERPRINT(email) as string) as x_email from {bq_project}.shopify_source.orders_batch_staging_{datetime} where umg_report_date = '{report_date}' and umg_load_time = '{load_time}'"
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               datetime='{{ts_nodash}}',
                               report_date = '{{ds}}',
                               load_time = '{{ts}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)
    
    load_to_bq_orders_main = UMGBigQueryOperator(
        task_id=config.task_name('load_to_bq_orders_main'),
        sql='''
        select * from `{{params.bq_project}}.{{params.bq_dataset}}.orders_batch_staging_{{ts_nodash}}`
        ''',
        use_legacy_sql=False,
        params={
            'bq_project': config.get('bq_project'), 'bq_dataset': config.get('dataset')
        },
        destination_dataset_table = config.get('bq_project') + '.'+ config.get('dataset')+'.orders_batch',
        write_disposition='WRITE_APPEND',
        dag=dag
    )
    
    load_reidentification_pii=UMGBigQueryOperator(
        task_id=config.task_name('load_reidentification_pii'),
        sql='sql/batch/reiden_pii.sql',
        destination_dataset_table='{project}.{dataset}.{table}'.
                                format(project=config.get('bq_project'),
                                dataset=config.get('shopify_reiden_pii_dataset'), 
                                table=config.get('shopify_reiden_pii_table')),
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        allow_large_results=True,
        use_legacy_sql=False,
        params={
            'bq_project':config.get('bq_project')
        },
        dag=dag
    )

    skip_check_bq_order_transactions = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-bq-orders_transactions'
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
                + config.get('shopify_source_path_v2_hourly')
                + 'report_date={report_date}/batch2/order_transactions/*'.format(report_date='{{ts_nodash}}'
                    ),
            },
        dag=dag,
        )
    load_to_bq_order_transactions = BashOperator(
        task_id=config.task_name('load-to-bq-order-transactions'),
        bash_command="""
                    set -e
                    echo "load order transactions..."
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.order_transactions_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/batch2/order_transactions/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               datetime='{{ts_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)

    skip_check_bq_risks = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-bq-risks'),
        provide_context=True,
        python_callable=is_file_unavailable_notify,
        trigger_rule='all_success',
        templates_dict={
            'email_sql': email_sql,
            'exec_date': '{{ts}}',
            'task_id': '{{ task_instance.task_id }}',
            'dag_id': '{{ dag.dag_id }} ',
            'file_location': config.get('gs_raw_bucket')
                + config.get('shopify_source_path_v2_hourly')
                + 'report_date={report_date}/batch3/risks/*'.format(report_date='{{ts_nodash}}'
                    ),
            },
        dag=dag,
        )
    load_to_bq_risks = BashOperator(
        task_id=config.task_name('load-to-bq-risks'),
        bash_command="""
                    set -e
                    echo "load risks..."
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.risks_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/batch3/risks/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               datetime='{{ts_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)

    skip_check_bq_shops = ShortCircuitOperator(
        task_id=config.task_name('precheck-load-to-bq-shops'),
        provide_context=True,
        python_callable=is_file_unavailable_notify,
        trigger_rule='all_success',
        templates_dict={
            'email_sql': email_sql,
            'exec_date': '{{ts}}',
            'task_id': '{{ task_instance.task_id }}',
            'dag_id': '{{ dag.dag_id }} ',
            'file_location': config.get('gs_raw_bucket')
                + config.get('shopify_source_path_v2_hourly')
                + 'report_date={report_date}/batch1/shop/*'.format(report_date='{{ts_nodash}}'
                    ),
            },
        dag=dag,
        )
    load_to_bq_shops = BashOperator(
        task_id=config.task_name('load-to-bq-shops'),
        bash_command="""
                    set -e
                    echo "load shops..."
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.shops_batch\${date} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/batch1/shop/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               datetime='{{ts_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)

    load_to_bq_error_report1 = BashOperator(
        task_id=config.task_name('load-to-bq-error-report1'),
        bash_command="""
                    set -e
                    echo "load error report..."
                    bq load --ignore_unknown_values --time_partitioning_field=umg_report_date --schema url:STRING,error_message:STRING,umg_report_date:DATE --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.error_report_batch1_hourly_{datetime}\${date} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/report/batch1/orders/*
                    """.format(bq_project=config.get('bq_project'),
                               dag_dir=config.dag_dir(),
                               date='{{ds_nodash}}',
                               datetime='{{ts_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)

    load_to_bq_error_report2 = BashOperator(
        task_id=config.task_name('load-to-bq-error-report2'),
        bash_command="""
                    set -e
                    echo "load error report..."
                    bq load --ignore_unknown_values --time_partitioning_field=umg_report_date --schema url:STRING,error_message:STRING,umg_report_date:DATE --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.error_report_batch2_hourly_{datetime}\${date} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/report/batch2/order_transactions/*
                    """.format(bq_project=config.get('bq_project'),
                                dag_dir=config.dag_dir(),
                                date='{{ds_nodash}}',
                                datetime='{{ts_nodash}}',
                                gs_raw_bucket=config.get('gs_raw_bucket'),
                                shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)
    
    load_to_bq_error_report3 = BashOperator(
        task_id=config.task_name('load-to-bq-error-report3'),
        bash_command="""
                    set -e
                    echo "load error report..."
                    bq load --ignore_unknown_values --time_partitioning_field=umg_report_date --schema url:STRING,error_message:STRING,umg_report_date:DATE --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:shopify_source.error_report_batch3_hourly_{datetime}\${date} {gs_raw_bucket}{shopify_source_path}report_date={datetime}/report/batch3/risks/*
                    """.format(bq_project=config.get('bq_project'),
                                dag_dir=config.dag_dir(),
                                date='{{ds_nodash}}',
                                datetime='{{ts_nodash}}',
                                gs_raw_bucket=config.get('gs_raw_bucket'),
                                shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)
    
    load_to_bq_error_report = UMGBigQueryOperator(
        task_id=config.task_name('load-to-bq-error-report'),
        sql='''
        SELECT *, timestamp('{{ts}}') as umg_load_time, "batch1" as source from `{{params.bq_project}}.shopify_source.error_report_batch1_hourly_{{ts_nodash}}` UNION ALL
        SELECT *, timestamp('{{ts}}') as umg_load_time, "batch2" as source from `{{params.bq_project}}.shopify_source.error_report_batch2_hourly_{{ts_nodash}}` UNION ALL
        SELECT *, timestamp('{{ts}}') as umg_load_time, "batch3" as source from `{{params.bq_project}}.shopify_source.error_report_batch3_hourly_{{ts_nodash}}`
        ''',
        use_legacy_sql=False,
        params={
            'bq_project': config.get('bq_project')
        },
        destination_dataset_table = config.get('bq_project') + '.shopify_source.error_report_hourly',
        write_disposition='WRITE_APPEND',
        dag=dag
    )

    remove_temp_reports = BashOperator(
        task_id=config.task_name('remove-temp-reports'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:shopify_source.error_report_batch1_hourly_{datetime}
                    bq rm -f -t {bq_project}:shopify_source.error_report_batch2_hourly_{datetime}
                    bq rm -f -t {bq_project}:shopify_source.error_report_batch3_hourly_{datetime}
                     """.format(bq_project=config.get('bq_project'), datetime='{{ts_nodash}}',
                                dag_dir=config.dag_dir()),
        trigger_rule='all_done',
        dag=dag)
    
    remove_order_staging = BashOperator(
        task_id=config.task_name('remove-order-staging'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:shopify_source.orders_batch_staging_{datetime}
                     """.format(bq_project=config.get('bq_project'),datetime='{{ts_nodash}}'),
        trigger_rule='all_done',
        dag=dag)

    trigger_shopify_sales = BashOperator(
        task_id=config.task_name('trigger_shopify_sales'),
        bash_command = "airflow trigger_dag 'datalake-shopify-shopify-sale-hourly-all-1.0{version}' -e '{date}'".format(
            version="-SNAPSHOT" if Variable.get("env") == 'dev' else "",

            date='{{ ts }}'),
        dag = dag
    )
    
    trigger_ecomm_shopify_sales = BashOperator(
        task_id=config.task_name('trigger_ecomm_shopify_sales'),
        bash_command = "airflow trigger_dag 'datalake-shopify-shopify-ecomm-sale-hourly-all-1.0{version}' -e '{date}'".format(
            version="-SNAPSHOT" if Variable.get("env") == 'dev' else "",

            date='{{ ts }}'),
        dag = dag
    )
    
    ecomm_hourly_reporting = BashOperator(
        task_id=config.task_name('ecomm_hourly_reporting'),
        bash_command = "airflow trigger_dag 'datalake-shopify-ecomm-reporting-hourly-1.0{version}' -e '{date}'".format(
        version="-SNAPSHOT" if Variable.get("env") == 'dev' else "",
        date='{{ ts }}'),
        dag = dag)
    
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
    
    load_reporting_jp_shopify_products_fields = UMGBigQueryOperator(
         task_id=config.task_name('load_reporting_jp_shopify_products_metafield'),
         sql='sql/ecommerce-reporting/shopify_product_metafields_jp.sql',
         destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('bq_project'),
                                    dataset=config.get('bq_shopify_dataset'), table='jp_shopify_metafields'),
         write_disposition='WRITE_TRUNCATE',
         params={
            'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),'bq_source_dataset':config.get('dataset')
        },
         allow_large_results=True,
       use_legacy_sql=False,
    
        dag=dag
         )

    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Batch Hourly"},
                           dag=dag)
                           
    # create tables
    download_batch_1 >> download_batch_2
    download_batch_1 >> download_batch_3
    download_batch_1 >> init_orders >> init_orders_staging \
        >> order_transform >> skip_check_bq_orders \
        >> load_to_bq_orders_staging >> load_to_bq_orders_main >> load_reidentification_pii \
        >> remove_order_staging >> finish
    remove_order_staging >> [trigger_shopify_sales] >> finish
    remove_order_staging >> [trigger_ecomm_shopify_sales] >> finish
    download_batch_2 >> init_order_transactions \
        >> skip_check_bq_order_transactions \
        >> load_to_bq_order_transactions >> finish
    load_to_bq_order_transactions >> [trigger_shopify_sales] >> finish
    load_to_bq_order_transactions >> [trigger_ecomm_shopify_sales] >> finish
    [order_transform, load_to_bq_order_transactions] >> ecomm_hourly_reporting >> finish
    download_batch_3 >> init_risks >> skip_check_bq_risks \
        >> load_to_bq_risks >> finish
    download_batch_1 >> init_shops >> skip_check_bq_shops \
        >> load_to_bq_shops >> load_reporting_shopify_shops_temp >> load_reporting_shopify_shops >> init_ecomm_reporting_shops >> load_ecomm_reporting_shopify_shops >> finish
    download_batch_1 >> init_error_report1 >> load_to_bq_error_report1 \
        >> init_error_report >> load_to_bq_error_report \
        >> remove_temp_reports >> finish
    download_batch_2 >> init_error_report2 >> load_to_bq_error_report2 \
        >> init_error_report >> load_to_bq_error_report \
        >> remove_temp_reports >> finish
    download_batch_3 >> init_error_report3 >> load_to_bq_error_report3 \
        >> init_error_report >> load_to_bq_error_report \
        >> remove_temp_reports >> finish
    [download_batch_1] >> skip_check_load_to_gcs_order_metafields >> download_batch_4 >> init_order_metafields >> skip_check_load_to_bq_order_metafields >> load_to_bq_order_metafields >> finish
    [load_to_bq_orders_main] >> load_reporting_shopify_products_fields >> finish
    [load_to_bq_orders_main] >> load_reporting_jp_shopify_products_fields >> finish
