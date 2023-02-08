from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from umg.util import on_job_finish
from airflow.operators.umg import UMGBigQueryOperator
from airflow.operators.umg import UMGDataFlowJavaOperator
from umg.lineage.dataset import *
from airflow.operators.bash_operator import BashOperator

config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Gutta Harikrishna',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 18),
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
        config.dag_name('ecomm_reporting_hourly'),
        catchup=False,
        max_active_runs=2,
        schedule_interval= None if config.get('schedule_interval')['unified'] == 'None' else config.get('schedule_interval')['unified'],
        user_defined_macros={
            'cur_ts': lambda exe_date: (exe_date + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        },
        default_args=default_args) as dag:
        
    ts_no_dash = '{{ts_nodash}}'
    
    order_reporting_transform = UMGDataFlowJavaOperator(
        task_id=config.task_name('order_reporting_transform'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ReportingOrdersTransform',
        options={
            'labels': config.labels(),
            'inputPath': config.get('gs_raw_bucket')+config.get('shopify_source_path_v2_hourly')+'report_date={{ts_nodash}}/batch1/orders/transformed/*',
            'outputPath': config.get('gs_raw_bucket')+config.get('shopify_source_path_v2_hourly')+'report_date={{ts_nodash}}/batch1/orders/reporting_transformed/',
            'workerMachineType': 'n1-standard-4',
            'numWorkers': '5'
        },

        dag=dag)
    
    init_reporting_orders_temp = UMGBigQueryOperator(
        task_id=config.task_name('init_reporting_orders_temp'),
        sql='sql/ecommerce-reporting/init_reporting_orders_temp.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('bq_shopify_dataset'),'bq_table': 'orders_batch_temp' },
        dag=dag
    )

    init_reporting_orders = UMGBigQueryOperator(
        task_id=config.task_name('init_reporting_orders'),
        sql='sql/ecommerce-reporting/init_reporting_orders.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('bq_shopify_dataset'),'bq_table':config.get('bq_shopify_orders_table')},
        dag=dag
    )
    
    load_reporting_orders_temp = BashOperator(
        task_id=config.task_name('load-to-bq-order-reporting'),
        bash_command="""
                    set -e
                    bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table}_{date_time} {gs_raw_bucket}{shopify_source_path}report_date={date_time}/batch1/orders/reporting_transformed/*
                    """.format(bq_project=config.get('bq_project'),
                               bq_dataset=config.get('bq_shopify_dataset'),
                               bq_table='orders_batch_temp',
                               date_time='{{ts_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2_hourly')),
        dag=dag)
    


    load_reporting_shopify_orders = UMGBigQueryOperator(
        task_id=config.task_name('load_reporting_orders'),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        sql='sql/report/load_reporting_shopify_orders.sql',
        params={'bq_project': config.get('bq_project'), 'bq_shopify_dataset': config.get('bq_shopify_dataset'),
                'bq_table':'orders_batch_temp','bq_source_table':config.get('bq_shopify_orders_table')},
        allow_large_results=True,
        dag=dag
    )


    remove_order_temp = BashOperator(
        task_id=config.task_name('remove_order_temp'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:{bq_dataset}.{bq_table}_{ts}
                     """.
                     format(bq_project=config.get('bq_project'),bq_dataset=config.get('bq_shopify_dataset'),bq_table='orders_batch_temp',ts=ts_no_dash),
        trigger_rule='all_done',
        dag=dag)
    # Ecommerce reporting - order transactions - load
    
    init_reporting_order_transaction = UMGBigQueryOperator(
        task_id=config.task_name('init_reporting_order_transaction'),
        sql='sql/ecommerce-reporting/init_shopify_order_transactions.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 'bq_dataset': config.get('bq_shopify_dataset'),'bq_table':config.get('bq_shopify_order_transactions_table')},
        dag=dag
    )

    load_reporting_shopify_order_transactions = UMGBigQueryOperator(
        task_id=config.task_name('load_reporting_order_transactions'),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        sql='sql/report/load_reporting_shopify_order_transactions.sql',
        params={'bq_project': config.get('bq_project'), 'bq_shopify_dataset': config.get('bq_shopify_dataset'),
                'bq_table': config.get('bq_shopify_order_transactions_table'), 'bq_temp_table': config.get('bq_shopify_order_transactions_table_temp')},
        allow_large_results=True,
        dag=dag
    )


    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Ecomm Reporting Hourly"},
                           dag=dag)


    order_reporting_transform >> init_reporting_orders_temp >>  load_reporting_orders_temp >> init_reporting_orders >> [load_reporting_shopify_orders] >> remove_order_temp >> finish
    init_reporting_order_transaction >> load_reporting_shopify_order_transactions >> finish
    
