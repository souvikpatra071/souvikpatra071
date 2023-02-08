from datetime import datetime, timedelta
from airflow import models
from umg.airflow_dag import DagConfig
from airflow.operators.umg import UMGBigQueryOperator
from umg.lineage.dataset import *
from umg.util import on_job_finish


config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Pei Chen',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 23, 10, 0),
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'dataflow_default_options': {
        'region': config.get('df-region'),
        'project': config.get("workflow_project"),
        'tempLocation': '{}shopify/dataflow/tmp/'.format(config.get('temp_bucket')),
        'gcpTempLocation': '{}shopify/dataflow/tmp/'.format(config.get('temp_bucket')),
        'autoscalingAlgorithm': 'THROUGHPUT_BASED',
        'serviceAccount': config.get('df-service-account')
    }
}

with models.DAG(
        config.dag_name('streaming-transform'),
        catchup=False,
        schedule_interval= None if config.get('schedule_interval')['hourly'] == 'None' else config.get('schedule_interval')['hourly'],
        default_args=default_args) as dag:

        insert_raw_orders = UMGBigQueryOperator(
            task_id=config.task_name('insert-raw-orders'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/streaming/insert_raw_orders.sql',
            destination_dataset_table= config.get('bq_project') + '.shopify_source.orders_stream${{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_event_date'
            },
            params={'bq_project':config.get('bq_project')},

            dataset_type=DatasetType.fact,
            dag=dag)

        insert_raw_customers = UMGBigQueryOperator(
            task_id=config.task_name('insert-raw-customers'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/streaming/insert_raw_customers.sql',
            destination_dataset_table= config.get('bq_project') + '.shopify_source.customers_stream${{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_event_date'
            },
            params={'bq_project':config.get('bq_project')},
            dataset_type=DatasetType.fact,
            dag=dag)

        insert_raw_collections = UMGBigQueryOperator(
            task_id=config.task_name('insert-raw-collections'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/streaming/insert_raw_collections.sql',
            destination_dataset_table= config.get('bq_project') + '.shopify_source.collections_stream${{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_event_date'
            },
            params={'bq_project':config.get('bq_project')},
            dataset_type=DatasetType.fact,
            dag=dag)

        insert_raw_inventory_items = UMGBigQueryOperator(
            task_id=config.task_name('insert-raw-inventory-items'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/streaming/insert_raw_inventory_items.sql',
            destination_dataset_table= config.get('bq_project') + '.shopify_source.inventory_items_stream${{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_event_date'
            },
            params={'bq_project':config.get('bq_project')},
            dataset_type=DatasetType.fact,
            dag=dag)

        insert_raw_inventory_levels = UMGBigQueryOperator(
            task_id=config.task_name('insert-raw-inventory-levels'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/streaming/insert_raw_inventory_levels.sql',
            destination_dataset_table= config.get('bq_project') + '.shopify_source.inventory_levels_stream${{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_event_date'
            },
            params={'bq_project':config.get('bq_project')},
            dataset_type=DatasetType.fact,
            dag=dag)

        insert_raw_order_transactions = UMGBigQueryOperator(
            task_id=config.task_name('insert-raw-order-transactions'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/streaming/insert_raw_order_transactions.sql',
            destination_dataset_table= config.get('bq_project') + '.shopify_source.order_transactions_stream${{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_event_date'
            },
            params={'bq_project':config.get('bq_project')},
            dataset_type=DatasetType.fact,
            dag=dag)

        insert_raw_products = UMGBigQueryOperator(
            task_id=config.task_name('insert-raw-products'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/streaming/insert_raw_products.sql',
            destination_dataset_table= config.get('bq_project') + '.shopify_source.products_stream${{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_event_date'
            },
            params={'bq_project':config.get('bq_project')},
            dataset_type=DatasetType.fact,
            dag=dag)
            
        finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Stream"},
                           dag=dag) 
                           
        insert_raw_orders >> finish
        insert_raw_customers >> finish
        insert_raw_collections >> finish
        insert_raw_inventory_items >> finish
        insert_raw_inventory_levels >> finish
        insert_raw_order_transactions >> finish
        insert_raw_products >> finish
