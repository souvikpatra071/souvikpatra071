from airflow import DAG
from airflow import utils
from airflow import models
from datetime import datetime, timedelta
from airflow.operators.umg import UMGBigQueryOperator, UmgBashOperator
from airflow.operators.dummy_operator import DummyOperator
from umg.airflow_dag import DagConfig
from umg.util import on_job_finish
import logging



config = DagConfig('@project.artifactId@', '@dag.version@')

docs = """
#### DAG Details

**Documentation:** [Wiki Link](https://umusic.atlassian.net/wiki/spaces/DataEngineering/pages/211125020/Shopify+Unified+Hourly+Pipeline)
"""

default_args = {
    'owner': 'Ratul',
    'depends_on_past': False,
    'email': config.get('shopify_unified_daily')['notification_email'],
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2022, 11, 21),
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(config.dag_name(data_source='unified_hourly'),
          description='shopify unified hourly update',
          schedule_interval=None if config.get('schedule_interval')['unified_hourly'] == 'None' else config.get('schedule_interval')['unified_hourly'],
          catchup=False,
          max_active_runs=1,
          default_args=default_args)

dag.doc_md = docs


start_unified_load = DummyOperator(task_id='start_unified_load', dag=dag)

end_unified_load = DummyOperator(task_id='end_unified_load',dag=dag)

task_finish = on_job_finish(
    subscribers=config.get('shopify_unified_hourly')['notification_email'],
    variables={"service_impacted": dag.description},
    dag=dag
)

sql_folder = 'unified_hourly'

load_shops_latest = UMGBigQueryOperator(
    task_id='shopify-unified-shops-latest',
    sql='sql/{sql_folder}/{sql_name}.sql'.format(sql_folder=sql_folder, sql_name='shops_latest'),
    destination_dataset_table=('{project}.{dataset}.{table}'.format(project=config.get('shopify_unified_project'),
                                                                    dataset=config.get('shopify_unified_hourly')['shopify_unified_dataset'],
                                                                    table='shopify_shops_latest')),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

load_orders_latest = UMGBigQueryOperator(
    task_id='shopify-unified-orders-latest',
    sql='sql/{sql_folder}/{sql_name}.sql'.format(sql_folder=sql_folder, sql_name='orders_latest'),
    destination_dataset_table=('{project}.{dataset}.{table}'.format(project=config.get('shopify_unified_project'),
                                                                    dataset='shopify_source',
                                                                    table='orders_combined_latest')),

    params={
        'orders_batch_table': 'umg-audience-data.shopify_source.orders_batch',
        'orders_stream_table': 'umg-audience-data.shopify_stream.orders',

    },

    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

load_orders_transaction_latest = UMGBigQueryOperator(
    task_id='shopify-unified-orders-transaction-latest',
    sql='sql/{sql_folder}/{sql_name}.sql'.format(sql_folder=sql_folder, sql_name='orders_transaction_latest'),
    destination_dataset_table=('{project}.{dataset}.{table}'.format(project=config.get('shopify_unified_project'),
                                                                    dataset='shopify_source',
                                                                    table='order_transactions_combined_latest')),

    params={
        'orders_transaction_batch_table': 'umg-audience-data.shopify_source.order_transactions_batch',
        'orders_transaction_stream_table': 'umg-audience-data.shopify_stream.order_transactions',

    },

    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)



for task in config.get('shopify_unified_hourly_task_list'):

    bq_task = UMGBigQueryOperator(
        task_id='shopify-unified-{specific_task}'.format(specific_task=task.split('shopify_'
                                                                                  )[-1].replace('_', '-')),
        sql='sql/{sql_folder}/{sql_name}.sql'.format(sql_folder=sql_folder, sql_name=task.split('shopify_'
                                                                    )[-1]),
        destination_dataset_table='{project}.{dataset}.{table}'.format(project=config.get('shopify_unified_project'),
                                                                             dataset=config.get('shopify_unified_hourly')['shopify_unified_dataset'],
                                                                             table=task),
        params={
            'orders_latest_table': config.get('shopify_unified_project') + '.shopify_source.orders_combined_latest',
            'orders_transaction_latest_table': config.get('shopify_unified_project') + '.shopify_source.order_transactions_combined_latest',

        },

        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        dag=dag,
    )


    start_unified_load >> bq_task >> end_unified_load

load_shops_latest >> load_orders_latest >> start_unified_load
load_shops_latest >> load_orders_transaction_latest >> start_unified_load
end_unified_load >> task_finish