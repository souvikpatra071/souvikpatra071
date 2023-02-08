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

**Documentation:** [Wiki Link](https://umusic.atlassian.net/wiki/spaces/DataEngineering/pages/211125020/Shopify+Unified+Daily+Pipeline)
"""

default_args = {
    'owner': 'Gautham Nair',
    'depends_on_past': False,
    'email': config.get('shopify_unified_daily')['notification_email'],
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2022, 11, 16),
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(config.dag_name(data_source='unified_daily'),
          description='shopify unified daily update',
          schedule_interval=None if config.get('schedule_interval')['unified_daily'] == 'None' else config.get('schedule_interval')['unified_daily'],
          catchup=False,
          max_active_runs=1,
          default_args=default_args)

dag.doc_md = docs

start_unified_load = DummyOperator(task_id='start_unified_load',
                                   dag=dag)

end_unified_load = DummyOperator(task_id='end_unified_load',
                                 dag=dag)

task_finish = on_job_finish(
    subscribers=config.get('shopify_unified_daily')['notification_email'],
    variables={"service_impacted": dag.description},
    dag=dag
)

if config.get('shopify_unified_daily_full_load') == True:
    sql_folder = 'unified_daily_full_load'
else:
    sql_folder = 'unified_daily'

load_shops_latest = UMGBigQueryOperator(
    task_id='shopify-unified-shops-latest',
    sql='sql/{sql_folder}/{sql_name}.sql'.format(sql_folder=sql_folder, sql_name='shops_latest'),
    destination_dataset_table=('{project}.{dataset}.{table}'.format(project=config.get('shopify_unified_project'),
                                                                    dataset=config.get('shopify_unified_daily')['shopify_unified_dataset'],
                                                                    table='shopify_shops_latest')),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

for task in config.get('shopify_unified_daily_task_list'):

    bq_task = UMGBigQueryOperator(
        task_id='shopify-unified-{specific_task}'.format(specific_task=task.split('shopify_'
                                                                                  )[-1].replace('_', '-')),
        sql='sql/{sql_folder}/{sql_name}.sql'.format(sql_folder=sql_folder, sql_name=task.split('shopify_'
                                                                    )[-1]),
        destination_dataset_table=(None if task == 'shopify_shops'
                                   else '{project}.{dataset}.{table}'.format(project=config.get('shopify_unified_project'
                                                                                                ),
                                                                             dataset=config.get('shopify_unified_daily')['shopify_unified_dataset'],
                                                                             table=task)),
        write_disposition=(None if task == 'shopify_shops'
                           else 'WRITE_TRUNCATE'),
        allow_large_results=True,
        use_legacy_sql=False,
        dag=dag,
    )


    start_unified_load>> bq_task >> end_unified_load

load_shops_latest >> start_unified_load
end_unified_load >> task_finish