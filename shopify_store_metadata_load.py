from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.umg import UMGBigQueryOperator,UmgBashOperator
from umg.lineage.dataset import *
from airflow.operators.bash_operator import BashOperator



config = DagConfig('@project.artifactId@', '@dag.version@')
store_metadata_config = config.get('shopify_metadata_load')

bq_shopify_metadata_table_stg= '{bq_shopify_metadata}_stg_{report_date}'.format(bq_shopify_metadata= store_metadata_config['bq_table'], report_date='{{ds_nodash}}')


default_args = {
    'owner': 'Madhurya Malladi',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 25),
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}



with models.DAG(
        config.dag_name('store_metadata_load'),
        catchup=False,
        max_active_runs=1,
        schedule_interval= None if config.get('schedule_interval')['store_metadata_load'] == 'None' else config.get('schedule_interval')['store_metadata_load'],
        default_args=default_args) as dag:


    init_stg_table = BashOperator(
        task_id=config.task_name('init_stg_table'),
        bash_command='bq mk --table {bq_project}:{bq_dataset}.{bq_table} {base_folder}/json/shopify_metadata.json'.format(bq_project= store_metadata_config['bq_project'], bq_dataset= store_metadata_config['bq_dataset'], bq_table= bq_shopify_metadata_table_stg, base_folder=config.dag_dir()),
        dag=dag
    )


    load_data_to_bq = UMGBigQueryOperator(
        task_id=config.task_name("load_data_to_bq"),
        sql = 'sql/shopify_store_metadata_load.sql',
        destination_dataset_table = '{bq_project}:{bq_dataset}.{bq_table}'.format(bq_project= store_metadata_config['bq_project'], bq_dataset= store_metadata_config['bq_dataset'], bq_table= bq_shopify_metadata_table_stg),
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        dag = dag
    )

    export_bq_table = BashOperator(
        task_id=config.task_name('export_bq_table'),
        bash_command='bq extract --destination_format CSV {bq_project}:{bq_dataset}.{bq_table} {gs_staging_bucket}{gs_path}{report_date}/{file_prefix}{report_date}.csv'.format(
            bq_project= store_metadata_config['bq_project'],
            bq_dataset= store_metadata_config['bq_dataset'],
            bq_table= bq_shopify_metadata_table_stg,
            gs_staging_bucket= store_metadata_config['gs_staging_bucket'],
            gs_path= store_metadata_config['gs_path'],
            report_date='{{ds_nodash}}',
            file_prefix= store_metadata_config['file_prefix']),
        dag=dag
    )

    delete_stg_table = UmgBashOperator(
        task_id=config.task_name('delete_stg_table'),
        bash_command='bq --project_id={bq_project} rm -f -t {bq_dataset}.{bq_table}'.format(bq_project= store_metadata_config['bq_project'], bq_dataset= store_metadata_config['bq_dataset'], bq_table= bq_shopify_metadata_table_stg),
        dag=dag
    )

    init_stg_table >> load_data_to_bq >> export_bq_table >> delete_stg_table