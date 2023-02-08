#!/usr/bin/python
# -*- coding: utf-8 -*-
from airflow import DAG
from airflow import utils
from airflow import models
from datetime import datetime, timedelta
from airflow.operators.umg import UMGBigQueryOperator, UmgBashOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
from umg.airflow_dag import DagConfig
from umg.util import on_job_finish
import logging



config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Hariharan',
    'depends_on_past': False,
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 3,
    'start_date': datetime(2022, 3, 23),
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(config.dag_name(data_source='unified'),
          description='shopify unified hourly update',
          schedule_interval=None if config.get('schedule_interval')['Sale_Hourly'] == 'None' else config.get('schedule_interval')['Sale_Hourly'], 
          catchup=False,
          default_args=default_args)

for task in config.get('shopify_unified_task_list'):

    bq_task = UMGBigQueryOperator(
        task_id='shopify-unified-{specific_task}'.format(specific_task=task.split('shopify_'
                )[-1].replace('_', '-')),
        sql='sql/unified/{sql_name}.sql'.format(sql_name=task.split('shopify_'
                )[-1]),
        destination_dataset_table=(None if task == 'shopify_shops'
                                    else '{project}.{dataset}.{table}'.format(project=config.get('shopify_unified_project'
                                   ),
                                   dataset=config.get('shopify_unified_dataset'
                                   ), table=task)),
        write_disposition=(None if task == 'shopify_shops'
                            else 'WRITE_TRUNCATE'),
        allow_large_results=True,
        use_legacy_sql=False,
        dag=dag,
        )

    check_unified_load = DummyOperator(task_id='check_unified_load',
            dag=dag)

    bq_task >> check_unified_load
    
bq_project = config.get('shopify_report_project')
bq_dataset = config.get('shopify_report_dataset')
bq_table = 'chart_reporting'
bq_source = config.get('bq_source')
temp_dataset = 'data_staging'

def skip_check(**kwargs):
    schedule_time = kwargs['templates_dict']['schedule_time']
    schedule_interval = kwargs['templates_dict']['schedule_interval']
    dt = datetime.strptime(schedule_time, '%Y%m%dT%H%M%S')
    run_hour = dt.hour
    if (run_hour % schedule_interval == 0):  #There is a match.
        logging.info('schedule time: {} matches {}. Enabling {}'.format(schedule_time, run_hour, schedule_time))
        return True
    logging.info('schedule time: {} did not match any in the run list {}. Skipping for {}'.format(schedule_time, run_hour, schedule_time))
    return False

#task to check schedule days
task_skip_check = ShortCircuitOperator(
    task_id=config.task_name('task_skip_check'),
    provide_context=True,
    python_callable=skip_check,
    templates_dict={
            'schedule_interval': 4,
            'schedule_time': '{{ ts_nodash }}'
        },
    dag=dag)

insert_source_data_au = UMGBigQueryOperator(
    task_id=config.task_name('insert_source_data_au'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    sql='sql/report/australia_combine.sql',
    destination_dataset_table=bq_source + '.' + temp_dataset
        + '.shopify_chart_report_au_{{ts_nodash}}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    dag=dag,
    )

temp_to_main_au = UMGBigQueryOperator(
    task_id=config.task_name('temp_to_main_au'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    sql="""SELECT * FROM `{project}.{dataset}.{table}`""".format(project=bq_source,
            dataset=temp_dataset,
            table='shopify_chart_report_au_{{ts_nodash}}'),
    destination_dataset_table=bq_project + '.' + bq_dataset + '.'
        + bq_table,
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    time_partitioning={'type': 'DAY', 'field': 'process_date'},
    dag=dag,
    )

delete_temp_table_au = \
    UmgBashOperator(task_id=config.task_name('delete_temp_table_au'),
                    bash_command="""bq --project_id={project} rm -f -t {dataset}.{table}""".format(project=bq_source,
                    dataset=temp_dataset,
                    table='shopify_chart_report_au_{{ts_nodash}}'),
                    dag=dag)

insert_source_data_us_digital = UMGBigQueryOperator(
    task_id=config.task_name('insert_source_data_us_digital'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    sql='sql/report/us_digital.sql',
    destination_dataset_table=bq_source + '.' + temp_dataset
        + '.shopify_chart_report_us_digital_{{ts_nodash}}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    dag=dag,
    )

insert_source_data_us_physical = UMGBigQueryOperator(
    task_id=config.task_name('insert_source_data_us_physical'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    sql='sql/report/us_physical.sql',
    destination_dataset_table=bq_source + '.' + temp_dataset
        + '.shopify_chart_report_us_physical_{{ts_nodash}}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    dag=dag,
    )

insert_combine_data_us = UMGBigQueryOperator(
    task_id=config.task_name('insert_data_us'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    sql="""SELECT * FROM (
            SELECT * FROM `{project}.{dataset}.{table1}` 
            UNION ALL 
            SELECT * FROM `{project}.{dataset}.{table2}`)""".format(project=bq_source,
            dataset=temp_dataset,
            table1='shopify_chart_report_us_digital_{{ts_nodash}}',
            table2='shopify_chart_report_us_physical_{{ts_nodash}}'),
    destination_dataset_table=bq_source + '.' + temp_dataset
        + '.shopify_chart_report_us_{{ts_nodash}}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    dag=dag,
    )

temp_to_main_us = UMGBigQueryOperator(
    task_id=config.task_name('temp_to_main_us'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    sql="""SELECT * FROM `{project}.{dataset}.{table}`""".format(project=bq_source,
            dataset=temp_dataset,
            table='shopify_chart_report_us_{{ts_nodash}}'),
    destination_dataset_table=bq_project + '.' + bq_dataset + '.'
        + bq_table,
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    time_partitioning={'type': 'DAY', 'field': 'process_date'},
    dag=dag,
    )

delete_temp_tables_us = \
    UmgBashOperator(task_id=config.task_name('delete_temp_table_us'),
                    bash_command="""
            bq --project_id={project} rm -f -t {dataset}.{table1}; 
            bq --project_id={project} rm -f -t {dataset}.{table2}; 
            bq --project_id={project} rm -f -t {dataset}.{table3};""".format(project=bq_source,
                    dataset=temp_dataset,
                    table1='shopify_chart_report_us_digital_{{ts_nodash}}'
                    ,
                    table2='shopify_chart_report_us_physical_{{ts_nodash}}'
                    , table3='shopify_chart_report_us_{{ts_nodash}}'),
                    dag=dag)

finish = on_job_finish(subscribers=config.get('notification_email'),
                       variables={'service_impacted': 'Shopify Unified Hourly'
                       }, dag=dag)

check_us_digital_physical = \
    DummyOperator(task_id='check_us_digital_physical', dag=dag)

task_skip_check >> insert_source_data_au >> temp_to_main_au >> delete_temp_table_au >> finish
task_skip_check >> insert_source_data_us_digital >> check_us_digital_physical
task_skip_check >> insert_source_data_us_physical >> check_us_digital_physical
check_us_digital_physical >> insert_combine_data_us >> temp_to_main_us >> delete_temp_tables_us >> finish
