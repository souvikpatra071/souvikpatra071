from datetime import datetime, timedelta
from airflow import models
from umg.airflow_dag import DagConfig
from airflow.operators.umg import UMGBigQueryOperator,UmgBashOperator
from airflow.operators.umg import UMGDataFlowJavaOperator
from umg.util import json_to_bq, on_job_finish
from umg.lineage.dataset import DatasetType
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Naveen',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 23),
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

shopify_sale_input_path=config.get('shopify_sale_input_path_v2')
shopify_sale_output_path=config.get('shopify_sale_output_path_v2')
bq_source_project=config.get('bq_project')
bq_source=config.get('bq_source')
data_provider=config.get('dataset')
temp_dataset='data_staging'
report_date_no_dash = '{{ ds_nodash }}'


with models.DAG(
        config.dag_name('shopify_sale_hourly_all'),
        catchup=False,
        schedule_interval= None if config.get('schedule_interval')['Sale_Hourly'] == 'None' else config.get('schedule_interval')['Sale_Hourly'],
        default_args=default_args) as dag:

        insert_source_data_temp = UMGBigQueryOperator(
            task_id=config.task_name('insert_source_data_temp'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/input_shopify_hourly.sql',
            destination_dataset_table= config.get('bq_source')+ '.' +temp_dataset+'.shopify_input_temp_{{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_report_date'
            },
            params={'bq_project':config.get('bq_source'),'data_provider':config.get('data_provider')},
            dataset_type=DatasetType.fact,
            dag=dag)

        insert_source_data_main = UMGBigQueryOperator(
            task_id=config.task_name('insert_source_data_main'),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='''
                SELECT s.*, 1/ce.rate_to_usd usd_exchange_rate 
                FROM `{{params.bq_source}}.{{params.temp_dataset}}.shopify_input_temp_{{ds_nodash}}` s
                LEFT JOIN `{{params.bq_source}}.master_tables.currency_exchange_rate` ce
                ON ce.gt_currency_code = s.currency1 
                AND TIMESTAMP(DATE(s.created_at)) BETWEEN ce.conversion_start_date AND ce.conversion_end_date
            ''',
            params={
                'bq_source': config.get('bq_source'),
                'temp_dataset': temp_dataset
            },
            destination_dataset_table = config.get('bq_source')+ '.' +temp_dataset+'.shopify_input_{{ds_nodash}}',
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            time_partitioning={
                'type':'DAY',
                'field':'umg_report_date'
            },
            dataset_type=DatasetType.fact,
            dag=dag)

        export_to_gcs = BashOperator(
            task_id = config.task_name('bq_to_gcs'),
            bash_command = 'bq extract --destination_format NEWLINE_DELIMITED_JSON {bq_source_project}:{data_provider}.{table} {input_path}'.format(
                bq_source_project=bq_source,
                data_provider=temp_dataset,
                table='shopify_input_{{ds_nodash}}',
                input_path=shopify_sale_input_path+'report_date={{ts_nodash}}/input*.json'
            ),
            dag = dag
        )
            
        shopify_sale_transform = UMGDataFlowJavaOperator(
        task_id=config.task_name('shopify_sale_transform'),
        gcp_conn_id='google_cloud_default',
        jar=config.remote_repo() + 'com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar'.format(
            version=config.get('version')),
        job_class='com.umusic.datalake.elt.shopify.ingestion.dataflow.ShopifySalesTransform',
        options={
            'labels': config.labels(),
            'inputPath': shopify_sale_input_path+'report_date={{ts_nodash}}/input*.json',
            'outputPath': shopify_sale_output_path+'report_date={{ts_nodash}}/',
            'workerMachineType': 'n1-standard-2',
            'numWorkers': '5',
            'maxNumWorkers': '100'
        },
        dag=dag)

        load_to_bq = BashOperator(
            task_id=config.task_name('load-to-bq'),
            bash_command="""
                    bq load --replace --max_bad_records=25 --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table} {shopify_sale_output_path}report_date={datetime}/* {base_folder}/json/schema.json
                    """.format(bq_project=bq_source_project,
                               bq_dataset=data_provider,
                               bq_table='sales',
                               shopify_sale_output_path=shopify_sale_output_path,
							   base_folder=config.dag_dir(),
                               datetime='{{ts_nodash}}'),
            dag=dag)
        task_delete_temp_table = UmgBashOperator(
            task_id=config.task_name('delete_temp_table'),
            bash_command="""bq --project_id={project} rm -f -t {dataset}.shopify_input_{date}""".format(
                project=bq_source,
                dataset=temp_dataset,
                date=report_date_no_dash
            ),
            dag=dag)

        trigger_shopify_unfied = BashOperator(
            task_id=config.task_name('trigger_shopify_unified'),
            bash_command = "airflow trigger_dag 'datalake-shopify-unified-1.0{version}' -e '{date}'".format(
            version="-SNAPSHOT" if Variable.get("env") == 'dev' else "",
            date='{{ ts }}'),
            dag = dag)

        finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Sale hourly"},
                           dag=dag)            

        insert_source_data_temp >> insert_source_data_main >> export_to_gcs >> shopify_sale_transform >> load_to_bq >> task_delete_temp_table >> [trigger_shopify_unfied] >> finish
        task_delete_temp_table >> finish
        