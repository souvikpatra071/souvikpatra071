
from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow import models
from umg.util import json_to_bq
from umg.lineage.dataset import DatasetType
from umg.util import copy_s3_to_gcs_operator
from airflow.operators.python_operator import ShortCircuitOperator
from umg.operator.sh_ops import UmgBashOperator
from airflow.operators.dummy_operator import DummyOperator
from umg.operator.gcp_df_ops import UMGDataFlowJavaOperator
from umg.operator.gcp_bq_ops import UMGBigQueryOperator
from umg.util import sqlchecker, url_generator
config = DagConfig('mdload-pipeline', '1.0-SNAPSHOT')

source_project=config.get('bq_source_project')
bq_dataset = config.get('bq_dataset')
gs_raw_bucket = config.get('gs_raw_bucket')
report_date="{{macros.ds_format(ds,'%Y-%m-%d','%d-%b-%Y')}}"
s3_source_path=config.get('s3_source_path')
staging_dataset=config.get('staging_dataset')
etl_common_version = config.get('elt_common_version_1x')
schema_path=config.get('schema_path')
schema_file_path = config.dag_dir()+ schema_path
file_path_date="{{macros.ds_add(ds, 1)}}"


default_args = {
    'owner': 'Murugan',
    'depends_on_past': True,
    'start_date': datetime(2020, 11, 16),
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 22,
    'retry_delay': timedelta(minutes=60),
    'dataflow_default_options': {
        'region': config.get('df-region'),
        'project': config.get('workflow_project'),
        'tempLocation': '{}master/dataflow/tmp/'.format(config.get('temp_bucket')),
        'gcpTempLocation': '{}master/dataflow/tmp/'.format(config.get('temp_bucket')),
        'stagingLocation': "{}master/dataflow/tmp/".format((config.get("temp_bucket"))),
        'autoscalingAlgorithm': 'THROUGHPUT_BASED',
        'serviceAccount': config.get('df-service-account')
        }
    }

dag = DAG(
    config.dag_name(data_source="load"),
    schedule_interval= None if config.get('schedule_interaval')['mdload'] == 'None' else config.get('schedule_interaval')['mdload'],
    catchup=False,
    default_args=default_args,
)
#s3_to_gcs = copy_s3_to_gcs_operator(task_id=config.task_name('s3-to-gcs'),
#                          src_path=s3_source_path+report_date+'/*.json',
#                          dest_path=gs_raw_bucket + "masterdata.music.v1_0/" +"reportdate={{ds_nodash}}/",
 #                         conn_id='master_load_s3',
 #                         dag=dag)
#var_s3_gcs= s3_to_gcs

for data_source_name in config.get('daily_partners_list'):
    data_source= config.get(data_source_name)
    table_name=data_source['bq_table']
    copy_from_activity = UmgBashOperator(
            task_id="copy_master_s3_to_gcs_"+table_name,
            bash_command='shell_script/copy_files.sh',
            params={
                'bq_table': 'artist',
                'src_path': s3_source_path,
                'dest_path': gs_raw_bucket +"source/masterdata.music.v1_0/reportdate=",
                'file_name' : data_source_name+".json",
                'bq_dataset':bq_dataset,
                'project':source_project
            },
            variable_to_env={"KMS_KEY_RING": "kms_key_ring",
                             "KMS_KEY": "kms_key"},
            s3_connection_to_env={
                "S3_SECRET": 'master_load_s3',
            },
            do_xcom_push=True,
            dag=dag
        )
    task_json_to_bq = UmgBashOperator(task_id='json-to-bq-'+table_name,
            bash_command='bq load --project_id='+source_project+' --replace --source_format=NEWLINE_DELIMITED_JSON '+staging_dataset+'.'+table_name+' '+gs_raw_bucket +"source/masterdata.music.v1_0/reportdate="+"{file_path_date}".format(file_path_date=file_path_date)+"/"+data_source_name+'.json'+' '+schema_file_path +table_name+".json",
                                 dag=dag)

    task_file_line_count = UMGDataFlowJavaOperator(task_id=config.task_name(name='file_line_count', data_source=table_name),
                            gcp_conn_id='google_cloud_default',
                            jar='gs://umg-maven-repo/release/com/umusic/data/datalake/elt/shared/elt-common/{elt_version}/elt-common-{elt_version}-dataflow.jar'.format(
                                elt_version = etl_common_version),
                            job_class='com.umusic.datalake.elt.shared.transform.profiling.FileLineCountProfiler',
                            options={
                                'labels': config.labels(),
                                'inputPath':gs_raw_bucket +"source/masterdata.music.v1_0/reportdate={file_path_date}/{file_name}".format(file_path_date=file_path_date,file_name=data_source_name)+'.json',
                                'tableRef': source_project + ':' + staging_dataset + '.data_staging_linecount_' +table_name,
                                'regex': '.*=(\d{4})-(\d{2})-(\d{2})/.*',
                                'workerMachineType': 'n1-standard-4',
                                'numWorkers': '5'
                            },
                            dag=dag)
    count_check = [{'name': table_name + '-count-check-',
                           'sql': "with a as (SELECT COUNT(1) as table_count FROM `{{ params.bq_first_table_name }}`  ), c as (select SUM(raw_line_count) line_count from `{{ params.bq_second_table_name }}` where umg_report_date = '{{params.file_path_date}}') select if(table_count = line_count, true, false) from a, c"}]
    task_bq_check = sqlchecker(count_check, "google_cloud_default", dag, {
                           'bq_first_table_name': source_project + '.' + staging_dataset + '.' + table_name,
                           'bq_second_table_name': source_project + '.' + staging_dataset + '.data_staging_linecount_' + table_name,
                           'file_path_date': file_path_date
                           },
                            dataset_name=staging_dataset)
    t_update_load_artist= UMGBigQueryOperator(
        task_id=config.task_name(name=data_source['bq_table']+'_load', data_source='bq'),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        sql="SELECT *,concat('{{params.source_uri}}','{{params.file_path_date}}','/','{{params.file_name}}') as umg_source_uri ,cast('{{params.file_path_date}}')}}' as date) as umg_report_date,current_timestamp() as umg_load_time FROM `{{ params.project_id}}.{{ params.bq_dataset }}.{{params.source_table }}`",

        params={'project_id': source_project,
            'source_table':data_source['bq_table'],'bq_dataset':staging_dataset,
            'source_uri':gs_raw_bucket +"source/masterdata.music.v1_0/reportdate=",
            'file_name':data_source_name+".json",
            'file_path_date': file_path_date},
        destination_dataset_table=source_project + '.' + bq_dataset + '.' + data_source['bq_table'],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        dag=dag)
    t_delete_staging_artist = UMGBigQueryOperator(
        task_id=config.task_name(name=data_source['bq_table']+'_del', data_source='bq'),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        sql="sql/load_delete.sql",
        params={'project_id': source_project,'bq_dataset':staging_dataset ,'bq_table': data_source['bq_table'] },
        dag=dag)


    copy_from_activity >> task_json_to_bq >> task_bq_check
    copy_from_activity >> task_file_line_count >> task_bq_check
    task_bq_check >>  t_update_load_artist >> t_delete_staging_artist
