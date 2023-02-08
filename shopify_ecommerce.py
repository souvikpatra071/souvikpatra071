from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.umg import UMGBigQueryOperator, UmgBashOperator
from umg.lineage.dataset import *
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from umg.util import on_job_finish

config = DagConfig('@project.artifactId@', '@dag.version@')


default_args = {
    'owner': 'Murugan',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
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
        config.dag_name('sharepoint-ecommerce'),
        catchup=False,
        schedule_interval= None if config.get('schedule')['sharepoint_ecommerce'] == 'None' else config.get('schedule')['sharepoint_ecommerce'] ,
        default_args=default_args) as dag:

    for source in config.get('table_name'):
        
        EXEC = '{{ execution_date.strftime("%m%d%Y") }}'
        task_source = config.get(source)
        
        insert_source_data = UMGBigQueryOperator(
            task_id=config.task_name('insert', source),
            bigquery_conn_id='google_cloud_default',
            use_legacy_sql=False,
            sql='sql/shopify_ecomm.sql',
            write_disposition='WRITE_TRUNCATE',
            destination_dataset_table=config.get('bq_source') + '.' + config.get('staging_dataset')+'.shopify_'+source+'_{{ds_nodash}}',
            allow_large_results=True,
            params={'bq_project': config.get('audience_project'), 'bq_dataset': config.get('audience_dataset'), 'bq_table': source},
            dataset_type=DatasetType.fact,
            dag=dag)

        export_to_gcs = BashOperator(
            task_id=config.task_name('bq_to_gcs', source),
            bash_command='bq extract --destination_format CSV {bq_source_project}:{data_provider}.{table} {input_path}'.format(
                bq_source_project=config.get('bq_source'),
                data_provider=config.get('staging_dataset'),
                table='shopify_'+source+'_{{ds_nodash}}',
                input_path=config.get('input_path')+'report_date={{ds_nodash}}/' + task_source['file_name']+EXEC+'.csv'
            ),
            dag=dag)

        k8_dev = KubernetesPodOperator(
            image='gcr.io/'+config.get('workflow_project')+'/sharepoint:'+config.get('snap_version'),
            arguments=['python3', 'gcs_to_sharepoint.py',
                       config.get('bucket_name'), 
                       config.get('shopify_ecomm_input_path')+'report_date={{ds_nodash}}/'+task_source['file_name']+EXEC+'.csv', 
                       task_source['file_name']+EXEC+'.csv', 
                       config.get('workflow_project'),
                       'global',
                       Variable.get("kms_key_ring"),
                       Variable.get("kms_key"),
                       Variable.get("shopify_ecom"),
                       config.get('application_id'),
                       config.get('sharepoint_url'),
                       config.get('sharepoint_access_path')
                       ],
            name='gcs-to-sharepoint',
            task_id=config.task_name('gcs-to-sharepoint', source),
            get_logs=True,
            image_pull_policy='Always',
            do_xcom_push=False,
            startup_timeout_seconds=1200,
            
            namespace='default',
            affinity={
                'nodeAffinity': {
                  'requiredDuringSchedulingIgnoredDuringExecution': {
                      'nodeSelectorTerms': [{
                          'matchExpressions': [{
                              'key': 'cloud.google.com/gke-nodepool',
                              'operator': 'In',
                              'values': [
                                  'airflow-app-pots'
                              ]
                          }]
                      }]
                  }
                }
            },
            dag=dag)

        task_delete_temp_table = UmgBashOperator(
            task_id=config.task_name('delete_temp_table', source),
            bash_command="""bq --project_id={project} rm -f -t {dataset}.{date}""".format(
                project=config.get('bq_source'),
                dataset=config.get('staging_dataset'),
                date='shopify_'+source+'_{{ds_nodash}}'
            ),
            dag=dag)
        finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify Ecommerce"},
                           dag=dag)            

        insert_source_data >> export_to_gcs >> k8_dev >> task_delete_temp_table >> finish
