#!/usr/bin/python
# -*- coding: utf-8 -*-
from airflow import utils
from datetime import datetime, timedelta
from umg.airflow_dag import DagConfig
from airflow import DAG, macros, models
from airflow.operators import PythonOperator, BashOperator, DummyOperator
from airflow.operators.umg import (
    UMGDataFlowJavaOperator,
    UMGBigQueryOperator,
    UmgBashOperator,
)
from umg.vault_encryption import get_vault_secret_with_gcp_auth_enc
from airflow.models import XCom
from sqlalchemy import func
from airflow.utils.db import provide_session

config = DagConfig('@project.artifactId@', '@dag.version@')

label_name = config.labels()
domo_auth_url = config.get("domo_auth_url")
domo_grant_type = config.get("domo_grant_type")
domo_job_base_url = config.get("domo_job_base_url")
domo_job_action = config.get("domo_job_action")
ecommrpt_shopify_unloadconnector_base_job_id = config.get("ecommrpt_shopify_unloadconnector_base_job_id")
ecommrpt_shopify_ga_combined_unlc_base_job_id = config.get("ecommrpt_shopify_ga_combined_unlc_base_job_id")
domo_vault_info = '{{task_instance.xcom_pull(task_ids="vault_encryption_domo", key="encrypt_string")}}'
kms_key_ring = config.get("kms_key_ring")
kms_key_ring_id = config.get("kms_key")
dag_env = config.get("env_name")
vault_location_id = "global"
worker_machine_type = "n1-standard-2"
dataflow_gcp_conn_id = "google_cloud_default"
remote_repo_name = config.remote_repo()
version = config.get("version")

vault_gcp_conn_id = "gcp-vault-auth-ro"
vault_hostname = config.get("central_vault_host")
domo_vault_role = config.get("domo_gcp_role_name")
domo_vault_path = config.get("domo_vault_path")

default_args = {
    'owner': 'Manikanda Prabu',
    'depends_on_past': False,
    'email': config.get('notification_email'),
    'email_on_failure': config.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 3,
    'start_date': datetime(2022, 4, 4),
    'retry_delay': timedelta(minutes=10),
    'dataflow_default_options': {
        'project': config.get("workflow_project"),
        'tempLocation': '{}shopify/domo/tmp/'.format(config.get('temp_bucket')),
        'gcpTempLocation': '{}shopify/domo/dataflow/tmp/'.format(config.get('temp_bucket')),
        'autoscalingAlgorithm': 'THROUGHPUT_BASED',
        'serviceAccount': config.get('df-service-account')
    }
}

dag = DAG(config.dag_name(data_source='domo-reporting'),
          description='Shopify domo reporting daily',
          schedule_interval=None if config.get('schedule')['domo'] == 'None' else config.get('schedule')['domo'],
          catchup=False,
          default_args=default_args)


#Calling the enc vault common function to get the
def get_vault_secret_with_enc(templates_dict, **kwargs):
    gcp_conn_id = templates_dict['gcp_conn_id']
    vault_host = templates_dict['vault_host']
    vault_role = templates_dict['vault_role']
    vault_path = templates_dict['vault_path']
    project_id = templates_dict['project_id']
    location_id = templates_dict['location_id']
    key_ring = templates_dict['key_ring']
    key_ring_id = templates_dict['key_ring_id']
    encrypt_string = get_vault_secret_with_gcp_auth_enc(gcp_conn_id, vault_host, vault_role, vault_path, project_id, location_id, key_ring, key_ring_id)
    kwargs['ti'].xcom_push(key='encrypt_string', value=encrypt_string)

#To clean xcom values from airflow database
@provide_session
def cleanup_xcom(templates_dict, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session = context["session"]
    session.query(XCom).filter(XCom.dag_id == dag_id, XCom.execution_date == func.date(templates_dict['date']), XCom.key == 'encrypt_string').delete(synchronize_session=False)

vault_encryption_domo = PythonOperator(
    task_id="vault_encryption_domo",
    python_callable=get_vault_secret_with_enc,
    provide_context=True,
    templates_dict={
        "gcp_conn_id": vault_gcp_conn_id,
        "vault_host": vault_hostname,
        "vault_role": domo_vault_role,
        "vault_path": domo_vault_path,
        "project_id": dag_env,
        "location_id": vault_location_id,
        "key_ring": kms_key_ring,
        "key_ring_id": kms_key_ring_id,
    },
    dag=dag,
    )


check_bq_tasks = DummyOperator(task_id='check-previous-bq-loads', dag=dag)

for task in config.get('shopify_domo_reporting_tasks'):
    bq_task = UMGBigQueryOperator(
        task_id='shopify-domo-reporting-{task}'.format(task=task.replace('_', '-')),
        sql='sql/domo-reporting/{sql_name}.sql'.format(sql_name=task),
        # create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project_domo_reporting')[0], 'bq_dataset': config.get('bq_dataset_domo_reporting')[0]},
        dag=dag
        )

    vault_encryption_domo >> bq_task >> check_bq_tasks

bq_task_temp_ecommrpt_shopify_shipping = UMGBigQueryOperator(
    task_id='shopify-domo-reporting-ecommrpt-shopify-shipping',
    sql='sql/domo-reporting/temp_ecommrpt_shopify_shipping.sql',
    use_legacy_sql=False,
    params={'bq_project': config.get('bq_project_domo_reporting')[0], 'bq_dataset': config.get('bq_dataset_domo_reporting')[0]},
    dag=dag
    )

bq_task_shopify_detail = UMGBigQueryOperator(
    task_id='shopify-domo-reporting-shopify-detail',
    sql='sql/domo-reporting/shopify_detail.sql',
    use_legacy_sql=False,
    params={'bq_project': config.get('bq_project_domo_reporting')[0], 'bq_dataset': config.get('bq_dataset_domo_reporting')[0]},
    dag=dag
    )

bq_task_shopify_product_minorderdate = UMGBigQueryOperator(
    task_id='shopify-domo-reporting-shopify-product-minorderdate',
    sql='sql/domo-reporting/shopify_product_minorderdate.sql',
    use_legacy_sql=False,
    params={'bq_project': config.get('bq_project_domo_reporting')[0], 'bq_dataset': config.get('bq_dataset_domo_reporting')[0]},
    dag=dag
    )

bq_task_ecommrpt_shopify = UMGBigQueryOperator(
    task_id='shopify-domo-reporting-ecommrpt-shopify',
    sql='sql/domo-reporting/ecommrpt_shopify.sql',
    use_legacy_sql=False,
    params={'bq_project': config.get('bq_project_domo_reporting')[1], 'bq_dataset': config.get('bq_dataset_domo_reporting')[1]},
    dag=dag
    )

bq_task_ecommrpt_shopify_ga_combined = UMGBigQueryOperator(
    task_id='shopify-domo-reporting-ecommrpt-shopify-ga-combined',
    sql='sql/domo-reporting/ecommrpt_shopify_ga_combined.sql',
    use_legacy_sql=False,
    params={'bq_project': config.get('bq_project_domo_reporting')[1], 'bq_dataset': config.get('bq_dataset_domo_reporting')[1]},
    dag=dag
    )

domo_api_call_source_first = UMGDataFlowJavaOperator(
    task_id=config.task_name("domo_api_call_source_first"),
    gcp_conn_id=dataflow_gcp_conn_id,
    jar=remote_repo_name
        + "com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar".format(
        version=version
    ),
    job_class="com.umusic.datalake.elt.shopify.ingestion.dataflow.DomoApi",
    options={
        "labels": label_name,
        "domoAuthUrl": domo_auth_url,
        "domoGrantType": domo_grant_type,
        "domoJobBaseUrl": domo_job_base_url,
        "domoJobAction": domo_job_action,
        "domoJobId": ecommrpt_shopify_unloadconnector_base_job_id,
        "vaultInfo": domo_vault_info,
        "KMSKeyRing": kms_key_ring,
        "KMSKey": kms_key_ring_id,
        "KMSProject": dag_env,
        "KMSLocation": vault_location_id,
        "workerMachineType": worker_machine_type,
        "numWorkers": "1",
    },
    dag=dag,
)

domo_api_call_source_second = UMGDataFlowJavaOperator(
    task_id=config.task_name("domo_api_call_source_second"),
    gcp_conn_id=dataflow_gcp_conn_id,
    jar=remote_repo_name
        + "com/umusic/data/datalake/elt/shopify/shopify-ingestion/{version}/shopify-ingestion-{version}-dataflow.jar".format(
        version=version
    ),
    job_class="com.umusic.datalake.elt.shopify.ingestion.dataflow.DomoApi",
    options={
        "labels": label_name,
        "domoAuthUrl": domo_auth_url,
        "domoGrantType": domo_grant_type,
        "domoJobBaseUrl": domo_job_base_url,
        "domoJobAction": domo_job_action,
        "domoJobId": ecommrpt_shopify_ga_combined_unlc_base_job_id,
        "vaultInfo": domo_vault_info,
        "KMSKeyRing": kms_key_ring,
        "KMSKey": kms_key_ring_id,
        "KMSProject": dag_env,
        "KMSLocation": vault_location_id,
        "workerMachineType": worker_machine_type,
        "numWorkers": "1",
    },
    dag=dag,
)


check_bq_tasks >> bq_task_temp_ecommrpt_shopify_shipping >> bq_task_shopify_detail >> bq_task_shopify_product_minorderdate >> bq_task_ecommrpt_shopify >> bq_task_ecommrpt_shopify_ga_combined >> domo_api_call_source_first >> domo_api_call_source_second
