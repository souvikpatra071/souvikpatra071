from umg.airflow_dag import DagConfig
from datetime import datetime, timedelta
from airflow import models
from umg.lineage.dataset import *
from umg.mail.mailjet import send_alert
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.umg import UMGBigQueryOperator
from umg.util import on_job_finish

config = DagConfig('@project.artifactId@', '@dag.version@')

default_args = {
    'owner': 'Murugan',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 12),
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
        config.dag_name('ecommerce_reporting_daily'),
        catchup=False,
        max_active_runs=1,
        schedule_interval= None if config.get('schedule_interval')['ecommerce_reporting'] == 'None' else config.get('schedule_interval')['ecommerce_reporting'],
        default_args=default_args) as dag:

    finish = on_job_finish(subscribers=config.get('notification_email'),
                           variables={"service_impacted": "Shopify ecommerce reporting daily"},
                           dag=dag)    
        
    for data_source_name in config.get('shopify_ecommerce_reporting'):
        init_metafields = UMGBigQueryOperator(
        task_id='init_'+data_source_name,
        sql='sql/ecommerce-reporting/init_'+data_source_name+'.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset')},
        dag=dag
		)
        load_metafields = UMGBigQueryOperator(
        task_id='load_'+data_source_name,
        sql='sql/ecommerce-reporting/'+data_source_name+'.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),'bq_source_dataset':config.get('dataset')},
        dag=dag
    )
        init_metafields >> load_metafields >> finish
        
    # nested fields are type cast here:
    init_shopify_payouts_temp = UMGBigQueryOperator(
            task_id='init_payouts_temp',
            sql='sql/ecommerce-reporting/init_shopify_payouts_temp.sql',
            create_disposition='CREATE_IF_NEEDED',
            use_legacy_sql=False,
            params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),
                    'bq_table':'shopify_payouts_temp'},
            dag=dag
            )
        
    load_shopify_payouts_temp = BashOperator(
        task_id=config.task_name('load_shopify_payouts_temp'),
        bash_command="""
                    set -e
                    echo "load products..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/payouts/*
                    """.format(bq_project=config.get('bq_project'),bq_dataset=config.get('bq_dataset'),
                               bq_table='shopify_payouts_temp',
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag
    )
    
    init_shopify_payouts = UMGBigQueryOperator(
            task_id='init_shopify_payouts',
            sql='sql/ecommerce-reporting/init_shopify_payouts.sql',
            create_disposition='CREATE_IF_NEEDED',
            use_legacy_sql=False,
            params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),
                    'bq_table':'shopify_payouts'},
            dag=dag
            )

    merge_shopify_payouts = UMGBigQueryOperator(
        task_id=config.task_name('merge_shopify_payouts_temp'),
        sql='sql/ecommerce-reporting/shopify_payouts.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 
                'bq_dataset': config.get('bq_dataset'),
                'bq_source_table': 'shopify_payouts_temp',
                'bq_table':'shopify_payouts'
                
                },
        dag=dag
    )
    
    delete_payouts_temp = BashOperator(
        task_id=config.task_name('remove-temp-payouts'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:{bq_dataset}.shopify_payouts_temp
                     """.format(bq_project=config.get('bq_project'),
                                bq_dataset=config.get('bq_dataset'),),
        dag=dag)
    
    # init_shopify_products_temp = UMGBigQueryOperator(
    #         task_id='init_products_temp',
    #         sql='sql/ecommerce-reporting/init_shopify_products.sql',
    #         create_disposition='CREATE_IF_NEEDED',
    #         use_legacy_sql=False,
    #         params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),
    #                 'bq_table':'shopify_products_temp'},
    #         dag=dag
    #         )
    #
    # load_shopify_products_temp = BashOperator(
    #     task_id=config.task_name('load_shopify_products_temp'),
    #     bash_command="""
    #                 set -e
    #                 echo "load products..."
    #                 bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/products/*
    #                 """.format(bq_project=config.get('bq_project'),bq_dataset=config.get('bq_dataset'),
    #                            bq_table='shopify_products_temp',
    #                            date='{{ds_nodash}}',
    #                            gs_raw_bucket=config.get('gs_raw_bucket'),
    #                            shopify_source_path=config.get('shopify_source_path_v2')),
    #     dag=dag
    # )
    #
    # init_shopify_products = UMGBigQueryOperator(
    #         task_id='init_shopify_products',
    #         sql='sql/ecommerce-reporting/init_shopify_products.sql',
    #         create_disposition='CREATE_IF_NEEDED',
    #         use_legacy_sql=False,
    #         params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),
    #                 'bq_table':'shopify_products'},
    #         dag=dag
    #         )
    #
    # merge_shopify_products = UMGBigQueryOperator(
    #     task_id=config.task_name('merge_shopify_products_temp'),
    #     sql='sql/ecommerce-reporting/shopify_products.sql',
    #     create_disposition='CREATE_IF_NEEDED',
    #     use_legacy_sql=False,
    #     params={'bq_project': config.get('bq_project'),
    #             'bq_dataset': config.get('bq_dataset'),
    #             'bq_source_table': 'shopify_products_temp',
    #             'bq_table':'shopify_products'
    #
    #             },
    #     dag=dag
    # )
    #
    # delete_products_temp = BashOperator(
    #     task_id=config.task_name('remove-temp-products'),
    #     bash_command="""
    #                  #!/usr/bin/env bash
    #                 bq rm -f -t {bq_project}:{bq_dataset}.shopify_products_temp
    #                  """.format(bq_project=config.get('bq_project'),
    #                             bq_dataset=config.get('bq_dataset'),),
    #     dag=dag)
    #

    init_shopify_customers = UMGBigQueryOperator(
            task_id='init_shopify_customers',
            sql='sql/ecommerce-reporting/init_shopify_customers.sql',
            create_disposition='CREATE_IF_NEEDED',
            use_legacy_sql=False,
            params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),
                    'bq_table':'shopify_customers'},
            dag=dag
            )

    merge_shopify_customers = UMGBigQueryOperator(
        task_id=config.task_name('merge_shopify_customers_temp'),
        sql='sql/ecommerce-reporting/shopify_customers.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 
                'bq_dataset': config.get('bq_dataset'),
                'bq_source_table': 'shopify_customers_temp',
                'bq_table':'shopify_customers'
                
                },
        dag=dag
    )
    
    
    init_shopify_collections_temp = UMGBigQueryOperator(
            task_id='init_collections_temp',
            sql='sql/ecommerce-reporting/init_shopify_collections.sql',
            create_disposition='CREATE_IF_NEEDED',
            use_legacy_sql=False,
            params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),
                    'bq_table':'shopify_collections_temp'},
            dag=dag
            )
        
    load_shopify_collections_temp = BashOperator(
        task_id=config.task_name('load_shopify_collections_temp'),
        bash_command="""
                    set -e
                    echo "load products..."
                    bq load --ignore_unknown_values --replace --source_format=NEWLINE_DELIMITED_JSON {bq_project}:{bq_dataset}.{bq_table} {gs_raw_bucket}{shopify_source_path}report_date={date}/batch1/smart_collections/*
                    """.format(bq_project=config.get('bq_project'),bq_dataset=config.get('bq_dataset'),
                               bq_table='shopify_collections_temp',
                               date='{{ds_nodash}}',
                               gs_raw_bucket=config.get('gs_raw_bucket'),
                               shopify_source_path=config.get('shopify_source_path_v2')),
        dag=dag
    )
    
    init_shopify_collections = UMGBigQueryOperator(
            task_id='init_shopify_collections',
            sql='sql/ecommerce-reporting/init_shopify_collections.sql',
            create_disposition='CREATE_IF_NEEDED',
            use_legacy_sql=False,
            params={'bq_project': config.get('bq_project'),'bq_dataset': config.get('bq_shopify_dataset'),
                    'bq_table':'shopify_collections'},
            dag=dag
            )

    merge_shopify_collections = UMGBigQueryOperator(
        task_id=config.task_name('merge_shopify_collections_temp'),
        sql='sql/ecommerce-reporting/shopify_collections.sql',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        params={'bq_project': config.get('bq_project'), 
                'bq_dataset': config.get('bq_dataset'),
                'bq_source_table': 'shopify_collections_temp',
                'bq_table':'shopify_collections'
                
                },
        dag=dag
    )
    
    delete_collections_temp = BashOperator(
        task_id=config.task_name('remove-temp-collections'),
        bash_command="""
                     #!/usr/bin/env bash
                    bq rm -f -t {bq_project}:{bq_dataset}.shopify_collections_temp
                     """.format(bq_project=config.get('bq_project'),
                                bq_dataset=config.get('bq_dataset'),),
        dag=dag)
    

    
    init_shopify_payouts_temp >> load_shopify_payouts_temp >> init_shopify_payouts >> merge_shopify_payouts >> delete_payouts_temp >> finish
    
   # init_shopify_products_temp >> load_shopify_products_temp >> init_shopify_products >> merge_shopify_products >> delete_products_temp >> finish
    
    init_shopify_collections_temp >> load_shopify_collections_temp >>  init_shopify_collections >> merge_shopify_collections >> delete_collections_temp >> finish

    init_shopify_customers >>  merge_shopify_customers >> finish
