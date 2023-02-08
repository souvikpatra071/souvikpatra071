from email.mime import message
import uuid
import datetime
import csv
from datetime import timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



def fetch_data(**context):
    sql_stmt=''' 
                select count(runs_off_bat) as total,CASE
                WHEN runs_off_bat = 6 THEN 'Total_sixes'
                WHEN runs_off_bat = 4 THEN 'Total_fours'
                ELSE 'None'
                END AS type_of_run from ipl where striker = 'JC Buttler'
                group by runs_off_bat
                having runs_off_bat = 6 or runs_off_bat = 4 
                 '''

    pg_hook = PostgresHook(
    postgres_conn_id = 'postgres_db',
    schema = 'temp'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    #for i in cursor:
    #   print("i : {0} - j : {1}".format(i[0] , i[1]))
    k=cursor.fetchall()
    context['ti'].xcom_push(key='df',value=k)
    

def converts(**context):
    fetch=context['ti'].xcom_pull(key='df')
    fields = ['Total', 'Type_of_run'] 
    rows=list(fetch)
    print(rows)
    
    with open('/Users/hariharanthirumurugan/Downloads/writer.csv', 'w') as f:
        write = csv.writer(f)
        write.writerow(fields)
        write.writerows(rows)


with DAG(dag_id="email_dag",start_date=datetime.datetime(2022, 7, 13),schedule_interval="@once",catchup=False) as dag:
     
     
     
     
     collecting=PythonOperator(
         task_id='fetch_data',
         python_callable=fetch_data
         )

    
     converting=PythonOperator(
        task_id ='convert_data',
        python_callable=converts

     )


     mail_job=EmailOperator(
        task_id='email_alerts',
        to='''hajira.shireen@agilisium.com''',
        subject="ipl_data",
        html_content='''<h1> Jos Buttler IPL stats </h1>''',
        files=['/Users/hariharanthirumurugan/Downloads/writer.csv'],
        trigger_rule=TriggerRule.ALL_DONE,

        
     )

 


collecting >> converting >> mail_job
