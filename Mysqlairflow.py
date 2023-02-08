#!/usr/bin/env python
# coding: utf-8

# In[31]:


import airflow
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator


from pre_processing import pre_process
from pre_process import process_data

default_args = {
    'owner': 'airflow',    
    'start_date': datetime(2022,3,7)}
with DAG(dag_id="workflow",default_args=default_args,schedule_interval='@daily') as dag:    

    pre_process=PythonOperator(
        task_id="pre_process",
        python_callable=pre_process
    )
    agg=PythonOperator(
        task_id="agg",
        python_callable = process_data
    )
    create_table=MySqlOperator(
        task_id = "create_table",
        mysql_conn_id ="mysql_db",
        sql="CREATE table IF NOT EXISTS employee(empid int, empname VARCHAR(25));"
    )
    pre_process >> agg >> create_table
    


# In[ ]:




