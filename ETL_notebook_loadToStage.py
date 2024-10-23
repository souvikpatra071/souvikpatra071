from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Define database connection
db_connection_str = 'mysql+mysqlconnector://user:password@host:port/database'
db_engine = create_engine(db_connection_str)

# Batch dataframe using as per the question input
def load_data_to_staging(provided_df, **kwargs):
    df = provided_df  # Use the provided dataframe

    # Insert into the staging table
    df[['Customer_Name', 'Customer_Id', 'Open_Date', 'Last_Consulted_Date', 'Vaccination_Id',
        'Dr_Name', 'State', 'Country', 'DOB', 'Is_Active']].to_sql('Staging_Customers', con=db_engine, if_exists='append', index=False)

def split_data_by_country(**kwargs):
    # Query to get all data from the staging table
    query = 'SELECT * FROM Staging_Customers'
    staging_df = pd.read_sql(query, con=db_engine)
    
    # Split the data by country
    countries = staging_df['Country'].unique()
    
    for country in countries:
        country_df = staging_df[staging_df['Country'] == country]
        table_name = f'Table_{country}'
        
        # Add derived columns AGE, country as per the input
        country_df['Age'] = (pd.Timestamp.now() - country_df['DOB']).dt.days // 365
        country_df['Days_Since_Last_Consulted'] = (pd.Timestamp.now() - country_df['Last_Consulted_Date']).dt.days
        
        # Insert into country-specific tables
        country_df[['Customer_Name', 'Customer_Id', 'Open_Date', 'Last_Consulted_Date', 'Vaccination_Id', 
                    'Dr_Name', 'State', 'Country', 'Post_Code', 'DOB', 'Is_Active', 
                    'Age', 'Days_Since_Last_Consulted']].to_sql(table_name, con=db_engine, if_exists='append', index=False)

# Create DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG('process_customer_data', default_args=default_args, schedule_interval='@daily')

# Define tasks
provided_df = pd.DataFrame({
    'Customer_Name': ['Alex', 'John', 'Mathew', 'Matt', 'Jacob'],
    'Customer_Id': [123457, 123458, 123459, 12345, 1256],
    'Open_Date': ['20101012', '20101012', '20101012', '20101012', '20101012'],
    'Last_Consulted_Date': ['20121013', '20121013', '20121013', '20121013', '20121013'],
    'Vaccination_Id': ['MVD', 'MVD', 'MVD', 'MVD', 'MVD'],
    'Dr_Name': ['Paul', 'Paul', 'Paul', 'Paul', 'Paul'],
    'State': ['SA', 'TN', 'WAS', 'BOS', 'VIC'],
    'Country': ['USA', 'IND', 'PHIL', 'NYC', 'AU'],
    'DOB': ['06031987', '06031987', '06031987', '06031987', '06031987'],
    'Is_Active': ['A', 'A', 'A', 'A', 'A']
})

load_data_task = PythonOperator(
    task_id='load_data_to_staging',
    python_callable=load_data_to_staging,
    op_kwargs={'provided_df': provided_df},  # Pass the provided dataframe
    dag=dag
)

split_data_task = PythonOperator(
    task_id='split_data_by_country',
    python_callable=split_data_by_country,
    dag=dag
)

# Define task dependencies
load_data_task >> split_data_task
