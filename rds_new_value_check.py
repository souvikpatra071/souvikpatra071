import psycopg2
from datetime import date

conn = psycopg2.connect(database="postgres",
                        host="dataquality.csqth4u0zi2z.us-west-2.rds.amazonaws.com",
                        user="postgres",
                        password="Dataquality",
                        port="5432")

cursor = conn.cursor()

def execute_query(query,statement):
    cursor.execute(query)
    if statement == "select":
        result = cursor.fetchall()
        return result
    elif statement == "insert":
        conn.commit()

master_select_stmt = "SELECT * FROM PUBLIC.MASTER_CONFIGURATION" 
master_result = execute_query(master_select_stmt,"select")

for master_row in master_result:
    master_table_name = master_row[0]
    master_column_name = master_row[1]
    master_type = master_row[2]

    if str(master_type).strip() == "value":

        value_config_select_stmt = "SELECT * FROM PUBLIC.VALUE_CONFIGURATION"
        value_config_result = execute_query(value_config_select_stmt,"select")

        value_config_check_value = []

        for value_config_row in value_config_result:
            value_config_tablename = value_config_row[0]
            value_config_column_name = value_config_row[1]

            if master_table_name == value_config_tablename and master_column_name == value_config_column_name:
                value_config_check_value = value_config_row[2]

        daily_select_stmt = "SELECT DISTINCT {} FROM PUBLIC.{}".format(master_column_name,master_table_name)
        daily_table_result = execute_query(daily_select_stmt,"select")

        daily_data_list = []

        anamoly_new_values_list = []
        anamoly_missing_values_list = []

        daily_data_report_date = str(date.today())

        for daily_row in daily_table_result:
            daily_data = daily_row[0]
            daily_data_list.append(daily_data)
            if str(daily_data) not in value_config_check_value:
                anamoly_new_values_list.append(daily_data)

        for value in value_config_check_value:
            if value not in daily_data_list:
                anamoly_missing_values_list.append(value)

        if len(anamoly_new_values_list) > 0:
            insert_stmt_for_new_values = "INSERT INTO PUBLIC.ANAMOLY_DETECTION_DEV VALUES('{}','{}','{}',{},'{}')".format(master_table_name,master_column_name,"New Values","ARRAY "+str(anamoly_new_values_list),daily_data_report_date)
            execute_query(insert_stmt_for_new_values,"insert")
            print("New value inserted...")
        
        if len(anamoly_missing_values_list) > 0:
            insert_stmt_for_missing_values = "INSERT INTO PUBLIC.ANAMOLY_DETECTION_DEV VALUES('{}','{}','{}',{},'{}')".format(master_table_name,master_column_name,"Missing Values","ARRAY "+str(anamoly_missing_values_list),daily_data_report_date)
            execute_query(insert_stmt_for_missing_values,"insert")
            print("Missing value inserted...")