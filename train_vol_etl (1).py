def run_train_volume():
    import psycopg2
    try:
        mydb = psycopg2.connect(host ='dataquality.csqth4u0zi2z.us-west-2.rds.amazonaws.com',
                                dbname= 'postgres',
                                user= 'postgres',
                                password= 'Dataquality',
                                port = 5432)
        cursor = mydb.cursor()
        
    except Exception as error:
        print(error)
        
    def execute_query(query,statement):
            cursor.execute(query)
            if statement == "select":
                result = cursor.fetchall()
                return result
            elif statement == "insert":
                mydb.commit()
            elif statement == "delete":
                mydb.commit()

    #selecting the latest log from congif_table 

    log_select = "select * from master_config where anamoly_check = 'volume'"
    log_result = execute_query(log_select,"select")

    # Based on the results SQL query is built and inserted into train table based on the requirment

    flag = 0

    for log in log_result:

        table_name = log[1]
        column_name = log[2]
        anomaly_check = log[3]
        agg_column = log[5]
        print(table_name)
        print(column_name)
        print(anomaly_check)
        print(agg_column)
        if agg_column == "date" :

                select_sql = """select max(date) from vol_train_table_1 where partner = '{tbl_name}' and column_name = '{col_name}'""".format(tbl_name = table_name, col_name = column_name)
                select_result = execute_query(select_sql,"select")
                for row in select_result:
                        max_date = row[0]

                if max_date != None :
                    
                    insert_sql = """insert into vol_train_table_1 (date,partner,column_name,metrics)
                                        select date,'{tbl_name}','{col_name}',sum({col_name})
                                        from {tbl_name} 
                                        where date <= current_date
                                        and date > '{m_date}'
                                        group by 1""".format(tbl_name = table_name, col_name = column_name,m_date = max_date)
                    execute_query(insert_sql,"insert")
                    print(log)
                    print("inserted latest records on table_1 for.."+table_name)
                    flag += 1
                else:
                    
                    insert_sql = """insert into vol_train_table_1 (date,partner,column_name,metrics)
                                        select date,'{tbl_name}','{col_name}',sum({col_name})
                                        from {tbl_name} 
                                        where date <= current_date 
                                        and date >= (select min(date) from {tbl_name})
                                        group by 1""".format(tbl_name = table_name, col_name = column_name)
                    execute_query(insert_sql,"insert")
                    print(log)
                    print("inserted full records on table_1 for.."+ table_name)
                    flag += 1

        else:

                select_sql = """select max(date) from vol_train_table_2 where partner = '{tbl_name}' and column_name = '{col_name}'""".format(tbl_name = table_name, col_name = column_name)
                select_result = execute_query(select_sql,"select")
                for row in select_result:
                        max_date = row[0]

                if max_date != None :

                    insert_sql = """insert into vol_train_table_2 (date,partner,column_name,agg_column,metrics)
                                    select date,'{tbl_name}','{col_name}',{ag_col},sum({col_name})
                                    from {tbl_name} 
                                    where date <= current_date 
                                    and date > '{m_date}'
                                    group by 1,{ag_col}""".format(tbl_name = table_name, col_name = column_name,ag_col = agg_column,m_date = max_date)
                    execute_query(insert_sql,"insert")
                    print(log)
                    print("inserted latest records on table_2 for.."+table_name)
                    flag += 1
                
                else:

                    insert_sql = """insert into vol_train_table_2 (date,partner,column_name,agg_column,metrics)
                                    select date,'{tbl_name}','{col_name}',{ag_col},sum({col_name})
                                    from {tbl_name} 
                                    where date <= current_date 
                                    and date >= (select min(date) from {tbl_name})
                                    group by 1,{ag_col}""".format(tbl_name = table_name, col_name = column_name,ag_col = agg_column)
                    execute_query(insert_sql,"insert")
                    print(log)
                    print("inserted full records on table_2 for.."+table_name)
                    flag += 1    
    print(flag)