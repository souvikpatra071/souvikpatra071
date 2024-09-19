# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ##Parameter for table name
# MAGIC * input parameter table_name1 = SourceTable = CUSTOMER_DAILY_SALES;
# MAGIC * table_name = table_name1 + '_VW' = CUSTOMER_DAILY_SALES_VW
# MAGIC * pk_table_name = table_name.replace('VW', 'PK_VW', 1) = CUSTOMER_DAILY_SALES_PK_VW      reading from oracle
# MAGIC * inc_table_name = table_name.replace('VW', 'INC_VW', 1) = CUSTOMER_DAILY_SALES_INC_VW   reading from oracle
# MAGIC * configuration_master = CUSTOMER_DAILY_SALES_VW;
# MAGIC * ttm_incr_tables.json =  R_T1_CUSTOMER_DAILY_SALES;
# MAGIC * tbl_name = str("R_T1_"+MDM_INFO.source_object_name).rstrip("_VW") = R_T1_CUSTOMER_DAILY_SALES
# MAGIC * mc_table_name = CUSTOMER_DAILY_SALES_VW

# COMMAND ----------

# DBTITLE 1,Parameters
from datetime import datetime as dt
table_name1 = getArgument("SourceTable")
scope = getArgument("scope")
mc_rs_schema= getArgument("rs_schema")
mc_s3_temp_path= getArgument("s3_temp_path")
delta_database_name = getArgument("delta_database_name")
delta_table_name = getArgument("delta_table_name")
mc_source_system = "TTM"
region = getArgument("region")

print(table_name1)
print(scope)
print(delta_database_name)
print(delta_table_name)
print(mc_source_system)
print(region)


# COMMAND ----------

# MAGIC %run "../Includes/TTM_Common_Function"

# COMMAND ----------

# MAGIC %run "../Includes/CommonUtils"

# COMMAND ----------

# MAGIC %run "../Sales/EU/File_Data_Quality"

# COMMAND ----------

# DBTITLE 1,Get Countries
# region = 'AP'
# region_org = 'AP'

lkp_query = """
SELECT distinct country_code from {0}.{1} where region={2}
""".format(delta_database_name,mc_table_name,region_org)
df_country_code = runDeltaQuery(lkp_query)
print("df_country_code")
print(df_country_code.count())

rows=df_country_code.collect()
final_country_code=""
for row in rows:
 final_country_code += "'"+str(row['country_code']) + "',"
final_country_code = final_country_code.rstrip(",")
print(final_country_code)


# COMMAND ----------

# DBTITLE 1,table filters
record_type_query = "select full_or_incremental_load from "+delta_database_name+"."+delta_table_name+" where active_flag='A' and source_object_name='"+table_name+"'"
record_type = spark.sql(record_type_query).collect()[0][0]
print(record_type)

table_filter = "and (source_system_code in ('SAP','Profit Center') and country_code in ({0})) and (record_type='{1}' or record_type is null or record_type not in ('W','D')) and country_code not in ('USA')".format(final_country_code,record_type)
rs_table_filter = "and (source_system_code in ('SAP','Profit Center') and country_code in ({0}) ) and (record_type='{1}' or record_type is null or record_type not in ('W','D')) and country_code not in ('USA') ".format(final_country_code,record_type)
print(table_filter)
print(rs_table_filter)



# COMMAND ----------

# DBTITLE 1,Databricks Load - Full / Incremental
try:

  if mc_load_type.upper() == 'I':
    fp_max_date=spark_session.sql("select max(delta_load_ts) max_date from {0}.{1} where source_object_name='{2}'".format(delta_database_name,delta_table_name,mc_table_name))
    max_date=[i['max_date'] for i in fp_max_date.select("max_date").collect()]
    #max_date=fp_max_date.select("max_date").rdd.flatMap(lambda x: x).collect()
    max_datetime_Obj=''
    db_load_count=[]

  #print(max_date[0], max_date)
  ############################################### #If true, full load is activated from oracle  ###############################################
  if mc_load_type.upper() == 'D' or mc_load_type.upper() == 'W' or (max_date[0] =='null' or max_date[0] =='') :#len(max_date)==1:
    print("Full Load is starting...")
    #Fetching Full load from db
    db_load_main_qry="select "+col_array+" from {0}.{1} where region={2} and country_code not in ('USA') and record_type='{3}'".format(delta_database_name,table_name,region_org,record_type)
    #Fetching count of full load
    db_load_cnt_qry="select count(*) as count from {0}.{1} where region={2} and record_type='{3}'".format(delta_database_name,table_name,region_org,record_type)

  ###If statement false, incremental load is initiated from oracle
  elif mc_load_type.upper()=='I' and (max_date[0] !='null'  or max_date[0] !='') :#len(str(max_date[0]))>10 :
    for m_dt in max_date:
      max_datetime_Obj=dt.strptime(m_dt,"%Y-%m-%d %H:%M:%S")
    print("Incremental load is starting...")
    #Fetching incremental load based out last updated date from log file

    db_load_main_qry="""select """+col_array+""" from {3}.{0} where datediff({1},'{2}')>0""".format(inc_table_name,mc_col_name,max_datetime_Obj.strftime("%Y-%m-%d %H:%M:%S"),delta_database_name)

    #Fetching count of incremental data from oracle
    db_load_cnt_qry="""select count(*) as count from {3}.{0} where datediff({1},'{2}')>0""".format(inc_table_name,mc_col_name,max_datetime_Obj.strftime("%Y-%m-%d"),delta_database_name)
  print(db_load_main_qry)

  df_db_load=spark.sql(db_load_main_qry)
  df_db_load_count = spark.sql(db_load_cnt_qry)

  #df_oracle_load=df_oracle_load.na.fill('')
  df_db_load=df_db_load.fillna('')
  db_load_count = df_db_load.count()
  print("db count: ", db_load_count)

except:
  error_log+=mc_table_name+"1"
  error_description+=mc_table_name+" - Error; Extraction is not completed; "
  log_capture_if_error(error_log, error_description)
  raise

# COMMAND ----------

# DBTITLE 1,Loading into table
# df_db_load.createOrReplaceTempView("new_cds_view")
# cds = spark.sql("create table gdna_reporting_dev.new_cds_automation (select * from new_cds_view)")
# print("table load completed")


# COMMAND ----------

# DBTITLE 1,Writing to S3 location
try:
  print("Output is writing into S3 location...")
  print(mc_s3_landing_path+mc_table_name+"/")
  #Delete existing files from landing folder
  dbutils.fs.rm(mc_s3_landing_path+mc_table_name+"/",True)
  if mc_load_type.upper() == 'D' or mc_load_type.upper() == 'W' or (max_date[0].lower() =='null' or max_date[0] =='') :
     print("There is no archive needed for full load")
  else :
    df_db_load1=df_db_load.fillna('')
    df_db_load1.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
    .option("header","true").option("inferSchema","true") \
    .option("delimiter", mc_delimiter).option("quote", mc_quote_char).option("quoteAll", "true").csv(mc_s3_landing_path+mc_table_name+'/')


  #df_oracle_load.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv').option("header","true").option("inferSchema","true").option("delimiter",mc_delimiter).csv(mc_s3_archive_path+mc_table_name+'/'+"Year="+dt.now().strftime("%Y")+"/Month="+dt.now().strftime("%m")+"/Day="+dt.now().strftime("%d"))

    dbutils.fs.cp(mc_s3_landing_path+mc_table_name+"/",mc_s3_archive_path+mc_table_name+'/'+"Year="+dt.now().strftime("%Y")+"/Month="+dt.now().strftime("%m")+"/Day="+dt.now().strftime("%d")+"/",True)

except:
  error_log+=mc_table_name+"2"
  error_description+=mc_table_name+" - Error; Data is not Saved in S3; "
  log_capture_if_error(error_log, error_description)
  raise

# COMMAND ----------

# DBTITLE 1,Implementing DQ checks
s3_bucket = 'amgen-s3-gco-dna-edl-dev'
region = 'EU'
country_code = 'AT'
source_layer = 'TTM'
target_source_system = 'TTM'
data_source_name = 'TTM'
file_uploaded_by = ''
notebookname = '/Users/svc-gcois-prod@am.corp.amgen.com/etl_jobs/TTM/load_gdna_ttm_new_global_jira'
file = 'CUSTOMER_DAILY_SALES_VW.csv'
error_path = 'amgen-s3-gco-dna-edl-dev/Dq_file/error/'
web_flag = False

# cds_df = spark.sql("select * from {0}.{1} where material_number = '9002405' and fiscal_date in ('2024-08-06','2024-08-07','2024-08-08') and indication_name is null and region = '{2}' and country_code not in ('USA') and (record_type = 'D' or record_type = '' or record_type is null)".format(delta_database_name, table_name, region))
data_extract = df_db_load
print("Started process for country: "+str(country_code))
try:
  config = getAllConfigMasterFields_new(
      delta_db_schema = delta_database_name, 
      region = region, 
      country_code = country_code, 
      source_system = mc_source_system, 
      source_layer = source_layer, 
      target_source_system = target_source_system
      )
except:
  config = None
  print('Config Entries not found for given filter conditions...')

for config_data in config:
    configuration_id = config_data['configuration_id']
    source_object_name = config_data['source_object_name']
    file_name_format = config_data['filename_format']
    source_type = config_data['location']
    delta_load_ts = config_data['delta_load_ts']
    delimiter = config_data['delimiter']
    delta_stg_tables = config_data['delta_stg_tables']
    delta_stg_table_view = config_data['delta_stg_table_view']
    delta_pret1_table = config_data['delta_pret1_table']
    delta_t1_table = config_data['target_table']
    date_pattern = config_data['date_pattern']
    source_path = config_data['source_path']
    landing_path = (config_data['s3_landing_path'] or '').format(s3_bucket = s3_bucket,country=country_code)
    archive_path = (config_data['s3_archive_path'] or '').format(s3_bucket = s3_bucket)
    column_list = config_data['column_list']
    pre_process_function = config_data['pre_process_function']
    quote_character = config_data['quote_character']
    encode_val = config_data['encode_val']
    decode_val = config_data['decode_val']
    header = config_data['header']
    inferschema = config_data['inferschema']
    actual_file_name = config_data['actual_file_name']
    stg_skip_indicator = config_data['stg_skip_indicator']
    pre_t1_skip_indicator = config_data["pre_t1_skip_indicator"]
    source_layer=config_data["source_layer"]
    country=config_data["country"]

    print("file_name_format from config master :", file_name_format)

errorTableName = 'err_'+delta_stg_tables
print(delta_stg_tables)
print(errorTableName)

dic = {"delta_stg_tables":delta_stg_tables,
        "delta_t1_table":delta_t1_table,
        "file_name_format":file_name_format,
        "source_path":source_path,
        "delta_db_schema":delta_database_name,
        "data_source_name":data_source_name,
        "country_code":country_code,
        "file_uploaded_by":file_uploaded_by,
        "s3_bucket":s3_bucket,
        "region":region,
        "notebookname":notebookname,
        "df":data_extract,
        "errorTableName":errorTableName,
        "file":file,
        "error_file_path":error_path,
        "web_flag": web_flag}

df_valid, error_file_s3_path, high_priority_flag = dataqualitychecks_ttm(**dic)

# COMMAND ----------

# DBTITLE 1,Merge into Delta

try:
  df_valid.createOrReplaceTempView('MainTempView')
#   mdm_table_details=secret_data['R_T1_'+mc_table_name.upper().replace('_VW',"")]
  #mdm_table_primarykey_list= mdm_table_details['primary']
  #length_of_pkeys=len(mdm_table_primarykey_list)
  # table_colums=mdm_table_details['columns']

  #json standardization
  col_query = "select column_list from "+delta_database_name+".dna_landing_query where source_system = '{0}' and filename = '{1}' ".format(mc_source_system,table_name)
  print(col_query)
  table_colums = spark.sql(col_query).collect()[0][0].split(',')


  length_of_pkeys=len(table_colums)
  print(length_of_pkeys)
  #delta_truncate_qry="TRUNCATE TABLE {0}.{1}".format(delta_database_name,"r_t1_"+mc_table_name.upper().rstrip('_VW'))
  delta_delete_qry = "delete from {0}.{1} where 1=1 {2}".format(delta_database_name,"r_t1_"+mc_table_name.upper().replace('_VW',""),table_filter)

#Creating merge query
  merge_query=""
  print("Creating Merge Query for the table: "+"r_t1_"+mc_table_name.upper().replace('_VW',""))

  merge_query="merge into {0}.{1} stag_table  using MainTempView tempview on stag_table.pkey = tempview.pkey ".format(delta_database_name,"r_t1_"+mc_table_name.upper().replace('_VW',""))

  merge_query+=" when matched then update set "
  merge_query1=" when not matched then insert ("
  merge_query2=") VALUES ("
  for table_cols_index in range(0,len(table_colums)):
    merge_query+="stag_table.{0} = tempview.{1},".format(table_colums[table_cols_index],table_colums[table_cols_index])
    merge_query1+="{0},".format(table_colums[table_cols_index])
    merge_query2+="tempview.{0},".format(table_colums[table_cols_index])
  #merge_query+="stag_table.{0} ='{1}'".format('dl_update_timestamp',str(dt.now().strftime('%Y-%m-%d %H:%M:%S')))
  #merge_query1+='dl_insert_timestamp,dl_update_timestamp'
  #merge_query2+="'"+str(dt.now().strftime('%Y-%m-%d %H:%M:%S'))+"','"+str(dt.now().strftime('%Y-%m-%d %H:%M:%S'))+"'"
  merge_query=merge_query.rstrip(",")
  merge_query1=merge_query1.rstrip(",")
  merge_query2=merge_query2.rstrip(",")
  dynamic_merge_qry=merge_query+merge_query1+merge_query2+")"
  #print(dynamic_merge_qry)
  print("Executing Merge/Full load Query...")
  #print(mc_load_type.upper(), max_date[0])

  if mc_load_type.upper() == 'D' or mc_load_type.upper() == 'W' or (max_date[0].lower() =='null' or max_date[0] =='') :
    #print("Coming into this")
    #df_oracle_load_new=df_oracle_load \
    #.withColumn("dl_insert_timestamp",lit(dt.now().strftime('%Y-%m-%d %H:%M:%S'))) \
    #.withColumn("dl_update_timestamp",lit(dt.now().strftime('%Y-%m-%d %H:%M:%S')))
    #df_oracle_load_new.createOrReplaceTempView('MainTempView')
    print("delete is starting")
    print(delta_delete_qry)
    spark_session.sql(delta_delete_qry)
    print('delete complete')
    delta_df_cnt=spark_session.sql("select count(*) as count from {0}.{1} where 1=1 {2}".format(delta_database_name,"r_t1_"+mc_table_name.upper().replace('_VW',""),table_filter))
    delta_df_var=[i['count'] for i in delta_df_cnt.select("count").collect()]
    #delta_df_var=delta_df_cnt.select("count").rdd.flatMap(lambda x:x).collect()
    print(delta_df_var[0])

    print("insert into table {0}.{1} select * from MainTempView ".format(delta_database_name,"r_t1_"+mc_table_name.upper().replace('_VW',"") ))
    spark_session.sql("insert into table {0}.{1} select * from MainTempView".format(delta_database_name,"r_t1_"+mc_table_name.upper().replace('_VW',"") ))
    print("Delete and Full Load is completed in Delta")

  elif mc_load_type.upper()=='I' and (max_date[0].lower() !='null'  or max_date[0] !='') :
    spark_session.sql(dynamic_merge_qry)
    print("Merge into delta tables from db is completed. :"+str(mc_table_name))

except:
  error_log+=mc_table_name+"3"
  error_description+=mc_table_name+" - Error; Merge qry is not executed; "
  log_capture_if_error(error_log, error_description)
  raise


# COMMAND ----------

# DBTITLE 1,Deleting pkeys from delta
try:
  print("Given Table is :"+mc_load_type.upper())
  if mc_load_type.upper() == 'I':
    #try:
    pkeys_db="select pkey from {0}.{1}".format(delta_database_name,pk_table_name)
    pkeys_df=''
    pkeys_del_cnt=0
    print(pkeys_db)

    print("Fetching Primary keys from db")
    pkeys_df = spark.sql(pkeys_db)
    print(pkeys_df.count())
    pkeys_df.createOrReplaceTempView('pkeys_temp')
    #pkeys_del_cnt=pkeys_df.count()
    print("deleting from staging table")
    #delete_qry="delete from staging.{0} where pkey not in(select pkey from staging.{1})".format(tbl_name,'delta_tmp_new')
    #del_cnt="select distinct pkey from staging.{0} minus select distinct pkey from staging.{1}".format(tbl_name,'delta_tmp_new')
    del_cnt="select pkey from {0}.{1} minus select pkey from pkeys_temp".format(delta_database_name,tbl_name)
    #delete_qry="delete from staging.{0} where pkey in(select distinct pkey from staging.{0} minus select distinct pkey from staging.{1})".format(tbl_name,'delta_tmp_new')

    #print(delete_qry)
    list_pkey=spark_session.sql(del_cnt)
    #pkey_val=list_pkey.select('pkey').rdd.flatMap(lambda x:x).collect()


    # del_cnt.show()
    print("DELETE ROWS: "+str(list_pkey.count()) )
    if list_pkey.count()!=0 :
      '''list_pkey_val=''
      for val in pkey_val:
        list_pkey_val+="'"+val+"',"
      list_pkey_val=list_pkey_val.rstrip(",")
      delete_qry="delete from {0}.{1} where pkey in ({2})".format(delta_database_name,tbl_name,list_pkey_val)'''
      delete_qry="delete from {0}.{1} where pkey in (select distinct pkey from {0}.{1} minus select distinct pkey from pkeys_temp) {2}".format(delta_database_name,tbl_name,table_filter)
      print(delete_qry)
      spark_session.sql(delete_qry)
      #print("True")
    #spark_session.sql("DROP TABLE staging.delta_tmp_new")
    spark_session.catalog.dropTempView("pkeys_temp")
except:
    error_log+=mc_table_name+"-Error in Delete"
    error_description+="Error in deleting data from delta files."
    log_capture_if_error(error_log, error_description)
    raise

#Capture completed status
if len(error_log) < 1:
  comp_status="Success"
else:
  comp_status="Failure"

#Capture End Time 
from datetime import datetime as dt

end_time=''
end_time=dt.now()
time_diff=end_time-start_time

parent_batch_process_name=mc_source_system + "-Upsert"

auditlog.file_process_log(mc_table_name,db_load_count,mc_config_id,mc_source_system,dt.now().strftime('%Y-%m-%d %H:%M:%S'),start_time.strftime('%Y-%m-%d %H:%M:%S'),new_parentlog,delta_database_name,"file_process_log")

auditlog.parent_batch_process(new_parentlog,parent_batch_process_name,start_time.strftime('%Y-%m-%d %H:%M:%S'),end_time.strftime('%Y-%m-%d %H:%M:%S'),comp_status,comp_status,error_log,error_description,mc_source_system,delta_database_name,"parent_batch_process")



# # COMMAND ----------

# # DBTITLE 1,Load into Redshift
# try:
#   rs_table_name="r_t1_"+mc_table_name.replace('_VW',"").lower()
#   #Query to fetch data from delta
#   delta_final_qry=""" SELECT *
#         from
#         {0}.{1} where 1=1 {2} """.format(delta_database_name,rs_table_name,table_filter)
#   print(delta_final_qry)
#   Delta_df=spark.sql(delta_final_qry)
#   Delta_df=Delta_df.fillna("")

#   print("delta fetched")
#   #Redshift delete Query
#   rs_delete_qry="delete from {0}.{1} where 1=1 {2} ".format(mc_rs_schema,rs_table_name,rs_table_filter)
#   #drop table if exists staging.r_t1_customer_address cascade
#   print(rs_delete_qry)
#   print("Delete and write in REDSHIFT table.")

#   # Changing the column width and loading into Redshift:
#   val_length=500
#   metadata = {'maxlength': val_length}
#   for colname in Delta_df.schema.fields:
#      if repr(colname.dataType) == 'StringType':
#         Delta_df = Delta_df.withColumn(colname.name, trim(Delta_df[colname.name]))

#         '''max_val=Delta_df.select(length(max(colname.name))).rdd.flatMap(lambda x: x).collect()
#         if max_val[0]!=0: 
#           max_val_len=max_val[0]*3
#         else:
#           max_val_len=50

#         if max_val_len >=120:
#           metadata = {'maxlength': max_val_len}
#           Delta_df = Delta_df.withColumn(colname.name, trim(Delta_df[colname.name]).alias(colname.name, metadata=metadata))'''


#   print("Column width changed")
#   Delta_df.write.format("com.databricks.spark.redshift").option("url", rs_jdbcUrl)\
#   .option("dbtable", mc_rs_schema+"."+ rs_table_name)\
#   .option("tempdir", mc_s3_temp_path).option("forward_spark_s3_credentials",True)\
#   .option("preactions",rs_delete_qry).option("tempformat","CSV").mode("append").save()

#   rs_load_stat=1
#   print("Delete and write completed")
# except:
#   error_log+=mc_table_name+" - Redshift load error "
#   error_description+=mc_table_name+" - Error; Redshift delete and load qry is not executed;"
#   log_capture_if_error(error_log, error_description)
#   raise



# # COMMAND ----------

# # DBTITLE 1,Logging into batch process log
# try:
#   #Idenfiying the max date from Redshift
#   if mc_load_type.upper()=='I' :
#     tgt_qry="""select max({0}) as updated_date from {1}""".format(mc_col_name,mc_rs_schema+"."+rs_table_name)
#     df_max_date=spark_session.read.format("com.databricks.spark.redshift").option("url", rs_jdbcUrl).option("query", tgt_qry).option("tempdir", mc_s3_temp_path).option("forward_spark_s3_credentials",True).load()
#     updated_date = df_max_date.select("updated_date").collect()[0]
#     print(updated_date[0])
#   else:
#     updated_date = []

#   #Updating Max date for incremental table in config master
#   if mc_load_type.upper()=='I' and oracle_load_count!=0 and rs_load_stat==1:
#     upd_dt=updated_date[0]
#     print('Update dates: ' + str(updated_date[0]))
#     #if upd_dt =='':
#     delta_bool=False
#     while (not(delta_bool)):
#       delta_bool=log_maxdata_to_delta(upd_dt,mc_table_name,mc_source_system)
#       if delta_bool:
#         break
#       else:
#         time.sleep(30)

# except:
#     error_log=mc_table_name+"Redshift / configuration master error"
#     error_description="Redshift / configuration master queries are not executed"
#     log_capture_if_error(error_log, error_description)
#     #raise

#     #Verifying Redshift count & Oracle count:
# src_qry="""select count(*) as count from {0}.{1} where 1=1 {2}""".format(delta_database_name,table_name1,table_filter)
# tgt_qry="""select count(*) as count from {0} where 1=1 {1}""".format(mc_rs_schema+"."+rs_table_name,rs_table_filter)
# #Verify Redshift count & DB count:
# src_cnt=spark.sql(src_qry)
# srccnt=[i['count'] for i in src_cnt.select("count").collect()]
# #srccnt=src_cnt.select("count").rdd.flatMap(lambda x: x).collect()

# tgt_cnt=spark_session.read.format("com.databricks.spark.redshift").option("url", rs_jdbcUrl).option("query", tgt_qry).option("tempdir", mc_s3_temp_path).option("forward_spark_s3_credentials",True).load()
# tgtcnt=[i['count'] for i in tgt_cnt.select("count").collect()]
# #tgtcnt=tgt_cnt.select("count").rdd.flatMap(lambda x: x).collect()


# print("DB count  :"+str(int(srccnt[0]))+"\nRedshift count: "+str(int(tgtcnt[0])))
# if srccnt[0] != int(tgtcnt[0]):
#   error_log=mc_table_name+"Redshift/DB count Mismatch"
#   error_description="Redshift / DB count Mismatch"
#   log_capture_if_error(error_log, error_description)
#   raise Exception ("Redshift and DB counts are mismatch")

# #Capture completed status
# if len(error_log) < 1:
#   comp_status="Success"
# else:
#   comp_status="Failure"

# #Capture End Time
# end_time=''
# end_time=dt.now()
# time_diff=end_time-start_time

# parent_batch_process_name=mc_source_system + "-Upsert"

# auditlog.file_process_log(mc_table_name,db_load_count,mc_config_id,mc_source_system,dt.now().strftime('%Y-%m-%d %H:%M:%S'),start_time.strftime('%Y-%m-%d %H:%M:%S'),new_parentlog,delta_database_name,"file_process_log")

# auditlog.parent_batch_process(new_parentlog,parent_batch_process_name,start_time.strftime('%Y-%m-%d %H:%M:%S'),end_time.strftime('%Y-%m-%d %H:%M:%S'),comp_status,comp_status,error_log,error_description,mc_source_system,delta_database_name,"parent_batch_process")

