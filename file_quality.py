# Databricks notebook source
from pytz import timezone
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import UserDefinedFunction
import pandas as pd
import paramiko
import os
import io
from urllib.parse import urlparse
from datetime import datetime as dqdt
import datetime as dt
from pyspark.sql import window as w
import boto3
from botocore.client import Config
import re
import time

import pyspark.sql.functions as f
import openpyxl

from pyspark.sql import Window
from pyspark.sql import functions as F
import time
from datetime import datetime
import xlsxwriter
import jinja2

web_dq_flag = False
scope=getArgument('scope')
aws_access_key_id = dbutils.secrets.get(scope, 'boto3-aws-access-key')
aws_secret_key_id = dbutils.secrets.get(scope, 'boto3-aws-secret-key')
s3 = boto3.client('s3', config=Config(signature_version='s3v4', region_name='us-west-2'),aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_key_id)

# COMMAND ----------

def get_time_zone(region):
  if region == 'ICON':
      time_zone = 'EST'
  elif region == 'EU':
      time_zone = "Europe/Amsterdam"
  else:
      time_zone = "Europe/Amsterdam"
  return time_zone

# COMMAND ----------

def highlight_field(row):   
  a=[]    
  ret = ['' for i in row.index]
  for x in row.index:
      y=x.replace('_',' ')
      a.append(y)

  for i in row.ERROR_DESCRIPTION.split(';'):
      i=i.replace('_',' ')
      for y in a:
        if y.lower() in i.lower():
          field_name=y.replace(' ','_')
          ret[row.index.get_loc(field_name)] = 'background-color: yellow'
  return ret

# COMMAND ----------

def list_columns_check(list_df_columns, column_list,data_source_name):  # igonre case sensitive(anand)
    print("Structure check Column_list of dataframe:", list_df_columns)
    print("List of Columns expected in file:", column_list)
    if any(i in data_source_name for i in ['MDM','ALLOCATION']) or data_source_name in ('IC_ITM_BR_MDTR','IC_ITM_BR_DDD','IC_ITM_BR_PM','IC_ITM_BR_NRA','LOCAL'):
        column_list = [x.upper() for x in column_list]
        list_df_columns = [y.upper() for y in list_df_columns]
    if list_df_columns == column_list:
        return True
    else:
        return False

# COMMAND ----------
def dataqualitystructurechecks(df_source_columns,region, file,file_name_format,delta_db_schema,data_source_name,country_code,delta_stg_tables,delta_t1_table,file_uploaded_by,web_flag,team='internal'):
    global web_dq_flag
    web_dq_flag = web_flag
    print("Enter:  dataqualitystructurechecks at " + str(time.ctime(int(time.time()))), region)
    dq_rules_delta_query = "select * from {0}.file_dq_rules where rule_name = 'structure_check' and upper(country_code) like upper('%{3}%') and upper(source_system) = upper('{1}') and upper('{2}') like upper(file_name) and upper(active_flag)= 'A' and replace(upper('{4}'),'*','') like concat(replace(upper(config_filename_format),'*','%'),'%')".format(delta_db_schema, data_source_name, file, country_code, file_name_format)
    print("file_dq_rules query: ", dq_rules_delta_query)
    df_dq_rules = spark.sql(dq_rules_delta_query)
    print("Fetching dq details")
    print("Dq rules rows : {0}".format(df_dq_rules.collect()))
    rule_cnt = df_dq_rules.count()
    print(rule_cnt, " rules to be process")
    if (rule_cnt == 0):
        print("Exit:  dataqualitystructurechecks at " +
              str(time.ctime(int(time.time()))))
        return 1
    elif(rule_cnt > 0):
        column_list = df_dq_rules.collect()
        print("Getting columns list for structure check")
        column_list = [column.column_name for column in column_list]
        column_list = column_list[0].split(',')
        print("Checking whether the columns are in Order")
        structure_check_result = list_columns_check(
            df_source_columns, column_list,data_source_name)
        if(team == 'mcm_internal'):
          if(structure_check_result == False):
            print('struct_mail_trigger here.....')
            send_mail('', team,file , delta_db_schema, country_code, "structure_check", '','',region,file_uploaded_by,'')
            validation_type = 'Structure_Check'  # redshift as
            rule_name = file_name_format.split("*")[0]+"_"+validation_type
            Error_description = 'Structure check failed'
            validation_flag = False
          else:
            validation_type = 'Structure_Check'
            rule_name = file_name_format.split("*")[0]+"_"+validation_type
            Error_description = ''
            validation_flag = True
          spark.sql("""insert into {0}.file_delivery_validation values('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')""".format(delta_db_schema, dqdt.now(), country_code, file_name_format, file, delta_stg_tables, delta_t1_table, rule_name, validation_type, '', '', Error_description, 'Pre_check', str(validation_flag).upper()))
          if validation_flag:
            print("Data inserted into the file_delivery_validation table")
            print("All the columns are in order")
            print("Exit:  dataqualitystructurechecks at " +
                  str(time.ctime(int(time.time()))))
            return True
          else:
            print("Exit:  dataqualitystructurechecks at " +
              str(time.ctime(int(time.time()))))
        else:
          if(structure_check_result == False):
              print('struct_mail_trigger here')
              send_mail('', "internal",file , delta_db_schema, country_code, "structure_check", '','',region,file_uploaded_by,'')
              validation_type = 'Structure_Check'  # redshift as
              rule_name = file_name_format.split("*")[0]+"_"+validation_type
              Error_description = 'Structure check failed'
              validation_flag = False
          else:
              validation_type = 'Structure_Check'
              rule_name = file_name_format.split("*")[0]+"_"+validation_type
              Error_description = ''
              validation_flag = True
          spark.sql("""insert into {0}.file_delivery_validation values('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')""".format(delta_db_schema, dqdt.now(), country_code, file_name_format, file, delta_stg_tables, delta_t1_table, rule_name, validation_type, '', '', Error_description, 'Pre_check', str(validation_flag).upper()))
          if validation_flag:
            print("Data inserted into the file_delivery_validation table")
            print("All the columns are in order")
            print("Exit:  dataqualitystructurechecks at " +
                  str(time.ctime(int(time.time()))))
            return True
          else:
            return False
    else:
        print("Exit:  dataqualitystructurechecks at " +
              str(time.ctime(int(time.time()))))
        return False

# COMMAND ----------

def get_template(web_dq_flag,x, to_day, er_cnt, country_code, url_list, fln_dist_cnt, sub, header, Action,level):
  head = """\
    <html>
      <body>  
        <table style="font-family:Calibri;border:1px solid black;width: 80%;">
        <tbody>
        <tr>
        <td align="center">
        <h1 align="center" style="font-size:12pt;padding:0;margin:0;serif;background-color:#1F60A9;font-weight:bold;text-align:center;margin:0;line-height:90%;"> <span style="color:White;font-size:16pt;letter-spacing:0.1pt;line-height:90%;"> FILE QUALITY CHECK  </span> </h1>
        </td>
        </tr>
        <tr>
        <td align="center">
        <h1 align="center" style="color: red;font-size:16pt;font-weight:bold;padding:0;margin:0;"> {header} </h1>
        </td>
        </tr>
        <tr>
        <td align="center">
        <h1 align="center" style="color: #FFA500;font-size:12pt;font-weight:bold;padding:0;margin:0;"> {to_day} </h1>
        </td>
        </tr>
        <tr>
        <td>&nbsp;</td>
        </tr>""".format(to_day=to_day, header=header)
  if level == 'structure_check':
    err_description = """\
          <tr>
          <td>
          <p style="color: red;background=color:yellow;">{sub}</p>
          <p> Kindly validate the structure of template and upload in source location with expected format. </p>  
          </td>
          </tr></br>
          
    """.format(sub=sub)
    action_items = ''
    error_summary_country_code = ''
    err_summary_source_err_file_loc = ''
    additional_spaces = ''
  else:
    err_description = """\
          <tr>
          <td align="left">
          <h1 align="left" style="color:black;font-size:12pt;font-weight:bold;padding:0;margin:0;">ERROR DESCRIPTION:</h1>
          </tr>
          <tr></tr>
          <tr>
          <td>
          <p style="color: red;background=color:yellow;">{sub}</p><br>
          </td>
          </tr>
          <tr>
          </tr>""".format(to_day=to_day, sub=sub, header=header)
    action_items = """
          <tr>
          <td align="left">
          <h1 align="left" style="color:black;font-size:12pt;font-weight:bold;padding:0;margin:0;">ACTION ITEMS:</h1>
          </tr>
          <tr>
          </tr></br>
          <tr>
          <td>
          <p style="color: green;background=color:yellow;"> {Action}
          </p>
          </td>
          </tr></br>
          """.format(Action=Action)
    error_summary_country_code="""<tr>
          <td align="left">
          <h1 align="left" style="color:black;font-size:12pt;font-weight:bold;padding:0;margin:0;">ERROR SUMMARY:</h1>
          </tr>
          <tr></tr>
          <td>
          <table style="border: 0px white;">
          <tr>
          <th align="left" style='width: 250px;'>Number of Error Records&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;:&nbsp{er_cnt} </th> 
          </tr>
          <tr>
          <th align="left" style='width: 250px;'>Number of Files Processed&nbsp;&nbsp;&nbsp;&nbsp:&nbsp{fln_dist_cnt}</th> 
          </tr>
          <tr> 
          <th align="left" style='width: 250px;' >Country Code&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;:&nbsp{country_code}</th>
          </tr>""".format(er_cnt=er_cnt,country_code=country_code,fln_dist_cnt=fln_dist_cnt)

    s3_link_list = []
    for file, link in url_list.items():
      s3_link_list.append(f"""<tr><td>{file}</td>""".format(file=file))
    presigned_url = '</tr>'.join(s3_link_list)
    err_summary_source_err_file_loc = """<tr></tr></br>
        <tr>
    <table style="font-family:Calibri;border:1px solid black;width: 80%;border-collapse: collapse;border-width:2px;">
    <tr><th>Source File Name</th>{presigned_url}</table>
    """.format(presigned_url=presigned_url)
    err_summary_source_err_file_loc = err_summary_source_err_file_loc.replace('<th>','<th style="border:1px solid black;border-width:2px;background-color: #1F60A9;border-color: black;color:White;" align="center">').replace('<td>','<td style ="border:1px solid black;border-width:2px;" align="left">')
      
    additional_spaces = """
        <tr><td>&nbsp;</td></tr>
        </tr>
        <tr>
        <td>&nbsp;</td>
        </tr>
        <tr>
        {x}
        </tr>
        <tr>
        <td>&nbsp;</td>
        </tr>
        <tr>
        </tr>
        <tr>
        <td>&nbsp;</td>
        </tr>""".format(x=x)
      
  footer = """\
        <tr>
        <td>**This is an automatically generated message. Please do not reply to this email**</td>
        </tr>
        <tr>
        <td>&nbsp;</td>
        </tr>
        <tr>
        <td>Thanks &amp; Regards,</td>
        </tr>
        <tr>
        <td>GDnA Team</td>
        </tr>
        </tbody>
        </table>
      </body>
      </html> """
  template = head + err_description + action_items + error_summary_country_code +err_summary_source_err_file_loc+additional_spaces+footer
  return template

# COMMAND ----------

def bind_data_into_html(cm1, delta_db_schema, country_code, file_name_up, level, er_cnt, fln_dist, to_day, sa_time, url_list,region,s3_bucket,team):
    pd.set_option('display.max_colwidth', 1000)
    try:
      x = cm1.to_html(index=False)
    except:
      x = ''
      print("No Value passed to error summary table")
    if web_dq_flag:
      ind = 'WEB UI DQ'
    else:
      ind = 'gDNA Batch DQ'
    if  level != 'structure_check':
      Action = f'Kindly make the necessary changes to the error records identified and upload again in the source_location, in order to process those records. Please intimate us at {"GDnAMDMSupport@amgen.com" if team=="internal_mdm" else "gdnaoperations@amgen.com"} once completed or in case of any clarifications.' 
    if level == 'medium':
        sub = """ Errors have been identified in the provided source files. Only records without error have been processed to the target table, while ones with discrepancies have been put on hold/not processed. Records with error entries have been attached, along with details of the error."""
        mail_sub = ']-File_Quality-{0} medium_priority-'.format(region)
        pri_msg = "["+delta_db_schema.split('_')[-1].upper()+" "+ind + \
            mail_sub+sa_time.strftime('%Y-%m-%d')
        header = 'Medium Priority*'
    elif level == 'high':
        sub = """The file {file_name_up} has not been processed as the data is not compatible with required criteria/format.""".format(file_name_up=file_name_up)
        mail_sub = ']-File_Quality-{0} high_priority-'.format(region)
        pri_msg = "["+delta_db_schema.split('_')[-1].upper() +" "+ind+ \
            mail_sub+sa_time.strftime('%Y-%m-%d')
        header = 'High Priority*'
        Action = f'Kindly make the necessary changes to the source file, and upload again in the source location. Please intimate us at {"GDnAMDMSupport@amgen.com" if team=="internal_mdm" else "gdnaoperations@amgen.com"} once completed or in case of any clarifications.'
    elif level == 'structure_check':
        sub = """Column Name mismatch found in {country_code} country notebook corresponding to the source file '{file_name_up}' .""".format(file_name_up=file_name_up,country_code=country_code)
        mail_sub = ']-File_Quality-{0}:Structure-'.format(region)
        header = 'Structure Check*'
        pri_msg = "["+delta_db_schema.split('_')[-1].upper() +" "+ind+ \
            mail_sub+sa_time.strftime('%Y-%m-%d')
    else:
        sub = """Errors have been identified in the provided source file(s). However, being low priority errors, all records have been processed and loaded into the target table."""
        mail_sub =']-File_Quality-{} low_priority-'.format(region)
        header = 'Low Priority*'
        pri_msg = "["+delta_db_schema.split('_')[-1].upper() +" "+ind+ \
            mail_sub+sa_time.strftime('%Y-%m-%d')

    x = x.replace('<td>', '<td style="border-style:solid;border-width:2px;width: 500px;border-collapse: collapse;margin-left: auto; margin-right: auto;border-color: black">').replace(
        '<tr>', '<tr style="text-align: center;border-color: white">').replace('<th>', '<th style = "background-color: #1F60A9;border-color: black;color:White;border-width:2px;">')
    if level == 'structure_check':
      message_data_quality = get_template(web_dq_flag,x, to_day, '', country_code, '', '', sub, header, '',level)
    else:
      message_data_quality = get_template(web_dq_flag,x, to_day, er_cnt, country_code, url_list, fln_dist.count(), sub, header, Action,level)
    return pri_msg, message_data_quality

# COMMAND ----------

def send_mail(ml_db, team, filename, delta_db_schema, country_code, priority_level, error_path,source_path,region,file_uploaded_by,s3_bucket):
    print("Enter into send_mail function")
    print('TEAM - ',team)
    print(error_path)
    #error_path=str(error_path)
    errorpath={i.replace('/dbfs/mnt/', 's3a://') for i in error_path}
    #errorpath=error_path.replace('/dbfs/mnt/', 's3a://')
    print(errorpath)
    catalog=spark.sql("select current_catalog()").collect()[0][0]
    volume_path=f'/Volumes/{catalog}/{delta_db_schema}/file_data_quality/'
    print('priority_level:',priority_level)
    if not os.path.exists(volume_path):
      os.mkdir(volume_path)
    if priority_level != 'structure_check':
      fln = ml_db.select('`Error Filename`')
      fln_dist = fln.distinct()
      er_cnt = int(ml_db.agg({"`Error Records`": "sum"}).collect()[0][0])
      cm1 = ml_db.toPandas()
    time_zone = get_time_zone(region)
    current_time = timezone(time_zone)
    sa_time = dqdt.now(current_time)
    sa_time.strftime('%Y-%m-%d')
    to_day = str(sa_time.strftime("%A")+", "+sa_time.strftime("%B") + " "+str(sa_time.day)+", "+str(sa_time.year))
    if priority_level == 'structure_check':
      file_name_up = filename.upper().replace('.XLSX', '')
    else:
      file_name_up = filename.upper().replace('ERR_', '', 1).replace('.XLSX', '')
    print("file_name_up:",file_name_up)
    print(f"Mail Trigger started for file {file_name_up} and {priority_level} priority")
    if(team == 'mcm_internal'):
      to = spark.sql("select distinct email_id from {delta_db_schema}.country_affiliate_configuration where upper(team)=upper('{team}') and country_code like '%{country_code}%' ".format(delta_db_schema=delta_db_schema, team=team,country_code=country_code))
    else:
      to = spark.sql("select distinct email_id from {delta_db_schema}.country_affiliate_configuration where upper(team)=upper('{team}') and upper(country_code) = upper('{country_code}') ".format(delta_db_schema=delta_db_schema, team=team,country_code=country_code))
    if to.count()>0:
      for frm in to.collect():
          to_email_id = frm.email_id
    else:
          to_email_id = ''
    url = {}    
    if priority_level != 'structure_check':
        print(errorpath)
        for err_path in errorpath:
          print('err_path :',err_path)
          err_s3_path = '/'.join(err_path.split('/')[4::])
          print('err_path :',err_path)
          print(err_s3_path)
          presigned_url = s3.generate_presigned_url(ClientMethod='get_object',Params={'Bucket': err_path.split('/')[3],'Key': err_s3_path},ExpiresIn=259200,)
          file_names = '_'.join(err_path.split('/')[-1].split('_')[1:-2])
          url[file_names] = presigned_url
        
    if web_dq_flag :
      reciever = file_uploaded_by
      mailCC = to_email_id
    else:
      reciever = to_email_id
      mailCC = to_email_id    
    if priority_level == 'structure_check':
      pri_msg, message_data_quality = bind_data_into_html('', delta_db_schema, country_code, file_name_up,priority_level,'','',to_day, sa_time, '',region,'',team)
    else:
      pri_msg, message_data_quality = bind_data_into_html(cm1, delta_db_schema, country_code, file_name_up,priority_level, er_cnt,fln_dist,to_day, sa_time, url,region,s3_bucket,team)
    msg = MIMEMultipart("related")
    partHTML = (MIMEText(message_data_quality, "html"))
    msg.attach(partHTML)
    if priority_level != 'structure_check':
      print("About to attach files",errorpath)
      for err_path in errorpath:
        dbutils.fs.cp(err_path,volume_path)
        if os.path.getsize(volume_path) <= 5e6:
            # dbutils.fs.mv(volume_path,err_path)
            # dbutils.fs.rm(volume_path)
            print(err_path)
            partFile = MIMEBase('application', "octet-stream")
            s3_resource=boto3.resource('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_key_id)
            s3_bucket=urlparse(err_path.replace('/dbfs/mnt/', 's3a://'))
            f=s3_resource.Object(s3_bucket.netloc,s3_bucket.path[1:]).get()['Body'].read()
            partFile.set_payload(f)
            encoders.encode_base64(partFile)
            partFile.add_header('Content-Disposition', 'attachment', filename=err_path.split('/')[-1])
            msg.attach(partFile)
    if priority_level == 'high':
      msg['X-Priority'] = '2'
      msg['From'] = "GDNA_File_Dq_Highpriority@amgen.com"
      global global_mail_sent_flag
      global_mail_sent_flag = True
    elif priority_level == 'structure_check':
      msg['X-Priority'] = '2'
      msg['From'] = "GDNA_data_quality@amgen.com" 
    else:
      msg['From'] = "GDNA_data_quality@amgen.com" 
    server = smtplib.SMTP('mailhost-i.amgen.com')
    msg['Subject'] = pri_msg
    msg['To'] ="tdevi@amgen.com"      # reciever
    msg['Cc'] =mailCC      # mailCC
    print("Mail sending to:", "tdevi@amgen.com",'\n',"Mail CC", mailCC)
    server.sendmail(msg['From'], msg['To'].split(";") + msg["Cc"].split(","), msg.as_string())
    server.close()
    print("Mail sent successfully")
    if not web_dq_flag and (priority_level in ('high','structure_check') or reciever=='' or mailCC == '') and team=='mcm_internal':
      if priority_level in ('high','structure_check'):
        print(f"Task is failed Due to high priority error or structure check failure")
      else:
        print(f"Failing Task because no email is configured in country_affliate_configuration table for country {country_code} and team {team}")
      
    elif not web_dq_flag and (priority_level in ('high','structure_check') or reciever=='' or mailCC == ''):
      if priority_level in ('high','structure_check'):
        print(f"Task is failed Due to high priority error or structure check failure")
      else:
        print(f"Failing Task because no email is configured in country_affliate_configuration table for country {country_code} and team {team}")
      raise

# COMMAND ----------

def save_to_target_location(df_invalid, file_name, error_path, priority,delta_db_schema,country_code,team):
    print("Enter into save_to_target_location")
    print(f"Started generating error file")
    cm1 = df_invalid.toPandas()
    # if priority == 'high':
    #     cm1['error_description_detailed'] = cm1["ERROR_DESCRIPTION"].str.split('#').str[1]
    #     cm2 = cm1.drop("NOTEBOOK_NAME", axis=1).drop("PROCESSED_DATE", axis=1).drop("FILENAME", axis=1).drop("ERROR_DESCRIPTION",axis=1).rename({"error_description_detailed": "ERROR_DESCRIPTION"}, axis=1)
    # else:
    #     cm1['error_description_detailed'] = cm1["custom_occurances"].str.split('#').str[1]
    #     cm2 = cm1.drop("NOTEBOOK_NAME", axis=1).drop("PROCESSED_DATE", axis=1).drop("FILENAME", axis=1).drop("custom_occurances", axis=1).drop("ERROR_DESCRIPTION", axis=1).rename({"error_description_detailed": "ERROR_DESCRIPTION"}, axis=1)
    
    cm1['error_description_detailed'] = cm1["ERROR_DESCRIPTION"].str.split('#').str[1]
    cm2 = cm1.drop("NOTEBOOK_NAME", axis=1).drop("PROCESSED_DATE", axis=1).drop("FILENAME", axis=1).drop("ERROR_DESCRIPTION",axis=1).rename({"error_description_detailed": "ERROR_DESCRIPTION"}, axis=1)
    
    pan_style = cm2.style.hide_index().set_properties(**{'border-color': 'black', 'border-style': 'solid'}).apply(highlight_field, axis=1)
    if priority == 'high':
        file = file_name.replace(".xlsx", '_high_priority.xlsx')
        print(f' High priority Error file name {file}')
        print('Done')
    elif priority == 'medium':
        file = file_name.replace(".xlsx", '_medium_priority.xlsx')
        print(f'Medium priority Error file name {file}')
    else:
        file = file_name.replace(".xlsx", '_low_priority.xlsx')
        print(f'Low priority Error file name {file}')
    with io.BytesIO() as output:
      with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        pan_style.to_excel(writer, sheet_name='Sheet1',startrow=1, header=False, index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        header_format = workbook.add_format({'bold': True,'text_wrap': False,'valign': 'center','fg_color': '#99ddff','border': 1})
        for col_num, value in enumerate(pan_style.columns.values):
            worksheet.write(0, col_num, value, header_format)
      data = output.getvalue()
      # s3 = boto3.resource('s3')
      error_path='s3a://'+error_path
      print(error_path)
      session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_key_id)
      s3 = session.resource('s3')
      # s3_bucket=urlparse(error_path.replace('/dbfs/mnt/', 's3a://'))
      s3_bucket=urlparse(error_path)
      s3.Bucket(s3_bucket.netloc).put_object(Key=s3_bucket.path[1:]+file, Body=data)
    print(f"File saved to location {error_path}")
    global global_err_file_path
    global_err_file_path = error_path + file
    print(global_err_file_path + " file saved.")
    print("Exited save_to_target_location ")
    return global_err_file_path

# COMMAND ----------

def save_send_invalid_data_on_priority(params=[]):
    file_path = ''
    final_df_lst = []
    err_path_list = set()
    print("Enter into save_send_invalid_data_on_priority function")
    for dic in params:
      errorTableName = dic['errorTableName']
      file_name = dic['file_name']
      country_code = dic['country_code']
      error_path = dic['error_path']
      priority_level = dic['priority_level']
      tot_cnt = dic['tot_cnt']
      delta_db_schema = dic['delta_db_schema']
      source_path = dic['source_path']
      region = dic['region']
      file_uploaded_by = dic['file_uploaded_by']
      s3_bucket = dic['s3_bucket']
      notebookname = dic['notebookname']
      
      print(f"******* Started processing for Error Table {errorTableName} *******")
      if errorTableName == 'INVALID':
        table_name = errorTableName
      else:
        table_name = delta_db_schema+"."+errorTableName
      if errorTableName =='err_stg_webinar_attendee':
        print('Entering point1')
        new_filename = re.sub('^err_', '', file_name)   # Remove the 'err_' prefix
        new_filename = re.sub('\.xlsx$', '', new_filename)  # Remove the '.xlsx' suffix
        new_filename += '.csv'  # Add a '.csv' suffix
        print(new_filename)
        if priority_level == "medium":
            cm = spark.sql("Select * from {table_name} where status is null and UPPER(PRIORITY_LEVEL) like '%MEDIUM%' and UPPER(PRIORITY_LEVEL) not like '%HIGH%' and lower(filename) = lower('{new_filename}')".format(table_name=table_name,new_filename=new_filename))
        elif priority_level == "low":
            cm = spark.sql("Select * from {table_name} where status is null and UPPER(PRIORITY_LEVEL) not like '%MEDIUM%' and UPPER(PRIORITY_LEVEL) not like '%HIGH%' and lower(filename) = lower('{new_filename}') ".format(table_name=table_name,new_filename=new_filename))
        elif priority_level == "high":
            cm = spark.sql("Select * from {table_name} where status is null and UPPER(PRIORITY_LEVEL) like '%HIGH%' and lower(filename) = lower('{new_filename}') ".format(table_name=table_name,new_filename=new_filename))
      else:
        if priority_level == "medium":
            cm = spark.sql("Select * from {table_name} where status is null and UPPER(PRIORITY_LEVEL) like '%MEDIUM%' and UPPER(PRIORITY_LEVEL) not like '%HIGH%' ".format(table_name=table_name))
        elif priority_level == "low":
            cm = spark.sql("Select * from {table_name} where status is null and UPPER(PRIORITY_LEVEL) not like '%MEDIUM%' and UPPER(PRIORITY_LEVEL) not like '%HIGH%' ".format(table_name=table_name))
        elif priority_level == "high":
            cm = spark.sql("Select * from {table_name} where status is null and UPPER(PRIORITY_LEVEL) like '%HIGH%' ".format(table_name=table_name))
      total_cnt = cm.count()
      print(f'Total number of error records found for priority level {priority_level} : {total_cnt}')
      cm1 = cm.withColumn('custom_occurances_dup', explode(split(col("ERROR_DESCRIPTION"), ";"))).drop('status').withColumnRenamed('_Status','Status')
      cm1 = cm1.withColumn('mail_group', split(cm1['custom_occurances_dup'], '-').getItem(0)).withColumn('custom_occurances', split(cm1['custom_occurances_dup'], '-',2).getItem(1))
      if total_cnt>0:
        mail_groups = cm1.select('mail_group').distinct().collect()[0][0]
        cm1_mail = cm1.drop("mail_group").drop("custom_occurances_dup").drop("ERROR_DESCRIPTION").\
          withColumnRenamed("custom_occurances", "ERROR_DESCRIPTION").\
          withColumn("priority_level", split(cm1["priority_level"], ';').getItem(0))
        if cm1_mail.count() > 0:
          print(f'trigger {mail_groups} file saving')
          file_path = save_to_target_location(cm1_mail, file_name, error_path, priority_level,delta_db_schema,country_code,mail_groups)
          err_path_list.add(file_path)
      print(f"Email Error Summary Table Generation Started")
      check_list = {'NUMERIC':{'Error_Type':'Numeric check','Error_Description':'Non numeric value found'},
                    'RANGE':{'Error_Type':'Date check','Error_Description':'Invalid date'},
                    'DUPLICATE':{'Error_Type':'Duplicate check','Error_Description':'Duplicate record Found'},
                    'INVALID%VALUE':{'Error_Type':'Valid value check','Error_Description':'Invalid value(s) found'},
                    'MANDATORY':{'Error_Type':'Null check','Error_Description':'Mandatory value is missing'}}
      dq_data_list = []
      schema = "`Country Code` string,`Error Filename` string,`Error Type` string,`Total Records` string,`Error Records` string,`Error Description` string,mail_group string"
      for check in check_list.keys():
        mail_group_df = cm1.where("upper(custom_occurances) like '%{check}%'".format(check=check)).select("mail_group").distinct().collect()
        mail_group = mail_group_df[0][0] if(len(mail_group_df) > 0) else ''
        cnt = cm1.where("upper(custom_occurances) like '%{check}%'".format(check=check)).count()
        err_type = check_list[check]['Error_Type']
        err_desc = check_list[check]['Error_Description']
        dq_data_list.append({'Country Code':country_code,'Error Filename':file_name,'Error Type':err_type,'Total Records':tot_cnt,'Error Records':str(cnt),'Error Description':err_desc,'mail_group':mail_group })
      dq_db = spark.createDataFrame(data = dq_data_list, schema = schema)
      dq_db.show()
      cm2 = cm1.where("custom_occurances like '%Custom%Check%' ").withColumn(
          'custom_occurances', split(cm1['custom_occurances'], '#').getItem(1))
      cm2.createOrReplaceTempView("tempTable")
      dq_db_custom = spark.sql("select '{0}' as `Country Code`,'{1}' as `Error Filename`,'Custom sql check' as `Error Type`,'{2}' as `Total Records`,count(1) as `Error Records`,custom_occurances as `Error Description`,mail_group from tempTable  where group by custom_occurances,mail_group".format(country_code, file_name, tot_cnt, delta_db_schema))
      dq_db1 = dq_db.union(dq_db_custom.where('`Error Records` > 0'))
      final_df_lst.append(dq_db1.collect())
      print(f"Email Error Summary Table Generation Completed ")
      if errorTableName =='err_stg_webinar_attendee':
        print('Entering point2')
        new_filename = re.sub('^err_', '', file_name)   # Remove the 'err_' prefix
        new_filename = re.sub('\.xlsx$', '', new_filename)  # Remove the '.xlsx' suffix
        new_filename += '.csv'  # Add a '.csv' suffix
        print(new_filename)
        if priority_level == "medium" and errorTableName !='INVALID':
            spark.sql("update {0}.{1} set status='processed' where status is null and UPPER(PRIORITY_LEVEL) like '%MEDIUM%'  and UPPER(PRIORITY_LEVEL) not like '%HIGH%' and lower(filename) = lower('{new_filename}') ".format(delta_db_schema, errorTableName,new_filename=new_filename))
        if priority_level == "low" and errorTableName !='INVALID':
            spark.sql("update {0}.{1} set status = 'processed' where status is null and UPPER(PRIORITY_LEVEL) not like '%MEDIUM%'  and UPPER(PRIORITY_LEVEL) not like '%HIGH%' and lower(filename) = lower('{new_filename}') ".format(delta_db_schema, errorTableName ,new_filename=new_filename))
        if priority_level == "high" and errorTableName !='INVALID':
            spark.sql("update {0}.{1} set status = 'processed' where status is null and UPPER(PRIORITY_LEVEL) like '%HIGH%' and lower(filename) = lower('{new_filename}')".format(delta_db_schema, errorTableName ,new_filename=new_filename))
        if web_dq_flag or 'data_quality_report' in notebookname.lower():
          spark.sql("update {delta_db_schema}.dq_rule_error_summary set email_status = 'sent' where filename = '{file_name}' ".format(delta_db_schema=delta_db_schema, file_name=file_name)) 
      
      else:
        if priority_level == "medium" and errorTableName !='INVALID':
            spark.sql("update {0}.{1} set status='processed' where status is null and UPPER(PRIORITY_LEVEL) like '%MEDIUM%'  and UPPER(PRIORITY_LEVEL) not like '%HIGH%' ".format(delta_db_schema, errorTableName))
        if priority_level == "low" and errorTableName !='INVALID':
            spark.sql("update {0}.{1} set status = 'processed' where status is null and UPPER(PRIORITY_LEVEL) not like '%MEDIUM%'  and UPPER(PRIORITY_LEVEL) not like '%HIGH%' ".format(delta_db_schema, errorTableName))
        if priority_level == "high" and errorTableName !='INVALID':
            spark.sql("update {0}.{1} set status = 'processed' where status is null and UPPER(PRIORITY_LEVEL) like '%HIGH%' ".format(delta_db_schema, errorTableName))
        if web_dq_flag or 'data_quality_report' in notebookname.lower():
          spark.sql("update {delta_db_schema}.dq_rule_error_summary set email_status = 'sent' where filename = '{file_name}' ".format(delta_db_schema=delta_db_schema, file_name=file_name))
      print('update for {errorTableName} error table and dq_rule_error_summary for {file_name} is completed'.format(errorTableName=errorTableName,file_name=file_name))
    final_m = spark.createDataFrame(final_df_lst[0]) 
    final_m.show()
    for index in range(len(final_df_lst)):
      if index != 0:
        final_m = final_m.union(spark.createDataFrame(final_df_lst[index])) 
    if total_cnt>0:
      print('Total_count: ' ,total_cnt)
      ml_db_send = final_m.where(f'`Error Records` > 0 and mail_group == "{mail_groups}"').drop("mail_group")
      print(ml_db_send.count())
      if ml_db_send.count() > 0:
        print(f"running mail_send for {mail_groups},{err_path_list}")
        print('Parameters:',ml_db_send,mail_groups,file_name,delta_db_schema, country_code, priority_level, err_path_list,source_path,region,file_uploaded_by,s3_bucket)
        send_mail(ml_db_send,mail_groups,file_name,delta_db_schema, country_code, priority_level, err_path_list,source_path,region,file_uploaded_by,s3_bucket)

# COMMAND ----------


def date_flexibility(df, date_cols, date_format_expected, delta_db_schema, country_code, file_name_format):
    print("Enter:  date_flexibility at " + str(time.ctime(int(time.time()))))
    print("df count: {}".format(df.count()))
    print("df columns: {}".format(df.columns))
    print("date_cols: {}".format(date_cols))
    print("date_format_expected: {}".format(date_format_expected))
    print("delta_db_schema: {}".format(delta_db_schema))
    print("country_code: {}".format(country_code))
    print("file_name_format: {}".format(file_name_format))

    # libs
    from pyspark.sql.functions import to_date, col, when, regexp_replace, date_format, to_timestamp
    import pandas as pd

    # funcs
    def apply_rule_logic(active_flag, file_name_format_tb, file_name_format):
        print("Enter:  apply_rule_logic at " + str(time.ctime(int(time.time()))))
        print("active_flag: {}".format(active_flag))
        print("file_name_format_tb: {}".format(file_name_format_tb))
        print("file_name_format: {}".format(file_name_format))
        if active_flag == "A":
            if file_name_format_tb.upper() in file_name_format.upper() or file_name_format_tb == '*':
                return True
            else:
                return False
        else:
            return False

    def timestamp_expected(date_format_local, date_format_expected):
        if 'T' in date_format_local:
            return "{}T{}".format(date_format_expected, date_format_local.split('T')[1])
        else:
            return "{} {}".format(date_format_expected, date_format_local.split(' ')[1])

    # vars
    separators = ['.', '-', '/', ',']
    found_separator = None

    # logic
    # STAGE 2
    try:
        date_format_local_list = spark.sql(
            "select local_date_format, timestamp_flag, active_flag, filename_format from {}.local_date_format where country_code = '{}'".format(
                delta_db_schema, country_code)).toPandas()
        print("date_format_local: {}".format(date_format_local_list))
    except IndexError:
        print("date format LOCAL: NOT FOUND")
        date_format_local_list = pd.DataFrame()

    for _, row in date_format_local_list.iterrows():
        date_format_local = row['local_date_format']
        timestamp_flag = row['timestamp_flag']
        active_flag = row['active_flag']
        file_name_format_tb = row['filename_format']

        print("timestamp_flag: {}".format(timestamp_flag))
        print("date_format_local: {}".format(date_format_local))
        print("active_flag: {}".format(active_flag))
        print("file_name_format_tb: {}".format(file_name_format_tb))

        if apply_rule_logic(active_flag, file_name_format_tb, file_name_format):
            print("rule is being applied")
            if timestamp_flag == "Y":
                print('Entering for timestamp format')
                for date_col in date_cols:
                    df = df.withColumn("date_parsed_expected", to_timestamp(col(date_col), date_format_expected))
                    df = df.withColumn("date_parsed_local", to_timestamp(col(date_col), date_format_local))
                    df = df.withColumn(
                        date_col,
                        when(
                            col("date_parsed_expected").isNull() & col("date_parsed_local").isNotNull(),
                            date_format(col("date_parsed_local"),
                                        timestamp_expected(date_format_local, date_format_expected))
                        ).otherwise(col(date_col))
                    )
                df = df.drop("date_parsed_expected", "date_parsed_local")
            else:
                print('Entering for date format')
                for date_col in date_cols:
                    df = df.withColumn("date_parsed_expected", to_date(col(date_col), date_format_expected))
                    df = df.withColumn("date_parsed_local", to_date(col(date_col), date_format_local))
                    df = df.withColumn(
                        date_col,
                        when(
                            col("date_parsed_expected").isNull() & col("date_parsed_local").isNotNull(),
                            date_format(col("date_parsed_local"), date_format_expected)
                        ).otherwise(col(date_col))
                    )
                df = df.drop("date_parsed_expected", "date_parsed_local")

    # STAGE 1
    for separator in separators:
        if separator in date_format_expected:
            found_separator = separator
            print("separator: {}".format(separator))
            break

    if found_separator == None:
        print("No separator in date format: {}".format(date_format_expected))
        return df

    for date_col in date_cols:
        if found_separator != "/":
            df = df.withColumn(date_col, regexp_replace(date_col, "[/]", found_separator))
        if found_separator != ",":
            df = df.withColumn(date_col, regexp_replace(date_col, "[,]", found_separator))
        if found_separator != "-":
            df = df.withColumn(date_col, regexp_replace(date_col, "[-]", found_separator))
        if found_separator != ".":
            df = df.withColumn(date_col, regexp_replace(date_col, "[.]", found_separator))

    return df


def dataqualitychecks(delta_stg_tables,delta_t1_table,file_name_format,source_path,delta_db_schema,data_source_name,country_code,file_uploaded_by,s3_bucket,region,notebookname,df,errorTableName,file,error_file_path,web_flag,delta_db_schema_tm=None,delta_db_schema_cm=None,delta_db_schema_pm=None,delta_db_schema_temp=None,mdm_catalog_ous=None):
    global web_dq_flag
    web_dq_flag = web_flag
    source_system_name = data_source_name
    try:
      delta_db_schema = delta_db_schema
    except:
      delta_db_schema=''
    try:
      delta_db_schema_tm = delta_db_schema_tm
    except:
      delta_db_schema_tm=''
    try:
      delta_db_schema_cm = delta_db_schema_cm
    except:
      delta_db_schema_cm=''
    try:
      delta_db_schema_pm = delta_db_schema_pm
    except:
      delta_db_schema_pm=''
    try:
      mdm_catalog_ous = mdm_catalog_ous
    except:
      mdm_catalog_ous=''
    try:
      delta_db_schema_temp = delta_db_schema_temp
    except:
      delta_db_schema_temp=''
    print("MDM Catalog :", mdm_catalog_ous)
    print("Enter:  dataqualitychecks at " + str(time.ctime(int(time.time()))))
    print('error_file_path: ', error_file_path)
    print("Source file",file)
    global global_err_file_path
    global_err_file_path = ''
    df_tot_count = df.count()
    high_flag = False
    print("Enter : dataqualitychecks")
    udf = UserDefinedFunction(lambda x: x[0]+': '+x[1] if ":" not in x[0] else x[0], StringType())
    spark.udf.register("udf", udf)
    dq_rules_delta_query = "select * from {0}.file_dq_rules where upper(country_code) like upper('%{3}%') and upper(source_system) = upper('{1}') and upper('{2}') like upper(file_name) and upper(active_flag)= 'A' and replace(upper('{4}'),'*','') like concat(replace(upper(config_filename_format),'*','%'),'%')".format(delta_db_schema, data_source_name, file, country_code, file_name_format)
    print(dq_rules_delta_query)
    df_dq_rules = spark.sql(dq_rules_delta_query)
    print("Fetching dq details")
    rule_cnt = df_dq_rules.count()
    print(rule_cnt, " rules to be process")
    if (rule_cnt == 0):
        print("No Dq Rules are configured")
        return df, '', False
    elif (rule_cnt > 0):
        print("Dq details found")
        pdf = df.toPandas()
        pdf['rownumber'] = pdf.index + 1
        pdf = pdf.astype(str)
        df = spark.createDataFrame(pdf)
        cols = [f.when(~f.col(x).isin("NaN", "nan", 'None', 'none'), f.col(x)).cast(
            StringType()).alias(x) for x in df.columns]
        df = df.select(*cols)
        df = df.withColumn('processing_status', lit(None)).withColumn(
            'priority_level_code', lit(None))
        for row in df_dq_rules.filter("rule_name!='structure_check'").collect():
            print(row['rule_name']+' is in progress')
            df = df.withColumn('subject_line', lit(row.subject_line)).withColumn(
                'priority_level', lit(row.priority_level)).withColumn('mail_group', lit(row.mail_group))
            if(row['rule_name'] == 'custom_check'):
                    df.createOrReplaceTempView("tempview")
                    df = spark.sql(row['sql_query'].format("tempview",delta_db_schema=delta_db_schema,delta_db_schema_tm=delta_db_schema_tm,delta_db_schema_cm=delta_db_schema_cm,delta_db_schema_pm=delta_db_schema_pm,delta_db_schema_temp=delta_db_schema_temp,mdm_catalog_ous=mdm_catalog_ous))
                    pyspark_cmd = """df.withColumn('processing_status',
                          when(col('processing_status').isNull(),concat(col('mail_group'),lit('-'), col('processing_status_2')))
                        .otherwise(when(col('processing_status').isNotNull() 
                                        & col('processing_status_2').isNotNull(),
                                        concat(col('processing_status'),lit(';'),col('mail_group'),lit('-'),col( 'processing_status_2')))
                                    .otherwise(col('processing_status')))).drop('processing_status_2').\
                                    withColumn('priority_level_code',when(col('priority_level_code').isNull(), col('priority_status_2'))
                        .otherwise(when(col('priority_level_code').isNotNull()& col('priority_status_2').isNotNull(),
                                        concat(col('priority_level_code'),lit(';'), col( 'priority_status_2')))
                                    .otherwise(col('priority_level_code')))).drop('priority_status_2')"""
            else:
                    pyspark_cmd = """df.withColumn('processing_status',
                            when({0},col('processing_status'))
                            .otherwise(when(df.processing_status.isNull(),concat(col('mail_group'),lit('-'),udf(struct(col('subject_line'),lit(rcolumn)))))
                            .otherwise(concat(df.processing_status,lit(';'),concat(col('mail_group'),lit('-'),udf(struct(col('subject_line'),lit(rcolumn))) ))))).\
                            withColumn('priority_level_code',when({0},col('priority_level_code'))
                              .otherwise(when(df.priority_level_code.isNull(),col('priority_level'))
                              .otherwise(concat(df.priority_level_code,lit(';'),col('priority_level')))))"""
            column_name = row['column_name']
            column_names = column_name.split(",")
            if(row['rule_name']).lower() == 'duplicate_check':
                    print(row['sql_query'])
                    if row['sql_query'] is None or row['sql_query'] == '':
                        columns = df.drop("processing_status").columns
                        df = df.withColumn('rownumber', row_number().over(w.Window.partitionBy(columns).orderBy(columns)))
                    else:
                        columns = row['sql_query'].split(',')
                        df = df.withColumn('rownumber', count(columns[0]).over(
                            w.Window.partitionBy(columns).orderBy(columns)))

            if (row['rule_name']).lower() == 'date_check':
                df = date_flexibility(df, column_names, row['sql_query'], delta_db_schema, country_code, file)

            for rcolumn in column_names:
                processing_status = concat(rcolumn, ' ', row['subject_line'])
                rule_name = row['rule_name'].lower()
                if rule_name != 'custom_check':
                  sql_query = spark.sql("select query from {delta_db_schema}.dq_master where rule = '{rule_name}' ".format(rule_name=rule_name,delta_db_schema=delta_db_schema)).collect()[0][0]
                  if rule_name == 'df_custom_check':
                    pyspark_cmd = pyspark_cmd.format(eval(sql_query))
                  else:
                    pyspark_cmd = pyspark_cmd.format(sql_query)
                try:
                  df = eval(pyspark_cmd)
                except Exception as e:
                  print(e)
                  print(rule_name,'-->',pyspark_cmd)

            validation_type = row['rule_name']
            rule_name = file_name_format.split("*")[0]+"_"+validation_type
            pdf_err = df.toPandas()
            pdf_invalid_err = pdf_err[pdf_err.processing_status.notnull()]
            error_desc = row['subject_line'].split("#")[1]
            err_desc = pdf_invalid_err['processing_status'].unique() 
            err_str = str(err_desc)
            if validation_type != 'custom_check':
                error_desc = error_desc.split(":")[0]
                final = [col for col in column_names if error_desc +
                            ": "+col in str(err_desc)]
                if(len(final) > 0):
                    Error_desc = error_desc+": "+",".join(final)
                    print("Error_data :", Error_desc)
                else:
                    Error_desc = ""
            elif validation_type == 'custom_check':
                Error_desc = error_desc if error_desc in str(err_desc) else ""
            error_status = "TRUE" if Error_desc == "" else "FALSE"
            spark.sql("""insert into {0}.file_delivery_validation values('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')""".format(delta_db_schema, dqdt.now(), country_code, file_name_format, file, delta_stg_tables, delta_t1_table, rule_name, validation_type, '', '', Error_desc, 'Pre_check', error_status))
            print(row['rule_name']+' Completed')
        df = df.drop('subject_line').drop('priority_level').withColumnRenamed('priority_level_code', 'priority_level')
        print("Total count:{0}".format(df.count()))
        print("converting df to pandas start",str(time.ctime(int(time.time()))))
        pdf = df.toPandas()
        print("converting df to pandas end", str(time.ctime(int(time.time()))))
        pdf_valid = pdf[pdf.processing_status.isnull()]
        print("pdf_valid output", str(time.ctime(int(time.time()))))
        if pdf_valid.empty == True:
            df_valid = spark.createDataFrame('', schema=df.schema)
        else:
            pdf_valid = pdf_valid.astype(str)
            df_valid = spark.createDataFrame(pdf_valid)
        print("if else output", str(time.ctime(int(time.time()))))
        cols = [f.when(~f.col(x).isin("NaN", "nan", 'None', 'none'), f.col(x)).cast(
            StringType()).alias(x) for x in df_valid.columns]
        df_valid = df_valid.select(*cols)
        df_valid = df_valid.drop('processing_status').drop(
            'rownumber').drop('priority_level').drop('mail_group')
        print("df_valid : ", df_valid.count())
        pdf_invalid = pdf[pdf.processing_status.notnull()]
        print("df_invalid : ", pdf_invalid.rownumber.count())
        if pdf_invalid.empty == False:
            pdf_invalid = pdf_invalid.astype(str)
            df_invalid = spark.createDataFrame(pdf_invalid)
            print(f"Columns before change {df_invalid.columns}")
            cols = [f.when(~f.col(x).isin("NaN", "nan", "None", 'none'), f.col(x)).cast(
                StringType()).alias(x.replace(' ','_')) for x in df_invalid.columns]
            df_invalid = df_invalid.select(*cols)
            print(f"Columns after change {df_invalid.columns}")
            df_invalid = df_invalid.withColumnRenamed("processing_status", "ERROR_DESCRIPTION").withColumn('FILENAME', lit(file)).\
                withColumn('RECORD_NUMBER', col("rownumber")).\
                withColumn('PROCESSED_DATE', lit(dqdt.now())).withColumn(
                    'NOTEBOOK_NAME', lit(notebookname)).drop('rownumber').drop('mail_group')
            df_invalid.createOrReplaceTempView("INVALID")
            print("errorTableName:"+errorTableName)
            if errorTableName !='NA':
              previous_date = dqdt.today() - dt.timedelta(days=3)
              print("Deleting error records processed before {previous_date} from {errorTableName} table ".format(errorTableName=errorTableName,previous_date=previous_date))
              spark.sql("delete from {delta_db_schema}.{errorTableName} where lower(filename) = lower('{file}') or to_date(processed_date)<to_date('{previous_date}') ".format(delta_db_schema=delta_db_schema, errorTableName=errorTableName,file=file,previous_date=previous_date))
              spark.sql("insert into {0}.{1} select *,null from INVALID".format(delta_db_schema, errorTableName))
              print("Error table load completed")
            else:
              print("passing view as errortable for MDM table")
              df_invalid.select(*[col(s).alias(re.sub("[^A-Za-z0-9]","_",s)) for s in df_invalid.columns]).withColumnRenamed("Status","_Status").withColumn("status",lit(None)).createOrReplaceTempView("INVALID")
              errorTableName = 'INVALID'
            source_file = file
            file = "err_"+file.lower().split(".")[0]+".xlsx"
            print(f"Source File is --> {source_file} ")
            print(file+" is going to write in s3")
            print('Priority task started')
            priority_var = ''
            priority_var_medium = ''
            priority_var_high = ''
            for pir in df_invalid.collect():
                Priority_List = [val.lower() for val in pir.priority_level.split(";")]
                if 'high' in Priority_List:
                    print("High priority task found -- alter mail tiggered to file uploader")
                    priority_var_high = 'HIGH'
                if 'medium' in Priority_List:
                    priority_var_medium = 'MEDIUM'
                    print("Medium priority task found -- Routing the Error record to error table ")
                if 'low' in Priority_List:
                    priority_var = 'LOW'
                    print("Low priority task found -- Error is routing to both error table and transaction table")
            print('priority_var_high: ', priority_var_high)
            save_send_func_conf = [{'errorTableName':errorTableName,
                                    'file_name':file,
                                    'country_code':country_code,
                                    'error_path':error_file_path,
                                    'priority_level':'',
                                    'tot_cnt':df.count(),
                                    'delta_db_schema':delta_db_schema,
                                    'source_path':source_path,
                                    'region':region,
                                    'file_uploaded_by':file_uploaded_by,
                                    's3_bucket':s3_bucket,
                                    'notebookname':notebookname}]
#             Delelting records for same file in dq_rule_Error_summary, if any
            spark.sql(f"delete from {delta_db_schema}.dq_rule_error_summary where filename = '{file}' ")
            if priority_var_high == 'HIGH':
                # handled high priority for dq_rule_error_summary entry and high priority for error file saving
                df_invalid_1 = df_invalid.withColumn("data_source_name", lit(source_system_name)).\
                    withColumn("filename", lit(file)).withColumn(
                        "country_code", lit(country_code))
                print("Summary insert start")
                df_error_summary = df_invalid_1.\
                    where("upper(priority_level) not like '%HIGH%'").\
                    groupBy("country_code", "data_source_name", "filename",
                            "error_description", "processed_date", "notebook_name").count()
                df_error_summary = df_error_summary.\
                    withColumn("error_table_name", lit(errorTableName)).withColumn("error_file", lit(error_file_path)).\
                    withColumn("email_status", lit(None)).withColumn("total_record", lit(df_tot_count)).\
                    withColumn("source_path", lit(source_path))
                df_error_summary.createOrReplaceTempView('df_error_summary')
                print("df_error_summary")
                df_error_summary.show()
                error_summary_insert = "insert into table {0}.dq_rule_error_summary select * from df_error_summary".format(
                    delta_db_schema)
                spark.sql(error_summary_insert)
                print("inserted into dq_rule_error_summary ")
                print("high priority - mail process start")

                high_flag = True
                save_send_func_conf[0]['priority_level'] = 'high'
                save_send_invalid_data_on_priority(save_send_func_conf)
                print("high priority - mail process completed")

            if priority_var == 'LOW':
                df_valid = df_valid.unionAll(df_invalid.where("upper(priority_level) not like '%MEDIUM%' and upper(priority_level) not like '%HIGH%'").drop('error_description').drop('filename').drop('priority_level').drop('record_number').drop('processed_date').drop('notebook_name'))
            if priority_var_medium == 'MEDIUM' or priority_var == 'LOW':
                print("******",priority_var_medium,priority_var,web_dq_flag,"*********")
                df_invalid = df_invalid.withColumn("data_source_name", lit(source_system_name)).\
                    withColumn("filename", lit(file)).withColumn("country_code", lit(country_code))
                if web_dq_flag:
                  print("Medium priority - save and send mail process started")
                  save_send_func_conf[0]['priority_level'] = 'medium'
                  save_send_invalid_data_on_priority(save_send_func_conf)
                  print("Medium priority - save and send mail process completed")                  
                  print("Low priority - save and send mail process started")
                  save_send_func_conf[0]['priority_level'] = 'low'
                  save_send_invalid_data_on_priority(save_send_func_conf)
                  print("Low priority - save and send mail process completed")
                print("Summary insert start")
                
                df_error_summary = df_invalid.\
                    groupBy("country_code", "data_source_name", "filename",
                            "error_description", "processed_date", "notebook_name").count()
                df_error_summary = df_error_summary.\
                    withColumn("error_table_name", lit(errorTableName)).withColumn("error_file", lit(error_file_path)).\
                    withColumn("email_status", lit(None)).withColumn("total_record", lit(df_tot_count)).\
                    withColumn("source_path", lit(source_path))
                df_error_summary.createOrReplaceTempView('df_error_summary')
                error_summary_insert = "insert into table {0}.dq_rule_error_summary select * from df_error_summary".format(
                    delta_db_schema)
                spark.sql(error_summary_insert)
                print("inserted into dq_rule_error_summary ")
        else:
            print("No error record found")
        print("final_df_valid:", df_valid.count())
        return df_valid, global_err_file_path, high_flag


# COMMAND ----------


def dataqualitychecks_ttm(delta_stg_tables,delta_t1_table,file_name_format,source_path,delta_db_schema,data_source_name,country_code,file_uploaded_by,s3_bucket,region,notebookname,df,errorTableName,file,error_file_path,web_flag,delta_db_schema_tm=None,delta_db_schema_cm=None,delta_db_schema_pm=None,delta_db_schema_temp=None,mdm_catalog_ous=None):
    global web_dq_flag
    web_dq_flag = web_flag
    source_system_name = data_source_name
    try:
      delta_db_schema = delta_db_schema
    except:
      delta_db_schema=''
    try:
      delta_db_schema_tm = delta_db_schema_tm
    except:
      delta_db_schema_tm=''
    try:
      delta_db_schema_cm = delta_db_schema_cm
    except:
      delta_db_schema_cm=''
    try:
      delta_db_schema_pm = delta_db_schema_pm
    except:
      delta_db_schema_pm=''
    try:
      mdm_catalog_ous = mdm_catalog_ous
    except:
      mdm_catalog_ous=''
    try:
      delta_db_schema_temp = delta_db_schema_temp
    except:
      delta_db_schema_temp=''
    print("MDM Catalog :", mdm_catalog_ous)
    print("Enter:  dataqualitychecks at " + str(time.ctime(int(time.time()))))
    print('error_file_path: ', error_file_path)
    print("Source file",file)
    global global_err_file_path
    global_err_file_path = ''
    df_tot_count = df.count()
    print("dfcount: ",df_tot_count)
    high_flag = False
    print("Enter : dataqualitychecks")
    udf = UserDefinedFunction(lambda x: x[0]+': '+x[1] if ":" not in x[0] else x[0], StringType())
    spark.udf.register("udf", udf)
    dq_rules_delta_query = "select * from {0}.file_dq_rules where upper(country_code) like upper('%{3}%') and upper(source_system) = upper('{1}') and upper('{2}') like upper(file_name) and upper(active_flag)= 'A' and replace(upper('{4}'),'*','') like concat(replace(upper(config_filename_format),'*','%'),'%')".format(delta_db_schema, data_source_name, file, country_code, file_name_format)
    print(dq_rules_delta_query)
    df_dq_rules = spark.sql(dq_rules_delta_query)
    print("Fetching dq details")
    rule_cnt = df_dq_rules.count()
    print(rule_cnt, " rules to be process")
    if (rule_cnt == 0):
        print("No Dq Rules are configured")
        return df, '', False
    elif (rule_cnt > 0):
        print("Dq details found")
        #pdf = df.toPandas()
        #pdf['rownumber'] = pdf.index + 1
        #pdf = pdf.astype(str)
        #df = spark.createDataFrame(pdf)
        df = df.withColumn('rownumber', f.monotonically_increasing_id().cast(StringType()))
        cols = [f.when(~f.col(x).isin("NaN", "nan", 'None', 'none'), f.col(x)).cast(
            StringType()).alias(x) for x in df.columns]
        df = df.select(*cols)
        df = df.withColumn('processing_status', lit(None)).withColumn(
            'priority_level_code', lit(None))
        df = df.repartition(200)
        print("Entering into the dq rules for loop at " + str(time.ctime(int(time.time()))))
        for row in df_dq_rules.filter("rule_name!='structure_check'").collect():
            print(row['rule_name']+' is in progress '+str(time.ctime(int(time.time()))))
            df = df.withColumn('subject_line', lit(row.subject_line)).withColumn(
                'priority_level', lit(row.priority_level)).withColumn('mail_group', lit(row.mail_group))
            if(row['rule_name'] == 'custom_check'):
              if delta_stg_tables.lower() == 'customer_daily_sales_vw':
                print(row['column_name'])
                desc_df = spark.sql("describe history {0}.{1}".format(delta_db_schema,delta_stg_tables))
                desc_df_filter = desc_df.filter(col('operation').ilike("WRITE%"))
                desc_df_with_date = desc_df_filter.withColumn('timestamp', date_format(col('timestamp'), 'yyyy-MM-dd')).filter(col('timestamp') != current_date().cast("string"))
                last_date = desc_df_with_date.agg(max("timestamp").alias("last_date")).collect()[0]["last_date"]
                result_df = desc_df_with_date.filter(col('timestamp') == last_date)
                # version_result = result_df.select(col('version')).collect()[0]["version"]
                version_result = 829
                print("version_result: ",version_result)

                df.createOrReplaceTempView("tempview")
                df = spark.sql(row['sql_query'].format("tempview", delta_db_schema, delta_stg_tables, version_result))
                print("custom_sql_query: ", df)
                print("custom sql_query completed at "+str(time.ctime(int(time.time()))))
                pyspark_cmd = """df.withColumn('processing_status',
                      when(col('processing_status').isNull(),concat(col('mail_group'),lit('-'), col('processing_status_2')))
                    .otherwise(when(col('processing_status').isNotNull() 
                                    & col('processing_status_2').isNotNull(),
                                    concat(col('processing_status'),lit(';'),col('mail_group'),lit('-'),col('processing_status_2')))
                                .otherwise(col('processing_status')))).drop('processing_status_2').\
                                withColumn('priority_level_code',when(col('priority_level_code').isNull(), col('priority_status_2'))
                    .otherwise(when(col('priority_level_code').isNotNull()& col('priority_status_2').isNotNull(),
                                    concat(col('priority_level_code'),lit(';'), col( 'priority_status_2')))
                                .otherwise(col('priority_level_code')))).drop('priority_status_2')"""
              else:      
                df.createOrReplaceTempView("tempview")
                df = spark.sql(row['sql_query'].format("tempview",delta_db_schema=delta_db_schema,delta_db_schema_tm=delta_db_schema_tm,delta_db_schema_cm=delta_db_schema_cm,delta_db_schema_pm=delta_db_schema_pm,delta_db_schema_temp=delta_db_schema_temp,mdm_catalog_ous=mdm_catalog_ous))
                pyspark_cmd = """df.withColumn('processing_status',
                      when(col('processing_status').isNull(),concat(col('mail_group'),lit('-'), col('processing_status_2')))
                    .otherwise(when(col('processing_status').isNotNull() 
                                    & col('processing_status_2').isNotNull(),
                                    concat(col('processing_status'),lit(';'),col('mail_group'),lit('-'),col('processing_status_2')))
                                .otherwise(col('processing_status')))).drop('processing_status_2').\
                                withColumn('priority_level_code',when(col('priority_level_code').isNull(), col('priority_status_2'))
                    .otherwise(when(col('priority_level_code').isNotNull()& col('priority_status_2').isNotNull(),
                                    concat(col('priority_level_code'),lit(';'), col( 'priority_status_2')))
                                .otherwise(col('priority_level_code')))).drop('priority_status_2')"""
            else:
                    pyspark_cmd = """df.withColumn('processing_status',
                            when({0},col('processing_status'))
                            .otherwise(when(df.processing_status.isNull(),concat(col('mail_group'),lit('-'),udf(struct(col('subject_line'),lit(rcolumn)))))
                            .otherwise(concat(df.processing_status,lit(';'),concat(col('mail_group'),lit('-'),udf(struct(col('subject_line'),lit(rcolumn))) ))))).\
                            withColumn('priority_level_code',when({0},col('priority_level_code'))
                              .otherwise(when(df.priority_level_code.isNull(),col('priority_level'))
                              .otherwise(concat(df.priority_level_code,lit(';'),col('priority_level')))))"""
            column_name = row['column_name']
            column_names = column_name.split(",")
            if(row['rule_name']).lower() == 'duplicate_check':
                print(row['sql_query'])
                if row['sql_query'] is None or row['sql_query'] == '':
                    columns = df.drop("processing_status").columns
                    df = df.withColumn('rownumber', row_number().over(w.Window.partitionBy(columns).orderBy(columns)))
                else:
                    columns = row['sql_query'].split(',')
                    df = df.withColumn('rownumber', count(columns[0]).over(
                        w.Window.partitionBy(columns).orderBy(columns)))

            if (row['rule_name']).lower() == 'date_check':
                df = date_flexibility(df, column_names, row['sql_query'], delta_db_schema, country_code, file)

            print("Entering into the rcolumn names for loop at " + str(time.ctime(int(time.time()))))
            for rcolumn in column_names:
                processing_status = concat(rcolumn, ' ', row['subject_line'])
                rule_name = row['rule_name'].lower()
                if rule_name != 'custom_check':
                  sql_query = spark.sql("select query from {delta_db_schema}.dq_master where rule = '{rule_name}' ".format(rule_name=rule_name,delta_db_schema=delta_db_schema)).collect()[0][0]
                  if rule_name == 'df_custom_check':
                    pyspark_cmd = pyspark_cmd.format(eval(sql_query))
                  else:
                    pyspark_cmd = pyspark_cmd.format(sql_query)
                try:
                  df = eval(pyspark_cmd)
                except Exception as e:
                  print(e)
                  print(rule_name,'-->',pyspark_cmd)
                print("Exiting from the rcolumn"+ rcolumn + "names for loop at " + str(time.ctime(int(time.time()))))

            validation_type = row['rule_name']
            rule_name = file_name_format.split("*")[0]+"_"+validation_type
            #pdf_err = df.toPandas()
            #pdf_invalid_err = pdf_err[pdf_err.processing_status.notnull()]
            error_desc = row['subject_line'].split("#")[1]
            #err_desc = pdf_invalid_err['processing_status'].unique() 
            pdf_invalid_err = df.filter(col('processing_status').isNotNull())
            err_desc = pdf_invalid_err.select(col('processing_status')).distinct()
            err_str = str(err_desc)
            if validation_type != 'custom_check':
                error_desc = error_desc.split(":")[0]
                final = [col for col in column_names if error_desc +
                            ": "+col in str(err_desc)]
                if(len(final) > 0):
                    Error_desc = error_desc+": "+",".join(final)
                    print("Error_data :", Error_desc)
                else:
                    Error_desc = ""
            elif validation_type == 'custom_check':
                Error_desc = error_desc if error_desc in str(err_desc) else ""
            error_status = "TRUE" if Error_desc == "" else "FALSE"
            spark.sql("""insert into {0}.file_delivery_validation values('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')""".format(delta_db_schema, dqdt.now(), country_code, file_name_format, file, delta_stg_tables, delta_t1_table, rule_name, validation_type, '', '', Error_desc, 'Pre_check', error_status))
            print(row['rule_name']+' Completed ' + str(time.ctime(int(time.time()))))
            print("dq rules for loop completed at " + str(time.ctime(int(time.time()))))
        df = df.drop('subject_line').drop('priority_level').withColumnRenamed('priority_level_code', 'priority_level')
        df.cache()
        print("Total count:{0}".format(df.count()))
        print("df count at", str(time.ctime(int(time.time()))))
        #print("converting df to pandas start",str(time.ctime(int(time.time()))))
        #pdf = df.toPandas()
        #print("converting df to pandas end", str(time.ctime(int(time.time()))))
        #pdf_valid = pdf[pdf.processing_status.isnull()]
        pdf_valid = df.filter(col('processing_status').isNull())
        print("pdf_valid output", str(time.ctime(int(time.time()))))
        pdf_valid.cache()
        if pdf_valid.count() == 0:
            df_valid = spark.createDataFrame([], schema=df.schema)
        else:
            #pdf_valid = pdf_valid.astype(str)
            #df_valid = spark.createDataFrame(pdf_valid)
            df_valid = pdf_valid
        print("if else output", str(time.ctime(int(time.time()))))
        cols = [f.when(~f.col(x).isin("NaN", "nan", 'None', 'none'), f.col(x)).cast(
            StringType()).alias(x) for x in df_valid.columns]
        df_valid = df_valid.select(*cols)
        df_valid = df_valid.drop('processing_status').drop(
            'rownumber').drop('priority_level').drop('mail_group')
        df_valid.cache()
        print("df_valid : ", df_valid.count())
        #pdf_invalid = pdf[pdf.processing_status.notnull()]
        pdf_invalid = df.filter(col('processing_status').isNotNull())
        pdf_invalid.cache()
        print("df_invalid : ", pdf_invalid.count())
        if pdf_invalid.count() != 0:
            #pdf_invalid = pdf_invalid.astype(str)
            #df_invalid = spark.createDataFrame(pdf_invalid)
            df_invalid = pdf_invalid
            print(f"Columns before change {df_invalid.columns}" + str(time.ctime(int(time.time()))))
            cols = [f.when(~f.col(x).isin("NaN", "nan", "None", 'none'), f.col(x)).cast(
                StringType()).alias(x.replace(' ','_')) for x in df_invalid.columns]
            df_invalid = df_invalid.select(*cols)
            print(f"Columns after change {df_invalid.columns}" + str(time.ctime(int(time.time()))))
            df_invalid = df_invalid.withColumnRenamed("processing_status", "ERROR_DESCRIPTION").withColumn('FILENAME', lit(file)).\
                withColumn('RECORD_NUMBER', col("rownumber")).\
                withColumn('PROCESSED_DATE', lit(dqdt.now())).withColumn(
                    'NOTEBOOK_NAME', lit(notebookname)).drop('rownumber').drop('mail_group')
            print("df_invalid: ", df_invalid)
            df_invalid.createOrReplaceTempView("INVALID")
            print("errorTableName:"+errorTableName)
            print(str(time.ctime(int(time.time()))))
            if errorTableName !='NA':
              previous_date = dqdt.today() - dt.timedelta(days=3)
              print("previous_date:"+str(previous_date))
              print("Deleting error records processed before {previous_date} from {errorTableName} table "+ str(time.ctime(int(time.time()))).format(errorTableName=errorTableName,previous_date=previous_date))
              spark.sql("delete from {delta_db_schema}.{errorTableName} where lower(filename) = lower('{file}') or to_date(processed_date)<to_date('{previous_date}') ".format(delta_db_schema=delta_db_schema, errorTableName=errorTableName,file=file,previous_date=previous_date))
              errtable_count = spark.sql("select count(*) from {delta_db_schema}.{errorTableName} where lower(filename) = lower('{file}') or to_date(processed_date)<to_date('{previous_date}') ".format(delta_db_schema=delta_db_schema, errorTableName=errorTableName,file=file,previous_date=previous_date))
              print("Error table count before deletion:"+str(errtable_count.collect()[0][0]))
              print("Error table deletion completed" + str(time.ctime(int(time.time()))))
              print("Inserting invalid records to the error table "+str(time.ctime(int(time.time()))))
              spark.sql("insert into {0}.{1} select *,null from INVALID".format(delta_db_schema, errorTableName))
              print("Error table load completed "+str(time.ctime(int(time.time()))))
            else:
              print("passing view as errortable for MDM table")
              df_invalid.select(*[col(s).alias(re.sub("[^A-Za-z0-9]","_",s)) for s in df_invalid.columns]).withColumnRenamed("Status","_Status").withColumn("status",lit(None)).createOrReplaceTempView("INVALID")
              errorTableName = 'INVALID'
            source_file = file
            file = "err_"+file.lower().split(".")[0]+".xlsx"
            print(f"Source File is --> {source_file} ")
            print(file+" is going to write in s3")
            print('Priority task started '+str(time.ctime(int(time.time()))))
            priority_var = ''
            priority_var_medium = ''
            priority_var_high = ''
            for pir in df_invalid.collect():
                Priority_List = [val.lower() for val in pir.priority_level.split(";")]
                if 'high' in Priority_List:
                    print("High priority task found -- alter mail tiggered to file uploader" + str(time.ctime(int(time.time()))))
                    priority_var_high = 'HIGH'
                if 'medium' in Priority_List:
                    priority_var_medium = 'MEDIUM'
                    print("Medium priority task found -- Routing the Error record to error table "+str(time.ctime(int(time.time()))))
                if 'low' in Priority_List:
                    priority_var = 'LOW'
                    print("Low priority task found -- Error is routing to both error table and transaction table"+str(time.ctime(int(time.time()))))
            print('priority_var_high: ', priority_var_high)
            print(str(time.ctime(int(time.time()))))
            save_send_func_conf = [{'errorTableName':errorTableName,
                                    'file_name':file,
                                    'country_code':country_code,
                                    'error_path':error_file_path,
                                    'priority_level':'',
                                    'tot_cnt':df.count(),
                                    'delta_db_schema':delta_db_schema,
                                    'source_path':source_path,
                                    'region':region,
                                    'file_uploaded_by':file_uploaded_by,
                                    's3_bucket':s3_bucket,
                                    'notebookname':notebookname}]
            # Delelting records for same file in dq_rule_Error_summary, if any
            print("Deleting records for same file in dq_rule_error_summary table "+str(time.ctime(int(time.time()))))
            spark.sql(f"delete from {delta_db_schema}.dq_rule_error_summary where filename = '{file}' ")
            print("Deleting records for same file in dq_rule_error_summary table completed "+str(time.ctime(int(time.time()))))
            if priority_var_high == 'HIGH':
                # handled high priority for dq_rule_error_summary entry and high priority for error file saving
                df_invalid_1 = df_invalid.withColumn("data_source_name", lit(source_system_name)).\
                    withColumn("filename", lit(file)).withColumn(
                        "country_code", lit(country_code))
                print("Summary insert start "+str(time.ctime(int(time.time()))))
                df_error_summary = df_invalid_1.\
                    where("upper(priority_level) not like '%HIGH%'").\
                    groupBy("country_code", "data_source_name", "filename",
                            "error_description", "processed_date", "notebook_name").count()
                print(str(time.ctime(int(time.time()))))
                df_error_summary = df_error_summary.\
                    withColumn("error_table_name", lit(errorTableName)).withColumn("error_file", lit(error_file_path)).\
                    withColumn("email_status", lit(None)).withColumn("total_record", lit(df_tot_count)).\
                    withColumn("source_path", lit(source_path))
                df_error_summary.createOrReplaceTempView('df_error_summary')
                print("df_error_summary "+str(time.ctime(int(time.time()))))
                df_error_summary.show()
                error_summary_insert = "insert into table {0}.dq_rule_error_summary select * from df_error_summary".format(
                    delta_db_schema)
                spark.sql(error_summary_insert)
                print("inserted into dq_rule_error_summary ")
                print("high priority - mail process start "+str(time.ctime(int(time.time()))))

                high_flag = True
                save_send_func_conf[0]['priority_level'] = 'high'
                save_send_invalid_data_on_priority(save_send_func_conf)
                print("high priority - mail process completed "+str(time.ctime(int(time.time()))))

            if priority_var == 'LOW':
                df_valid = df_valid.unionAll(df_invalid.where("upper(priority_level) not like '%MEDIUM%' and upper(priority_level) not like '%HIGH%'").drop('error_description').drop('filename').drop('priority_level').drop('record_number').drop('processed_date').drop('notebook_name'))
            if priority_var_medium == 'MEDIUM' or priority_var == 'LOW':
                print("******",priority_var_medium,priority_var,web_dq_flag,"*********")
                df_invalid = df_invalid.withColumn("data_source_name", lit(source_system_name)).\
                    withColumn("filename", lit(file)).withColumn("country_code", lit(country_code))
                if web_dq_flag:
                  print("Medium priority - save and send mail process started")
                  save_send_func_conf[0]['priority_level'] = 'medium'
                  save_send_invalid_data_on_priority(save_send_func_conf)
                  print("Medium priority - save and send mail process completed")                  
                  print("Low priority - save and send mail process started")
                  save_send_func_conf[0]['priority_level'] = 'low'
                  save_send_invalid_data_on_priority(save_send_func_conf)
                  print("Low priority - save and send mail process completed")
                print("Summary insert start "+str(time.ctime(int(time.time()))))
                
                df_error_summary = df_invalid.\
                    groupBy("country_code", "data_source_name", "filename",
                            "error_description", "processed_date", "notebook_name").count()
                print(str(time.ctime(int(time.time()))))
                df_error_summary = df_error_summary.\
                    withColumn("error_table_name", lit(errorTableName)).withColumn("error_file", lit(error_file_path)).\
                    withColumn("email_status", lit(None)).withColumn("total_record", lit(df_tot_count)).\
                    withColumn("source_path", lit(source_path))
                df_error_summary.createOrReplaceTempView('df_error_summary')
                error_summary_insert = "insert into table {0}.dq_rule_error_summary select * from df_error_summary".format(
                    delta_db_schema)
                spark.sql(error_summary_insert)
                print("inserted into dq_rule_error_summary ")
        else:
            print("No error record found")
        print("final_df_valid:", df_valid.count())
        print(str(time.ctime(int(time.time()))))
        return df_valid, global_err_file_path, high_flag
