from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
'/Users/souvikpatra/Downloads/my-project-youtube-vs-facebook-6e4ecd604171.json')

project_id = 'my-project-youtube-vs-facebook'
client = bigquery.Client(credentials= credentials,project=project_id)


query_job = client.query("""SELECT * FROM `my-project-youtube-vs-facebook.mine.london_special` LIMIT 1000 """).to_dataframe()
print(query_job)

query_job1=client.query("""SELECT  *, dense_rank()over(partition by extract(month from timestamp),extract(year from timestamp) order by extract(date from timestamp)) as dense_rank FROM `my-project-youtube-vs-facebook.mine.london_special`""").to_dataframe()
print(query_job1)

user=int(input())
filename=input()
partition_dataframe=query_job1.loc[query_job1['dense_rank']==user]
partition_dataframe.to_csv('/Users/souvikpatra/Downloads/{filename}.csv')
  
