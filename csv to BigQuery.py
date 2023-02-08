#!/usr/bin/env python
# coding: utf-8

# In[3]:


from google.cloud import bigquery
from google.oauth2 import service_account
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "/Users/souvikpatra/Downloads/my-project-youtube-vs-facebook-e5449a9aac58.json"
credentials = service_account.Credentials.from_service_account_file('/Users/souvikpatra/Downloads/my-project-youtube-vs-facebook-e5449a9aac58.json')
project_id = 'my-project-youtube-vs-facebook'
client = bigquery.Client(credentials= credentials,project=project_id)

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "my-project-youtube-vs-facebook.mine.required_table1"

job_config = bigquery.LoadJobConfig(
    schema=[
       bigquery.SchemaField("MovieID", "INTEGER"),
       bigquery.SchemaField("BoxOffice", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://my-project-youtube-vs-facebook.appspot.com/TemporaryFile/BoxOffice - Sheet1.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))


# In[ ]:




