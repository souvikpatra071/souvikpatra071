#!/usr/bin/env python
# coding: utf-8

# In[61]:


from pyspark.sql.session import SparkSession


# In[62]:


spark = SparkSession.builder.appName("Python spark assesment").config("spark.driver.memory", "40g").getOrCreate()


# In[63]:


spark


# In[64]:


df = spark.read.json("/Users/souvikpatra/Downloads/user.json")
df1 =spark.read.json("/Users/souvikpatra/Downloads/review.json")
df2=spark.read.json("/Users/souvikpatra/Downloads/yelp_academic_dataset_business 2.json")


# In[65]:


df.show()
df.printSchema()


# In[60]:


display(df)


# In[66]:


df1.show()
df1.printSchema()


# In[87]:


df2=df2.withColumn("categories",explode(split("categories",",")))
df2=df2.withColumn("attributes",col("attributes").cast("string"))
df2=df2.withColumn("hours",col("hours").cast("string"))
df2.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("business")
df2.write.option("header","true").csv("business4.csv")
df2.show()


# In[88]:


df2.show()
df2.printSchema()


# In[79]:



df1.write.option("header","true").csv("review5.csv")
df1.show()


# In[78]:


partialSchema = StructType([StructField(" date", StringType(), nullable=True)])      


# In[48]:


from pyspark.sql.functions import explode,split


# In[54]:


from pyspark.sql.functions import *


# In[58]:


from pyspark.sql.functions import col,count
df2.withColumn("categories",explode(split("categories",',')))
df2.groupBy("categories").agg(count("categories")).show()


# In[144]:


df2.createOrReplaceTempView("df2")


# In[145]:


spark.sql("select  business_id,name,review_count,stars from df2 where review_count>=10 order By stars desc,review_count desc limit 5").show()


# In[126]:


spark.sql("select business_id,name,review_count,stars,categories from df2 where categories='Home Services' order by review_count desc limit 5").show()


# In[ ]:





# In[139]:


spark.sql("select city,max(stars) from df2 where categories LIKE 'Nightlife%' group by city order by 2 desc limit 1").show()


# In[140]:


df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")


# In[143]:


spark.sql("select df.user_id, df2.name, df2.categories ,df2.review_count from df inner join df1 on df.user_id=df1.user_id inner join df2 on df1.business_id=df2.business_id").show() 


# In[ ]:




