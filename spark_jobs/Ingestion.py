#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark


# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AmazonReviews").getOrCreate()
spark


# In[3]:


reviews_df = spark.read.json("/Users/kavanamanvi/Desktop/AmazonReviews/airflow/Data/reviews.json")


# In[4]:


reviews_df.show(5)


# In[5]:


reviews_df.printSchema()


# In[6]:


reviews_df.write.mode("overwrite").parquet("/Users/kavanamanvi/Desktop/AmazonReviews/airflow/processed/bronze/reviews_raw")


# In[ ]:




