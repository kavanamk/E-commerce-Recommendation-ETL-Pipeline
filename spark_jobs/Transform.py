#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transform").getOrCreate()


# In[37]:


reviews_df = spark.read.parquet("/Users/kavanamanvi/Desktop/AmazonReviews/airflow/processed/bronze/reviews_raw")


# In[7]:


reviews_df.show(5)


# In[9]:


reviews_df.printSchema()


# ## 2B) Clean Data

# In[12]:


#selecting only the columns I will need for Recc algo
clean_df = reviews_df.select("user_id", "asin", "rating", "timestamp")


# In[14]:


clean_df.show(5)
clean_df.printSchema()


# ### Step 2B.2)Remove rows with missing key values
# 
# Why?
# 
# Because:
# 
# user_id cannot be null
# 
# asin cannot be null
# 
# rating cannot be null

# In[17]:


clean_df = clean_df.dropna(subset=["user_id", "asin", "rating"])


# In[19]:


clean_df.show(5)


# ### Step 2B.3) Convert timestamp into real date

# In[23]:


from pyspark.sql.functions import from_unixtime, to_timestamp

clean_df = clean_df.withColumn(
    "event_time",
    to_timestamp(from_unixtime("timestamp"))
)


# In[25]:


clean_df.show(5)


# ### Step 2B.4) Inspect the cleaned dataset

# In[30]:


clean_df.printSchema()
clean_df.show(10)
clean_df.count()


# ### Step 2B.5) Save CLEANED data to Parquet

# In[35]:


clean_df.write.mode("overwrite").parquet("/Users/kavanamanvi/Desktop/AmazonReviews/airflow/processed/silver/reviews_clean")


# In[ ]:




