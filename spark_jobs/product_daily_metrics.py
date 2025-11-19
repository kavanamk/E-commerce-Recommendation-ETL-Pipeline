#!/usr/bin/env python
# coding: utf-8

# ## Initialize SparkSession

# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ProductDailyMetrics") \
    .getOrCreate()


# ### Load Silver clean data

# In[6]:


silver_path = "/Users/kavanamanvi/Desktop/AmazonReviews/airflow/processed/silver/reviews_clean"
df = spark.read.parquet(silver_path)

df.show(5)
df.printSchema()


# ### Add a date column

# In[9]:


from pyspark.sql.functions import to_date

df_with_date = df.withColumn("date", to_date("event_time"))
df_with_date.show(5)


# ## Prepare Aggregations
# 
# We need metrics per product per day.
# 
# ### Metrics:
# 
# avg rating
# 
# review count
# 
# verified count (if present)
# 
# helpful vote sum (if present)
# 
# rating distribution
# 
# min/max
# 
# earliest/latest review date

# In[12]:


# Import functions needed for aggregation
from pyspark.sql.functions import (
    avg, count, sum as spark_sum, min, max,
    countDistinct, expr, first, last
)


# In[22]:


#Group by asin, date and compute aggregates


# In[17]:


from pyspark.sql import functions as F

daily_metrics = df_with_date.groupBy("asin", "date").agg(
    F.avg("rating").alias("avg_rating"),
    F.count("*").alias("review_count"),
    F.min("rating").alias("min_rating"),
    F.max("rating").alias("max_rating"),

    F.sum((F.col("rating") == 1).cast("int")).alias("rating_1_count"),
    F.sum((F.col("rating") == 2).cast("int")).alias("rating_2_count"),
    F.sum((F.col("rating") == 3).cast("int")).alias("rating_3_count"),
    F.sum((F.col("rating") == 4).cast("int")).alias("rating_4_count"),
    F.sum((F.col("rating") == 5).cast("int")).alias("rating_5_count"),

    F.min("event_time").alias("first_review_date"),
    F.max("event_time").alias("last_review_date"),
)


# ## Write the gold dataset partitioned by date

# In[21]:


daily_metrics.printSchema()
daily_metrics.show(10)


# In[ ]:


output_path = "/Users/kavanamanvi/Desktop/AmazonReviews/airflow/processed/gold/product_daily_metrics"

daily_metrics.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(output_path)


# In[ ]:




