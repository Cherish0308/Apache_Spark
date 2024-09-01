#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/fakefriends-header.csv")

# Select only age and numFriends columns
friendsByAge = lines.select("age", "friends")

# From friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# Sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# Formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()

spark.stop()


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func


# In[6]:


spark=SparkSession.builder.appName('FriendsByAge').getOrCreate()
lines=spark.read.option('header','true').option('inferSchema','true').csv('fakefriends-header.csv')


# In[8]:


friendsByAge = lines.select("age", "friends")
friendsByAge


# In[13]:


friendsByAge.groupby('age').avg('friends').show()


# In[14]:


friendsByAge.groupBy('age').avg('friends').sort('age').show()


# In[18]:


#friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

friendsByAge.groupBy('age') \
    .agg(func.round(func.avg('friends'), 2).alias('avg_friends')) \
    .sort('age') \
    .show()


# In[ ]:




