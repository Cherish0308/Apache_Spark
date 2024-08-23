#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)


# In[8]:


input = sc.textFile("Book")
input.take(20)


# In[10]:


words=input.flatMap(lambda x : x.split())
words


# In[13]:


for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode())


# In[ ]:




