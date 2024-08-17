#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install pyspark


# In[2]:


from pyspark import SparkConf,SparkContext


# In[9]:


sc=SparkContext.getOrCreate()


# In[10]:


def parseLine(line):
    fields=line.split(',')
    stationID=fields[0]
    entryType=fields[2]
    temperature=float(fields[3])*0.1*(0.9/0.5)+32.0
    return (stationID,entryType,temperature)


# In[12]:


lines=sc.textFile('1800.csv')

parsedLines=lines.map(parseLine)
minTemps=parsedLines.filter(lambda x:'TMIN' in x[1])
stationTemps=minTemps.map(lambda x:(x[0],x[2]))
minTemps=stationTemps.reduceByKey(lambda x,y:min(x,y))
results=minTemps.collect()
results


# Summary:
# 
# Parsed the raw lines into structured data (station ID, temperature type, temperature).
# 
# Filtered the data to keep only minimum temperature records.
# 
# Mapped the data to focus on station ID and temperature.
# 
# Reduced by station to find the minimum temperature for each station.
# 
# Collected the results back to the driver and printed them.

# In[ ]:




