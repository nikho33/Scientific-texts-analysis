#!/usr/bin/env python
# coding: utf-8

# # 1. Settings

# In[1]:


import os
import findspark

findspark.init()

from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql import functions as F


# In[4]:


local = "local[*]"

appName = "Scientific papers analysis app"

configLocale = SparkConf().setAppName(appName).setMaster(local). set("spark.executor.memory", "4G"). set("spark.driver.memory", "4G"). set("spark.sql.catalogImplementation", "in-memory")

spark = SparkSession.builder.config(conf=configLocale).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark


# In[8]:


print("Id : ", sc.applicationId)
print("Version : ", sc.version)


# In[9]:


# https://medium.com/@connectwithghosh/topic-modelling-with-latent-dirichlet-allocation-lda-in-pyspark-2cb3ebd5678e
import pandas as pd
import pyspark
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[24]:


# For building the model
from pyspark.ml.feature import CountVectorizer, HashingTF, IDF
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.mllib.clustering import LDA, LDAModel

# # 2. Data
path = "./Data/DataMicroTAS/"
filename = "Datas_MicroTAS2018.csv"
# Reading the data
data = sqlContext.read.format("csv")   .options(header='true', inferschema='true')    .load(os.path.realpath(path + filename))

#print(type(data), dir(data))
data.show(2)

selection = "text"
data2 = data.select("filename", F.regexp_replace(F.col(selection), "[\[\]']", "").alias(selection)) # [\$#,]
data2 = data2.select("filename", split(col(selection), ",\s*").alias(selection))
data2.show(2)
data2

#tokens = data.select("filename", "text")
#tokens.show(2)

# # 3. LDA algorithm

#df_txts = sqlContext.createDataFrame(tokens, ["list_of_words",'index'])
df_txts = data2.select("filename", "text")
df_txts.show(2)
df_txts

# TF
cv = CountVectorizer(inputCol="text", outputCol="raw_features", vocabSize=5000, minDF=10.0)
cvmodel = cv.fit(df_txts)
result_cv = cvmodel.transform(df_txts)

print(cvmodel)
print(result_cv.show(3))

# IDF
idf = IDF(inputCol="raw_features", outputCol="features")
idfModel = idf.fit(result_cv)
result_tfidf = idfModel.transform(result_cv)

print(result_tfidf)


# LDA
num_topics = 10
max_iterations = 100
lda_model = LDA.train(result_tfidf[['filename','features']].map(list), k=num_topics, maxIterations=max_iterations)

## representation
wordNumbers = 5
topicIndices = sc.parallelize(lda_model.describeTopics \
                                  (maxTermsPerTopic = wordNumbers))
def topic_render(topic):
    terms = topic[0]
    result = []
    for i in range(wordNumbers):
        term = vocabArray[terms[i]]
        result.append(term)
    return result

topics_final = topicIndices.map(lambda topic:
                                topic_render(topic)).collect()
for topic in range(len(topics_final)):
    print ("Topic" + str(topic) + ":")
    for term in topics_final[topic]:
        print (term)
    print ('\n')

