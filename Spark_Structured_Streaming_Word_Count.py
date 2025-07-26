#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Spark real-time streaming exercise
# July 26, 2025
# Ref: https://spark.apache.org/docs/latest/streaming/getting-started.html
# Use virenv since latest Java, Spark, PySpark, and Python are not compatible 
# pyenv install 3.11.9
# pyenv virtualenv 3.11.9 pyspark311
# Remember to run the Jupyter Notebook within the virtualenv
# export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
# export PATH="$JAVA_HOME/bin:$PATH"
# pip install pyspark jupyterlab findspark
# pyenv activate pyspark311
# jupyter lab


# In[ ]:


# Verified Java is installed properly
import os
print(os.environ.get("JAVA_HOME"))

# Should see something like this:/Users/miaolin/Library/Caches/Coursier/arc/https/github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.27%252B6/OpenJDK11U-jdk_aarch64_mac_hotspot_11.0.27_6.tar.gz/jdk-11.0.27+6/Contents/Home


# In[ ]:


# nano ~/.zshrc
# Add the path to the end of the file
# source ~/.zshrc


# In[2]:


# Fix var path
import os
import findspark
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
os.environ["SPARK_HOME"] = "/Users/miaolin/Downloads/spark-3.5.6-bin-hadoop3"
os.environ["PATH"] = os.environ["SPARK_HOME"] + "/bin:" + os.environ.get("PATH", "")

findspark.init(os.environ["SPARK_HOME"])

spark = SparkSession.builder.appName("Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully!")


# In[4]:


import os
print("JAVA_HOME =", os.environ.get("JAVA_HOME"))
print("SPARK_HOME =", os.environ.get("SPARK_HOME"))


# In[10]:


# Verified PySpark installation
# pip show pyspark


# In[5]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


# In[ ]:


# nc -lk 9999


# In[ ]:


# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()


# In[ ]:


 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()


# In[ ]:




