from pyspark.sql import SQLContext, Row


import pyspark
from pyspark import SparkContext, SparkConf
import os
os.environ["SPARK_HOME"] = "/usr/lib/spark"


conf = SparkConf()
sc = SparkContext(conf=conf)
sqlcontext = SQLContext(sc)
dict = [['Sachin','7'],['Kapil','8']]
rdd = sc.parallelize(dict)

# rdd1 = rdd.map(lambda b: b.split(','))
rdd2 = rdd.map(lambda x : Row(name = x[0], id = int(x[1])))
print(rdd2.collect())
df = sqlcontext.createDataFrame(rdd)
print(df)
df.write.save('file:/home/cloudera/Desktop/data.json',format = 'json')
