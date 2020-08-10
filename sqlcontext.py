from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
import os
import json
os.environ["SPARK_HOME"] = "/usr/lib/spark"


conf = SparkConf()
sc = SparkContext(conf=conf)

sqlcontext = SQLContext(sc)

file_data = sc.textFile('file:/home/cloudera/Desktop/file.txt')

parquet_data = sqlcontext.read.json('file:/home/cloudera/Desktop/data.json')
print('data is',parquet_data.show)
each_line = file_data.map(lambda l:l.split(','))
key_value_pair = each_line.map(lambda p:Row(name = p[0], age = int(p[2])))
print('Key value pair',type(key_value_pair))

employees = sqlcontext.createDataFrame(key_value_pair)
employees.registerTempTable('people')
only_names = sqlcontext.sql('select name from people')
print(type(only_names))
only_names.show()
# only_names.select("name").write.save("file:/home/cloudera/Desktop/namesAndAges.parquet", format="parquet")
rd_names = only_names.map(lambda n: 'name '+ n.name)
print(type(rd_names))
print(rd_names.collect())
# read_json = sqlcontext.read.json('file:/home/cloudera/Desktop/file.json')
# schemaPeople = sqlcontext.createDataFrame(file_data)
# schemaPeople.show()