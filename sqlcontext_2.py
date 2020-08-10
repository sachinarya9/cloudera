from pyspark.sql import SQLContext,Row
from pyspark import SparkContext,SparkConf
import os

os.environ["SPARK_HOME"] = "/usr/lib/spark"

sparkconf = SparkConf()
sc = SparkContext(conf = sparkconf)
sqlcontext = SQLContext(sc)
rdd = sc.textFile('file:/home/cloudera/Desktop/file.txt')

rdd2 = rdd.map(lambda r:r.split(','))
rdd3 = rdd2.map(lambda l:Row(name=l[0],age = int(l[2])))
print(rdd3.collect())
df = sqlcontext.createDataFrame(rdd3)

df.registerTempTable('people')

df2 = sqlcontext.sql('select * from people where age >30')
df3 = sqlcontext.sql('select * from parquet.`file:/home/cloudera/Desktop/parquet_data.parquet` where name = "Sachin Arya"')
df3.show()
df.write.save('file:/home/cloudera/Desktop/data.json',format = 'json',mode = 'append')
# df.write.save('file:/home/cloudera/Desktop/parquet_data.parquet',format = 'parquet')