from __future__ import print_function
from pyspark import SparkConf, SparkContext
import os

os.environ["SPARK_HOME"] = "/usr/lib/spark"

# counter = 0
conf = SparkConf()
sc = SparkContext(conf = conf)
file_rdd = sc.textFile('file:/home/cloudera/Desktop/test.txt')
# file_rdd.collect().foreach(println)

pairs = file_rdd.map(lambda s:(s,1))
print(pairs.take(100))
new_pairs = file_rdd.flatMap(lambda s:(s,1))
print(new_pairs.take(100))
# print(type(pairs))
# print(file_rdd)
# print(file_rdd.take(100))
# print(pairs.take(100))
count = pairs.reduceByKey(lambda a,b : a+b)

data = [2,4,6,8,3]
counter = sc.accumulator(0)
rdd = sc.parallelize(data)
def increment_counter(num):
    print('Sachin')
    global counter
    counter += num
    # print(counter)

cn = rdd.foreach(increment_counter)
print(counter)