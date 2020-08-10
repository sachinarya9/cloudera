import pyspark
from pyspark import SparkContext, SparkConf

import os
os.environ["SPARK_HOME"] = "/usr/lib/spark"


conf = SparkConf()
sc = SparkContext(conf=conf)  # type: SparkContext
data = {'Name':['Sachin','Arya'],'Nature':['Great','Man'],'Work':['Great','Work']}
simple_rdd = sc.parallelize(data)
# print(simple_rdd.collect())
new_rdd = simple_rdd.reduce(lambda a,b:a+b)
# print('Take function',new_rdd)
# print('Collect function',simple_rdd.collect())
file_rdd = sc.textFile('file:/home/cloudera/Desktop/test.txt')
# print(file_rdd.reduce(lambda a,b:a+b))
split_rdd = file_rdd.flatMap(lambda a: a.split(' '))
# print(split_rdd.take(100))
number_rdd = split_rdd.map(lambda a:(a,1))
# print(number_rdd.take(100))
final_rdd = number_rdd.reduceByKey(lambda a,b:a+b)
print(final_rdd.take(100))
sequence_rdd = sc.parallelize(range(1,4)).map(lambda x:(x,'a'*1))
# print(type(sequence_rdd))
# sequence_rdd.saveAsSequenceFile('file:/home/cloudera/Desktop/sequence_file.txt')
# print(combined_data)
# sc.sequenceFile()
data_new = [2,3,4]
rdd_new = sc.parallelize(data_new)
# print(rdd_new.flatMap(lambda a:range(1,a)).take(100))
if __name__ == '__main__':
    def first_def(s):
        print('SAchin')
        split_list = s.split(" ")
        print(type(split_list))
        return len(split_list)
    print(sc.textFile('/home/cloudera/Desktop/test.txt').map(first_def))
