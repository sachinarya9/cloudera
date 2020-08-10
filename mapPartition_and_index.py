import pyspark
from pyspark import SparkContext, SparkConf
import os
os.environ["SPARK_HOME"] = "/usr/lib/spark"


conf = SparkConf()
sc = SparkContext(conf=conf)  # type: SparkContext
rdd = sc.parallelize([1,2,3,4],2)

def part(iterator):
    sum = 0
    for elem in iterator:
        sum += elem
    return sum



map_rdd = rdd.mapPartitions(part)
# print(map_rdd.take(100))

rdd_sample = sc.parallelize(range(100),4)

# print(rdd_sample.sample(True,0.1,81).collect())
# print(rdd_sample.collect())


rdd_un = sc.parallelize([1,1,2,3])
rdd_inter = sc.parallelize([3])
print('Sachin',rdd_un.intersection(rdd_inter).collect())


dis = rdd_un.distinct()
print(sorted(dis.collect()))

rdd_gr = sc.parallelize((range(10)))
tran_gr = rdd_gr.map(lambda x:(x,x*'a'))
tran_gr2 = rdd_gr.map(lambda a,b: (a,b))

rdd_group = sc.parallelize([('and',2),('the',1),('and',1),('the',2)])
join_rdd = sc.parallelize([('and',7),('and',9),('by',6)])
# rdd_group = sc.parallelize(['the','and','the'])
map_group = rdd_group.map(lambda a:(a,1))
# reduce by key example
reduced_rdd = map_group.reduceByKey(lambda a,b:a+b)
grouped_rdd = map_group.groupByKey().mapValues(list)
list7 = [('I',1),('am',1),('a',3),('learning',4),('associate',5)]
sort_rdd = sc.parallelize(list7).sortByKey(True,2,keyfunc=lambda a:a.lower())
# map_group = rdd_group.groupByKey(lambda a,b:a+b)
# print(sort_rdd.collect())

joined_rdd = rdd_group.fullOuterJoin(join_rdd)
# print(joined_rdd.collect())


a=  sc.parallelize([('the',1)])
b = sc.parallelize([('bye',1),('the',3)])
c = [('get',1)]
w = [('Hey',1),('Hi',1)]
cogrouped_rdd = sorted(list(a.cogroup(b).collect()))
final_list = []
# print(cogrouped_rdd.collect())
for x,y in cogrouped_rdd:
    final_list.append((x,tuple(map(list,y))))
print(final_list)
cogrouped_rdd = [(a, tuple(map(list, b))) for a, b in sorted(list(a.cogroup(b).collect()))]
print(cogrouped_rdd)

##################################cartesian

a =sc.parallelize([1,2])
b = sc.parallelize([3,4])
cartesian_rdd = a.cartesian(b)
# print(cartesian_rdd.take(100))


##################################coalesce

a = sc.parallelize([1,2,3,4,5,6],3)
b = a.coalesce(1).glom()
print(b.collect())


##################################repartition

a = sc.parallelize([1,2,3,4,5,6],1).repartition(3).glom().collect()
print('After repartition',a)

##################################repartitionandsortwithinpartition

a = sc.parallelize([('a',1),('d',2),('b',1),('e',1)],1)
b = a.repartitionAndSortWithinPartitions(2).glom().collect()
print('repartition with sort',b)


##################################Closure

data = [1,2,3,4,5,6]
counter = 0

def increment_counter(x):
    global counter
    counter += x

rdd = sc.parallelize(data)
rdd.foreach(increment_counter)
print(counter)

###################################################COunting number of same lines

text = sc.textFile('file:/home/cloudera/Desktop/test.txt')
print('TYpe is',type(text))
print(text.collect())
text_map = text.map(lambda s:(s,1))
print('Type for text map is',type(text_map))
print(text_map.collect())
text_reduce = text_map.reduce(lambda a,b:a+b)
print('Type for reduced text is',type(text_reduce))
print(text_reduce)


