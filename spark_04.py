from pyspark import SparkConf,SparkContext
import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\徐健521\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'


cs = SparkConf().setMaster("local[*]").setAppName("test_spark")

sc = SparkContext(conf=cs)

rdd = sc.parallelize([('nan', 99), ('nan', 88), ('nv', 99), ('nv', 66)])
def func(x, y):
    return x+y

rdd2 = rdd.reduceByKey(func)

print(rdd2.collect())
