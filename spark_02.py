from pyspark import SparkConf,SparkContext
import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\徐健521\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'


cs = SparkConf().setMaster("local[*]").setAppName("test_spark")

sc = SparkContext(conf=cs)


rdd = sc.parallelize([1, 2, 3, 4, 5])


def func(data):
    return data * 10



rdd2 = rdd.map(func)

print(rdd2.collect())