from pyspark import SparkConf, SparkContext
import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\徐健521\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'
conf = SparkConf().setMaster("local[*]").setAppName("test_spark")
sc = SparkContext(conf=conf)

# 准备一个RDD
rdd = sc.parallelize([1, 1, 3, 3, 5, 5, 7, 8, 8, 9, 10])
# 对RDD的数据进行去重
rdd2 = rdd.distinct()

print(rdd2.collect())