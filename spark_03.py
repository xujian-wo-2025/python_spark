from pyspark import SparkConf,SparkContext
import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\徐健521\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'


cs = SparkConf().setMaster("local[*]").setAppName("test_spark")

sc = SparkContext(conf=cs)

rdd = sc.parallelize(['itheima itcast 666', 'itheima itheima itcast', 'python c++'])

rdd2 = rdd.flatMap(lambda x: x.split(' '))

print(rdd2.collect())