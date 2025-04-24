from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[*]').setAppName('test_spark_app')


sc = SparkContext(conf=conf)


rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.parallelize((1, 2, 3, 4, 5))
rdd3 = sc.parallelize("abcdefg")
rdd4 = sc.parallelize({1, 2, 3,4 ,5,6})
rdd5 = sc.parallelize({'key1': 'value1', "key2": 'values'})
# rdd6 = sc.textFile("")    #文件的路径


print(rdd1.collect())
print(rdd2.collect())
print(rdd3.collect())
print(rdd4.collect())
print(rdd5.collect())

sc.stop()