# 1. 构建执行环境入口对象
from pyspark import SparkContext, SparkConf
import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\徐健521\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'

conf = SparkConf().setMaster("local[*]").setAppName("test_spark")
sc = SparkContext(conf=conf)
# 2. 读取数据文件
rdd = sc.textFile("D:\\py基础\\spark的学习\\wordcount.txt")
# 3. 取出全部单词
word_rdd = rdd.flatMap(lambda x: x.split(","))
print(word_rdd.collect())
# 4. 将所有单词都转换成二元元组，单词为Key，value设置为1
word_with_one_rdd = word_rdd.map(lambda word: (word, 1))
print(word_with_one_rdd.collect())
# 5. 分组并求和
result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a + b)
# 6. 打印输出结果
print(result_rdd.collect())
