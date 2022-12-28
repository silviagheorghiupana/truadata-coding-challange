from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("silvia-part1-RDD").setMaster("local[*]")
sc = SparkContext(conf=conf)
from google.colab import files
upload_files = files.upload()
lines = sc.textFile("groceries.csv")
lines.take(5)
rdd01=lines.flatMap(lambda x: [(w) for w in x.split(',')])
rdd01.take(10)
rdd02=rdd01.distinct()
rdd02.take(10)
rdd02.saveAsTextFile('out/out_1_2a.txt')

