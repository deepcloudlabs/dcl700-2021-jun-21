import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

os.environ['HADOOP_HOME'] = "D:\\opt\\hadoop-3.2.2"
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = "D:\\opt\\hadoop-3.2.2\\lib\\native"

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 9999)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination(timeout=100)
