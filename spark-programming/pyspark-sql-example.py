from pyspark.sql import SparkSession
import os

os.environ['HADOOP_HOME'] = "D:\\stage\\opt\\hadoop-3.3.1"
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = "D:\\stage\\opt\\hadoop-3.3.1\\lib\\native"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Example") \
    .getOrCreate()

df = spark.read.json("world.json")

df.show()

df.printSchema()

df.select("name").show()

df.select(df['name'], df['population'] + 1).show()

df.filter(df['population'] > 10000000).show()

df.groupBy("continent").count().show()

df.createOrReplaceTempView("countries")

sqlDF = spark.sql("SELECT count(*) FROM countries where population > 10000000")
sqlDF.show()

sqlDF = spark.sql("SELECT continent , count(*) FROM countries group by continent")
sqlDF.show()
