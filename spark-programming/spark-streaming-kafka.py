import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, desc
from pyspark.sql.types import StructType, StringType

os.environ['HADOOP_HOME'] = "D:\\stage\\opt\\hadoop-3.3.1"
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = "D:\\opt\\hadoop-3.3.1\\lib\\native"

ss = (SparkSession.builder.master('local')
      .appName('trade-processor')
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
      .getOrCreate())

df = ss.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "binance-trades") \
    .option("startingOffsets", "earliest") \
    .load()

df_schema = StructType() \
    .add("s", StringType()) \
    .add("p", StringType()) \
    .add("q", StringType()) \
    .add("b", StringType()) \
    .add("a", StringType())

df1 = df.select(from_json(col("value").cast("string"), df_schema).alias("trade"))
df1 = df1.withColumn('price', df1['trade.p'].cast("float"))
df1 = df1.withColumn('quantity', df1['trade.q'].cast("float"))
df1 = df1.withColumn('volume', df1['price'] * df1['quantity'])

df1.printSchema()

aggDF = df1.groupBy("trade.a").agg(sum("volume").alias("vol"), count("volume")).orderBy(desc("vol"))

df_console_write = aggDF \
    .writeStream \
    .option("numRows", 80) \
    .option("truncate", "false") \
    .outputMode("complete") \
    .format("console") \
    .start()

df_console_write.awaitTermination()
