import os

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from pyspark.sql import SparkSession

os.environ['HADOOP_HOME'] = "E:\\var\\big.data\\hadoop-3.2.2"
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = "E:\\var\\big.data\\hadoop-3.2.2\\lib\\native"

# Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('binance-trade-consumer')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate()
         )

schema = StructType().add("s", StringType()).add("p", StringType()).add("q", StringType())

df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-server:9092")  # kafka server
      .option("subscribe", "trades")  # topic
      .option("startingOffsets", "earliest")  # start from beginning
      .load()
      .select(col("key").cast("string"), from_json(col("value").cast("string"), schema))
      )
