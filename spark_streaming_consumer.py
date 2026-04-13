from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("sensor_id", IntegerType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", LongType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING)")

json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

result = json_df.groupBy("sensor_id") \
    .agg(avg("temperature").alias("avg_temp"),
         avg("humidity").alias("avg_humidity"))

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
