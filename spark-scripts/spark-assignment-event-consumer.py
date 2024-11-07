import os
import pyspark
import pyspark.sql
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import col, count, from_json, from_unixtime, sum, window
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

SPARK_HOST_NAME = os.getenv("SPARK_MASTER_HOST_NAME")
SPARK_PORT = os.getenv("SPARK_MASTER_PORT")
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{SPARK_HOST_NAME}:{SPARK_PORT}"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"

spark = pyspark.sql.SparkSession.builder\
        .appName("AssignmentStreaming")\
        .master("local")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
        .config("spark.sql.shuffle.partitions", 4)\
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)\
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Mapping schema from producer
schema = StructType(
    [
        StructField('order_id', StringType(), nullable=True),
        StructField('customer_id', IntegerType(), nullable=True),
        StructField('customer_name', StringType(), nullable=True),
        StructField('furniture', StringType(), nullable=True),
        StructField('color', StringType(), nullable=True),
        StructField('price', IntegerType(), nullable=True),
        StructField('timestamp', LongType(), nullable=True)

    ]
)

# Read Streaming data from kafka
stream_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")\
        .option("subscribe", KAFKA_TOPIC)\
        .option("startingOffsets", "latest")\
        .load()


# Simple preprocessing and convert timestamp from column "timestamp" unix type to datetime
# for using watermark method
parsed_df = stream_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"), schema=schema).alias("data"))\
        .select("data.*")\
        .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))

# Aggegation to handling late data, and groupBy using window for group data aggregate
count_price_df = parsed_df.withWatermark("event_time", "1 hour")\
        .groupBy(window("event_time", "1 hour"))\
        .agg(
            count("*").alias("count_event_by_hour"),
            sum("price").alias("count_price_by_hour")
        )

# Return result using console and using complete mode to show all data 
# by event time include update data when using watermark
query = count_price_df.writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()

query.awaitTermination()