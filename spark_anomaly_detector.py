# spark_anomaly_detector.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, when, unix_timestamp, lag, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.window import Window

# 1. Initialize Spark Session

spark = SparkSession \
    .builder \
    .appName("SupplyChainAnomalyDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Reduce word quality of Spark logs

print("Spark Session created successfully.")

# 2. Define Schema for Kafka Message Value

schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("timestamp", StringType(), True), # Kafka timestamps are often strings
    StructField("location", StringType(), True),
    StructField("status", StringType(), True),
    StructField("estimated_arrival", StringType(), True),
    StructField("actual_arrival", StringType(), True),
    StructField("delay_hours", IntegerType(), True)
])

# 3. Read Data from Kafka

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "supply_chain_events") \
    .load()

print("Reading from Kafka topic 'supply_chain_events'...")

# 4. Parse JSON Value and Select Columns

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
                    .select(from_json(col("json_value"), schema).alias("data")) \
                    .select("data.*") \
                    .withColumn("processed_timestamp", col("timestamp").cast(TimestampType()))

print("Data parsed and 'processed_timestamp' column added.")

# 5. Simple Anomaly Detection Logic (e.g., high 'delay_hours' or unexpected 'status')


# Anomaly Condition 1: High Delay Hours
anomaly_flagged_df = parsed_df.withColumn(
    "is_anomaly",
    when(col("delay_hours") > 5, lit(True))
    .when((col("status") == "delayed") & (col("delay_hours") <= 0), lit(True))
    .otherwise(lit(False))
).withColumn(
    "anomaly_type",
    when(col("delay_hours") > 5, lit("HighDelay"))
    .when((col("status") == "delayed") & (col("delay_hours") <= 0), lit("InconsistentStatus"))
    .otherwise(lit(None)) # No anomaly type if not an anomaly
)

# Anomaly Condition 2: 'delivered' status but actual_arrival is null (inconsistency)
anomaly_flagged_df = anomaly_flagged_df.withColumn(
    "is_anomaly",
    when(
        (col("status") == "delivered") & (col("actual_arrival").isNull()), 
        lit(True)
    ).otherwise(col("is_anomaly"))
).withColumn(
    "anomaly_type",
    when(
        (col("status") == "delivered") & (col("actual_arrival").isNull()),
        lit("DeliveredButNoArrival")
    ).otherwise(col("anomaly_type"))
).withColumn( # NEW COLUMN ADDED HERE
    "is_actual_arrival_set",
    when(col("actual_arrival").isNotNull(), lit(True)).otherwise(lit(False))
)

# Anomaly Condition 3: Compare timestamp with estimated_arrival to calculate actual delay difference
from pyspark.sql.functions import unix_timestamp

anomaly_flagged_df = anomaly_flagged_df.withColumn(
    "delay_difference_hours",
    (
        (unix_timestamp(col("actual_arrival")) - unix_timestamp(col("estimated_arrival"))) / 3600
    )
)

anomaly_flagged_df = anomaly_flagged_df.withColumn(
    "is_anomaly",
    when(
        (col("delay_difference_hours") > 2) & (col("actual_arrival").isNotNull()),  # e.g., more than 2 hours late
        lit(True)
    ).otherwise(col("is_anomaly"))
).withColumn(
    "anomaly_type",
    when(
        (col("delay_difference_hours") > 2) & (col("actual_arrival").isNotNull()),
        lit("LateArrival")
    ).otherwise(col("anomaly_type"))
)

anomalies_to_store_df = anomaly_flagged_df.filter(col("is_anomaly") == True) \
                                         .select(
                                             col("shipment_id"),
                                             col("processed_timestamp").alias("timestamp"), # Map to Cassandra 'timestamp' column
                                             col("location"),
                                             col("status"),
                                             col("estimated_arrival"),
                                             col("actual_arrival"),
                                             col("delay_hours"),
                                             col("is_anomaly"),
                                             col("anomaly_type")
                                         )

print("Anomaly detection logic applied and anomalies filtered.")


# You could also add more sophisticated checks:
# - Check if actual_arrival is null for 'delivered' status (inconsistency)
# - Compare timestamp with estimated_arrival to calculate actual delay difference

# 6. Output to Console (for testing)
# In a real scenario, you'd write to Cassandra/MongoDB or another sink.
query = anomalies_to_store_df \
    .writeStream \
    .option("checkpointLocation", "file:///tmp/spark-checkpoints/anomalies_cassandra") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "supplyguard_ks") \
    .option("table", "detected_anomalies") \
    .outputMode("append") \
    .start()

print("Spark streaming query started, writing anomalies to Cassandra...")

query.awaitTermination()