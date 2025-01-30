from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, IntegerType

# Package dependencies
kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1"  # Adjust the version as per your Spark version
redshift_package = "io.github.spark-redshift-community:spark-redshift_2.12:6.2.0-spark_3.5"  # Community version for Scala 2.12

# Initialize Spark Session with Kafka, Redshift, and Avro packages
spark = SparkSession.builder \
    .appName("PySpark Kafka to Redshift with Stateful Deduplication") \
    .config("spark.jars.packages", f"{kafka_package},{redshift_package}") \
    .config("spark.jars", "/Users/shubham/Desktop/NooB 2.0/Project Class 3/kafka-spark-redshift-streaming/redshift-jdbc42-2.1.0.12.jar") \
    .getOrCreate()

# Kafka Configuration
kafka_bootstrap_servers = 'localhost:9092'  # Replace with your Kafka server address
kafka_topic = 'telecom-data'

# Schema of Incoming Data
schema = StructType() \
    .add("caller_name", StringType()) \
    .add("receiver_name", StringType()) \
    .add("caller_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("start_datetime", StringType()) \
    .add("end_datetime", StringType()) \
    .add("call_duration", IntegerType()) \
    .add("network_provider", StringType()) \
    .add("total_amount", StringType())

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option('startingOffsets', 'latest') \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Data Quality Check (Example: Ensuring call_duration is positive)
df = df.filter(df.call_duration > 0)

# Redshift Configuration
redshift_jdbc_url = "jdbc:redshift://redshift-cluster-1.cp6taicsq2ry.us-east-1.redshift.amazonaws.com:5439/dev"
redshift_table = "telecom.telecom_data"
s3_temp_dir = "s3n://temp-gds-2/temp/"

# Writing Data to Redshift
def write_to_redshift(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", redshift_jdbc_url) \
        .option("user", "admin") \
        .option("password", "Admin123") \
        .option("dbtable", redshift_table) \
        .option("tempdir", s3_temp_dir) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .mode("append") \
        .save()

print("Streaming started !")
print("********************************")
# Execute Streaming Query
query = df.writeStream \
    .foreachBatch(write_to_redshift) \
    .outputMode("update") \
    .start()

query.awaitTermination()
