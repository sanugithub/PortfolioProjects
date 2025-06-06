import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

from dotenv import load_dotenv
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
# import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'


# packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
#             "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0"]

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Ads Stream") \
    .master("local[3]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
load_dotenv()

# Define the schema of the JSON data
schema = StructType([
    StructField("ad_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("clicks", IntegerType()),
    StructField("views", IntegerType()),
    StructField("cost", FloatType()),
])

# Read the stream from Kafka
df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "ads_data")\
    .option("startingOffsets", "earliest")\
    .load()


# Parse the JSON data and select the fields
df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Perform the aggregation in windows of 1 minute and sliding interval of 30 secs
df = df.groupBy("ad_id", window(df.timestamp, "1 minute", "30 seconds")).agg(sum("clicks").alias("total_clicks"),
                                                                             sum("views").alias("total_views"),
                                                                             sum("cost").alias("total_cost"),
                                                                             sum("cost")/sum("views").alias("avg_cost_per_view"))


########## connect cassandra db ###########

# This secure connect bundle is autogenerated when you download your SCB, 
# if yours is different update the file name below
cloud_config= {
  'secure_connect_bundle': 'secure-connect-ads-data-db.zip'
}

# This token JSON file is autogenerated when you download your token, 
# if yours is different update the file name below
with open("ads_data_db-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")

keyspace="ads_data"
################## end ####################

#### create table #######
# Table does not exist, create it
create_table_query = f"""
CREATE TABLE IF NOT EXISTS ads_data.agg_ads_data (
    ad_id TEXT,
    total_clicks INT,
    total_views INT,
    total_cost FLOAT,
    avg_cost_per_view FLOAT,
    PRIMARY KEY (ad_id)
)
"""
session.execute(create_table_query)

# Convert Spark DataFrame to Pandas DataFrame
# pandas_df = df.toPandas()

# Write the output to the console in complete mode
write_query = df.writeStream \
.outputMode("update") \
.format("org.apache.spark.sql.cassandra") \
.option("keyspace", "ads_data") \
.option("table", "agg_ads_data") \
.option("truncate", "false") \
.trigger(processingTime="3 second") \
.start() \
.awaitTermination()


# Insert data into Cassandra table
# for index, row in pandas_df.iterrows():
#     existing_record_query = f"select ad_id, total_clicks, total_views, total_cost, avg_cost_per_view from ads_data.agg_ads_data where ad_id='{row['ad_id']}' ALLOW FILTERING"
#     result_er = session.execute(existing_record_query)
#     existing_record = result_er.one()

#     if count(existing_record[0])>=1:
#        update_query = f"""UPDATE ads_data.agg_ads_data SET total_clicks={existing_record[1]+row['total_clicks']}, total_views={existing_record[2]+row['total_views']}, total_cost={existing_record[3]+row['total_cost']}, avg_cost_per_view={(existing_record[3]+row['total_cost'])/(existing_record[2]+row['total_views'])} WHERE ad_id='{row['ad_id']}'
#         """
#        session.execute(update_query)

       
#     else:
#        insert_query = f"""INSERT INTO ads_data.agg_ads_data (ad_id, total_clicks, total_views, total_cost, avg_cost_per_view) VALUES ('{row['ad_id']}', {row['total_clicks']}, {row['total_views']}, {row['total_cost']}, {row['avg_cost_per_view']})
#         """
#        session.execute(insert_query)

       

# CHECKER
query = "SELECT * FROM ads_data.agg_ads_data"
result = session.execute(query)

# Print the results
for row in result:
    print(row)

############ end ###########
