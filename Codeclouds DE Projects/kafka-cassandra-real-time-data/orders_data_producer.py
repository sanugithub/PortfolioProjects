#!/usr/bin/env python
# coding: utf-8

# In[70]:


#Import necessary libraries

import threading
from time import sleep
from uuid import uuid4

# import mysql.connector
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from avro import schema, io
# from datetime import datetime, timedelta
# import time
# import pickle
# import json
import math
import pandas as pd


# In[71]:


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Order record {}: {}".format(msg.key(), err))
        return
    print('Order record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


# In[72]:


def fetch_and_produce_data(producer, data):
    for index, row in data.iterrows():
        # Include all fields from the CSV file in the orders_data dictionary
        orders_data = {
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "order_status": row["order_status"],
            "order_purchase_timestamp": row["order_purchase_timestamp"],
            "order_approved_at": row["order_approved_at"],
            "order_delivered_carrier_date": row["order_delivered_carrier_date"],
            "order_delivered_customer_date": row["order_delivered_customer_date"],
            "order_estimated_delivery_date": row["order_estimated_delivery_date"]
            # Add other fields as needed
        }

        # Produce to Kafka with GPSprovider as key
        producer.produce(
            topic='ecommerce-orders',  # Replace with your Kafka topic
            key=f"{row['customer_id']}_{row['order_id']}",
            value=orders_data,
            on_delivery=delivery_report
        )

        print("Produced message:", orders_data)


# In[73]:

# def convert_nan_to_long(value):
#   if math.isnan(value):
#     return 0
#   else:
#     return value

# Load CSV data into Pandas DataFrame
data = pd.read_csv('olist_orders_dataset.csv')  # Replace with your CSV file path


# In[74]:


data.head(10)


# In[75]:


object_columns = data.select_dtypes(include=['object']).columns
data[object_columns] = data[object_columns].fillna('unknown value')


# In[76]:


data.dtypes


# In[77]:





# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'DSZRFJEEYC5DJRGR',
    'sasl.password': 'G2Ipwc64/MaBLAHyCO7r5ilzvwL1WfYGx8kmJYR6mW0IdFSC6SZS9ePJOteK+Wtr'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-10dzz.ap-southeast-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('L5DYQTKB4BQ6PNOI', '5/+eLthGNYX3o61kbqm37EhIuqmjcSSnQZOE+FsOgQUh5zvOYkQ5mzNdSJYZ5Zsi')
})

# Fetch the latest Avro schema for the value
subject_name = 'ecommerce-orders-value'  # Adjust the subject name accordingly
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

fetch_and_produce_data(producer, data)

# Close the producer after processing all rows
producer.flush()


# In[ ]:




