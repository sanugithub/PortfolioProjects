from kafka import KafkaProducer
import json
import random
from datetime import datetime, timedelta
from faker import Faker
import time

# Kafka configuration
bootstrap_servers = ['localhost:9092']  # Change to your Kafka server address
topicName = 'telecom-data'  # Change to your Kafka topic name

# Creating Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Initialize Faker for generating random names
fake = Faker()

# Function to generate random telecom data with names
def generate_telecom_data():
    random_names = [fake.name() for _ in range(100)]
    call_duration = random.randint(1, 1200)
    start_datetime = datetime.now()
    end_datetime = start_datetime + timedelta(seconds=call_duration)
    caller_name, receiver_name = random.sample(random_names, 2)
    caller_id = f'+1{random.randint(1000000000, 9999999999)}'
    receiver_id = f'+1{random.randint(1000000000, 9999999999)}'
    network_providers = ['Verizon', 'AT&T', 'T-Mobile', 'Sprint']
    network_provider = random.choice(network_providers)
    rate_per_minute = 0.05
    total_amount = round((call_duration / 60) * rate_per_minute, 2)

    return {
        'caller_name': caller_name,
        'receiver_name': receiver_name,
        'caller_id': caller_id,
        'receiver_id': receiver_id,
        'start_datetime': start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        'end_datetime': end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        'call_duration': call_duration,
        'network_provider': network_provider,
        'total_amount': total_amount
    }

# Infinite loop to continuously send data
try:
    while True:
        data = generate_telecom_data()
        producer.send(topicName, value=data)
        print(f"Data sent: {data}")
        # Wait for 1 second before sending the next event
        time.sleep(3)
except KeyboardInterrupt:
    print("Data generation stopped.")
