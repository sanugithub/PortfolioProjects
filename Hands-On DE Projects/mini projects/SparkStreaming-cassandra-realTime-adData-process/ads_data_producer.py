from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")

with open('mock_ads_data.json') as f:
    data = json.load(f)

p = Producer({'bootstrap.servers': 'localhost:9092'})

for record in data:
    p.poll(0)
    record_str = json.dumps(record)
    p.produce('ads_data', record_str, callback=delivery_report)
    print("Message Published -> ",record_str)
    time.sleep(0.5) # wait for half second before sending the next record

p.flush()
