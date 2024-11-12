import time
import random
from confluent_kafka import Producer
import json
import pandas as pd

# Configure Confluent Kafka Producer
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**conf)

# Load dataset
data = pd.read_csv(r'D:\SEMS-5\BIG DATA\Project2\project-kafka-spark-loan\loan_data.csv')

# Delivery report handler
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Stream data row-by-row
for index, row in data.iterrows():
    message = row.to_dict()
    # Send message to 'LoanSpark' topic
    producer.produce(
        'LoanSpark',
        value=json.dumps(message),
        callback=delivery_report
    )
    producer.poll(0)  # Ensures delivery report callbacks are handled
    print(f'Sent: {message}')
    time.sleep(random.uniform(0.01, 0.03))  # Random delay

# Wait for any outstanding messages to be delivered
producer.flush()