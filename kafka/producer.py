import json
import csv
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('../data/Loan_Default.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Serialize row to JSON format
        message = json.dumps(row).encode('utf-8')
        print("Sending message:", message)  # Debugging line

        # Send to Kafka
        producer.send('loan_topic', value=message)
        
        # Random delay
        time.sleep(random.uniform(0.5, 2.0))

producer.flush()
producer.close()
