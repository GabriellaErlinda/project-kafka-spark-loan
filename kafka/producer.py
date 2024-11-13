import json
import csv
import time
from kafka import KafkaProducer

# Kafka setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Batch configuration
batch_size = 100
num_batches = 3
batch = []

with open('../data/Loan_Default.csv', 'r') as file:
    reader = csv.DictReader(file)
    batch_count = 0

    for row in reader:
        # Add the message to the current batch
        batch.append(row)

        # Check if the batch has reached the specified batch size
        if len(batch) == batch_size:
            # Serialize batch to JSON format and send it
            batch_message = json.dumps(batch).encode('utf-8')
            producer.send('loan_topic', value=batch_message)
            
            # Clear the batch and increment the batch counter
            batch = []
            batch_count += 1

            print(f"proses batch {batch_count} udah selesai")
            
            # Check if we've sent the desired number of batches
            if batch_count == num_batches:
                break
            
            # Delay between batches
            time.sleep(1.0)

print("Proses selesai")  # Print only once when all batches are sent

# Ensure all messages are sent before closing the producer
producer.flush()
producer.close()
