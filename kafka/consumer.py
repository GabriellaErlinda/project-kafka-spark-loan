from kafka import KafkaConsumer
import time
import json
import os

# Kafka setup
consumer = KafkaConsumer(
    'loan_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='loan-consumer-group'
)

# Directory for saving batches
os.makedirs('../data/batches', exist_ok=True)

batch_count = 0

for message in consumer:
    # Decode the JSON message
    decoded_message = message.value.decode('utf-8')
    
    # Load the JSON data
    try:
        batch_data = json.loads(decoded_message)
    except json.JSONDecodeError:
        print("Skipping message: not valid JSON format")
        continue

    # Save the batch data to a new file
    batch_filename = f'../data/batches/batch_{batch_count + 1}.json'
    with open(batch_filename, 'w') as f:
        json.dump(batch_data, f, indent=4)
    print(f"Batch {batch_count + 1} saved to {batch_filename}")

    # Increment the batch count
    batch_count += 1

    # Print each message in the batch to simulate real-time streaming
    for data in batch_data:
        print(data)  # Display each record in the batch

    # Add a small delay to simulate continuous processing
    time.sleep(0.1)
