from kafka import KafkaConsumer
import time
import json
import os

consumer = KafkaConsumer(
    'loan_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='loan-consumer-group'
)

batch = []
batch_size = 50    # Define batch size based on message count
time_window = 30   # Define time window in seconds

start_time = time.time()

# Ensure the output directory exists
os.makedirs('../data/batches', exist_ok=True)

for message in consumer:
    try:
        # Attempt to decode and parse JSON
        decoded_message = message.value.decode('utf-8')
        print("Received raw message:", decoded_message)  # Debugging line
        json_message = json.loads(decoded_message)
        batch.append(json_message)

    except json.JSONDecodeError:
        print("Skipping message: not valid JSON format")
        continue

    # Check if batch size or time window has been met
    if len(batch) >= batch_size or (time.time() - start_time) >= time_window:
        # Save the batch data
        batch_filename = f'../data/batches/batch_{int(time.time())}.json'
        with open(batch_filename, 'w') as f:
            json.dump(batch, f, indent=4)
        print(f"Batch saved to {batch_filename}")

        # Reset batch and timer
        batch = []
        start_time = time.time()
