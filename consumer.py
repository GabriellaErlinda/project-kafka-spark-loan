from confluent_kafka import Consumer
import json
import pandas as pd
from datetime import datetime, timedelta

# Initialize Kafka Consumer with configuration dictionary
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
})

# Subscribe to the topic
consumer.subscribe(['LoanSpark'])

# Batch and processing parameters
batch = []
batch_size = 1000  # Define your batch size
time_window = timedelta(minutes=5)
start_time = datetime.now()
batch_count = 0  # Counter to limit to 3 batches
max_batches = 3  # Limit to 3 batches

# Directory to save CSV files
save_directory = 'D:/SEMS-5/BIG DATA/Project2/project-kafka-spark-loan/output/loan_batch'

while True:
    message = consumer.poll(0.1)  # Poll with a timeout of 1 second

    if message is None:
        continue
    if message.error():
        print(f"Consumer error: {message.error()}")
        continue

    # Deserialize and add message to batch
    batch.append(json.loads(message.value().decode('utf-8')))

    # Check if batch meets size or time window criteria
    if len(batch) >= batch_size or (datetime.now() - start_time) >= time_window:
        # Save batch as a CSV file
        df = pd.DataFrame(batch)
        df.to_csv(f'{save_directory}/batch_{batch_count+1}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv', index=False)
        
        # Clear batch and reset time
        batch = []
        start_time = datetime.now()
        
        # Increment batch count and check limit
        batch_count += 1
        if batch_count >= max_batches:
            print("Processed 3 batches. Stopping consumer.")
            break

# Close the consumer
consumer.close()
