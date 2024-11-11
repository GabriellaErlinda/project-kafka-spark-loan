import time
import pandas as pd
import random
from kafka import KafkaProducer

# Baca data CSV
file_path = r'loan_data.csv'
data = pd.read_csv(file_path)

# Inisialisasi Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Kirim data per baris dengan jeda acak
for index, row in data.iterrows():
    message = row.to_json().encode('utf-8')  # Ubah ke JSON format
    producer.send('streaming-data', value=message)
    print(f"Sent: {message}")
    
    # Jeda acak antara 1 hingga 5 detik
    time.sleep(random.randint(1, 5))

producer.flush()
producer.close()
