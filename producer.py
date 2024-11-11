import time
import csv
import json
import random
from confluent_kafka import Producer

# Fungsi callback untuk menangani pengiriman pesan
def delivery_report(err, msg):
    if err is not None:
        print(f"Pesan gagal dikirim: {err}")
    else:
        print(f"Pesan terkirim ke {msg.topic()} [{msg.partition()}]")

# Inisialisasi Kafka Producer dengan konfigurasi
conf = {
    'bootstrap.servers': 'localhost:9092',  # Alamat Kafka server
}

producer = Producer(conf)

# Baca file CSV dan kirim data per baris dengan jeda acak
file_path = 'loan_data.csv'

with open(file_path, mode='r') as file:
    csv_reader = csv.DictReader(file)  # Membaca CSV sebagai dictionary per baris
    for row in csv_reader:
        message = json.dumps(row).encode('utf-8')  # Ubah setiap baris menjadi JSON dan encode
        producer.produce('streaming-data', value=message, callback=delivery_report)
        print(f"Sent: {message}")

        # Jeda acak antara 1 hingga 5 detik
        time.sleep(random.randint(1, 5))

# Menunggu semua pesan yang belum terkirim untuk diproses
producer.flush()
