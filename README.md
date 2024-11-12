# project-kafka-spark-loan

1. start docker
```sh
docker-compose up -d
```

![aktif docker](https://github.com/user-attachments/assets/27cd0522-ce1d-43aa-b650-91c35f242837)


2. menjalankan producer dan consumer

```sh
cd kafka
python3 producer.py
python3 consumer.py
```

![aktif producer](https://github.com/user-attachments/assets/2d400534-945b-41b4-b082-3b5ae77e916f)

![aktif consumer](https://github.com/user-attachments/assets/f40c0fda-570c-4720-baa6-06ccc5685b80)

3. tambahin topic dulu
```sh
docker exec -it kafka-loan kafka-topics.sh --create --topic loan_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

4. train model
```sh
cd spark/ && spark-submit model_training.py
```

5. test api

- Health Check
Untuk memastikan bahwa API dan Spark session berjalan dengan baik serta model telah dimuat:

```sh
curl -X GET "http://localhost:8000/health-check"
```

- List Available Models
Untuk mendapatkan daftar model yang tersedia beserta path-nya:

```sh
curl -X GET "http://localhost:8000/models"
```

-Predict Loan Approval (Menggunakan Model)
Untuk melakukan prediksi pada aplikasi pinjaman, pastikan mengganti {model_name} dengan nama model yang ada (misalnya, model_1, model_2, atau model_3):

```sh
curl -X POST "http://localhost:8000/predict/{model_name}" -H "Content-Type: application/json" -d '{
  "Credit_Score": 700,
  "LTV": 80.0,
  "loan_amount": 50000
}'

```

Contoh lengkap dengan nama model model_1:

```sh
curl -X POST "http://localhost:8000/predict/model_1" -H "Content-Type: application/json" -d '{
  "Credit_Score": 700,
  "LTV": 80.0,
  "loan_amount": 50000
}'
```

- Root Endpoint
Untuk menampilkan pesan selamat datang serta informasi model yang tersedia:

```sh
curl -X GET "http://localhost:8000/"
```