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

5. api