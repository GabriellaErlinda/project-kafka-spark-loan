# project-kafka-spark-loan

1. start docker
```sh
docker-compose up -d
```

2. menjalankan producer dan consumer

```sh
cd kafka
python3 producer.py
python3 consumer.py
```

3. tambahin topic dulu
```sh
docker exec -it kafka-loan kafka-topics.sh --create --topic loan_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

4. train model
```sh
cd spark/ && spark-submit model_training.py
```

5. api