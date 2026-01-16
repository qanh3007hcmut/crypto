
# Docker Setup and Services

## Start All Services
Run the following command to start all Docker containers in detached mode:
```
docker-compose up -d
```

## Access URLs
- **Kafka Control Center**: [http://localhost:9021/](http://localhost:9021/)
- **Airflow**: [http://localhost:8080/login/](http://localhost:8080/login/)
- **Spark Web UI**: [http://localhost:9090/](http://localhost:9090/)

---

# Subscribe topic

### Subscribe Post topic
```
cd kafka_manual
python subscribe_post.py
```

### Subscribe Coin topic
```
cd kafka_manual
python subscribe_coin.py
```


# Spark Job Submissions

### Submit Spark Streaming Jobs
Use the following commands to submit Spark streaming jobs:

1. **Post Stream Job**:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 spark_stream_post.py
```

2. **Price Stream Job**:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 spark_stream_price.py
```

### Submit with Spark Master
If you need to submit a job to a Spark master, use this command:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 --master spark://localhost:7077 spark_stream.py
```

---

# Kafka Fake Producer

### Fake Producer Post
```
cd airflow/dags
python producer_post.py
```

### Fake Producer Coin
```
cd airflow/dags
python producer_coin.py
```

# Config Superset
```
docker exec -it superset superset fab create-admin

docker exec -it superset superset db upgrade

docker exec -it superset superset init
```
