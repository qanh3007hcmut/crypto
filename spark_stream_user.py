from postgres_db import PostgresConnection, UserTable
from spark import SparkConnection, UserKafkaConsumer

if __name__ == "__main__":
    # Connection with PostgreSQL
    pg_conn = PostgresConnection()
    pg_conn.create_connection()
    user_table = UserTable(pg_conn)

    # Spark connection
    spark_conn = SparkConnection()
    spark_conn.create_connection()

    # User kafka consumer
    user_kafka_consumer = UserKafkaConsumer(spark_conn.s_conn, user_table)
    user_kafka_consumer.connect_to_kafka()
    user_kafka_consumer.streaming_data()
