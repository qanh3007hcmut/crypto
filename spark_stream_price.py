from postgres_db import PostgresConnection, PriceTable
from spark import SparkConnection, PriceKafkaConsumer

if __name__ == "__main__":
    # Connection with PostgreSQL
    pg_conn = PostgresConnection()
    pg_conn.create_connection()
    table = PriceTable(pg_conn)

    # Spark connection
    spark_conn = SparkConnection()
    spark_conn.create_connection()

    # User kafka consumer
    consumer = PriceKafkaConsumer(spark_conn.s_conn, table)
    consumer.connect_to_kafka()
    consumer.streaming_data()
