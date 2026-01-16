from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

class UserKafkaConsumer:
    def __init__(self, 
                 spark_conn, 
                 table,
                 kafka_bootstrap_servers='localhost:9092', 
                 topic_name='users_created'):
        self.spark_conn = spark_conn
        self.table = table
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic_name = topic_name

    def connect_to_kafka(self):
        try:
            # Create a streaming DataFrame from Kafka
            self.spark_df = self.spark_conn.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', self.kafka_bootstrap_servers) \
                .option('subscribe', self.topic_name) \
                .option('auto_offset_reset', 'earliest') \
                .load()

            print(f"Kafka DataFrame for topic '{self.topic_name}' created successfully")
        except Exception as e:
            print(f"Kafka DataFrame could not be created for topic '{self.topic_name}' because: {e}")

    def create_selection_df(self):
        # Define the schema to match the data structure in the Kafka topic
        schema = StructType([
            StructField("id", StringType()),
            StructField("address", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("first_name", StringType(), nullable=True),
            StructField("gender", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("picture", StringType(), nullable=True),
            StructField("post_code", StringType(), nullable=True),
            StructField("registered_date", TimestampType(), nullable=True),
            StructField("username", StringType(), nullable=True),
        ])

        # Parse and select relevant fields from the Kafka DataFrame
        self.selection_df = (self.spark_df
               .selectExpr("CAST(value AS STRING) AS raw_value")
               .select(from_json(col('raw_value'), schema).alias('data'))
               .select("data.*"))

        # Print schema for debugging
        self.selection_df.printSchema()

    def streaming_data(self):
        # Connect to Kafka with Spark connection
        self.connect_to_kafka()

        print("Streaming is being started...")
        self.create_selection_df()

        # Streaming data to PostgreSQL
        def foreach_batch_function(df, batch_id):
            # Collect the data and insert it into PostgreSQL
            for row in df.collect():
                user_data = (
                    row['id'],  # id
                    row['first_name'],  # first_name
                    row['last_name'],  # last_name
                    row['gender'],  # gender
                    row['address'],  # address
                    row['post_code'],  # post_code
                    row['email'],  # email
                    row['username'],  # username
                    row['registered_date'],  # registered_date
                    row['phone'],  # phone
                    row['picture']  # picture
                )
                self.table.insert(user_data)

        # Write the stream data to PostgreSQL
        streaming_query = self.selection_df.writeStream \
            .foreachBatch(foreach_batch_function) \
            .outputMode("append") \
            .start()

        streaming_query.awaitTermination()
