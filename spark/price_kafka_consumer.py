from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

class PriceKafkaConsumer:
    def __init__(self, 
                 spark_conn, 
                 table,
                 kafka_bootstrap_servers='localhost:9092', 
                 topic_name='stream_coin'):
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
            StructField("symbol", StringType(), nullable=True),  # symbol like 'WIFUSDT'
            StructField("timestamp", TimestampType(), nullable=True),  # timestamp in string format
            StructField("open", DoubleType(), nullable=True),  # open price
            StructField("high", DoubleType(), nullable=True),  # high price
            StructField("low", DoubleType(), nullable=True),  # low price
            StructField("close", DoubleType(), nullable=True),  # close price
            StructField("volume", DoubleType(), nullable=True)  # volume traded
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

        # List to keep track of processed symbols in the current batch
        symbols_batch = []

        # Streaming data to PostgreSQL
        def foreach_batch_function(df, batch_id):
            nonlocal symbols_batch  # Use symbols_batch within the function
            # Collect the data and insert it into PostgreSQL
            for row in df.collect():
                price_data = (
                    row['symbol'],
                    row['timestamp'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                )
                self.table.insert(price_data)

                # Track symbols processed in the batch
                if row['symbol'] not in symbols_batch:
                    symbols_batch.append(row['symbol'])
            
            # After streaming, update market_cap and circulating_supply for all symbols
            if symbols_batch:
                self.table.update_market_cap_and_supply(symbols_batch)

        # Write the stream data to PostgreSQL
        streaming_query = self.selection_df.writeStream \
            .foreachBatch(foreach_batch_function) \
            .outputMode("append") \
            .start()

        # Wait for the streaming to finish
        streaming_query.awaitTermination()

