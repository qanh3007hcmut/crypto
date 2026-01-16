from pyspark.sql import SparkSession

class SparkConnection:
    def __init__(
            self, 
            app_name='SparkDataStreaming', 
            kafka_package="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"):
        self.app_name = app_name
        self.kafka_package = kafka_package
        self.s_conn = None

    def create_connection(self):
        try:
            self.s_conn = SparkSession \
                .builder \
                .appName(self.app_name) \
                .config('spark.jars.packages', self.kafka_package) \
                .getOrCreate()

            self.s_conn.sparkContext.setLogLevel("ERROR")
            print("Spark connection created successfully!")
        except Exception as e:
            print(f"Couldn't create the spark session due to exception {e}")

        return self.s_conn

    def get_connection(self):
        return self.s_conn
