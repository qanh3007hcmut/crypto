from kafka import KafkaConsumer

def manual_subscribe_to_kafka(host='localhost:9092', topic='my_topic', group_id=None):
    """
    Manually subscribes to a Kafka topic and consumes messages.
    
    Args:
        host (str): The Kafka bootstrap server (e.g., 'localhost:9092').
        topic (str): The Kafka topic to subscribe to.
        group_id (str): Consumer group ID. If None, it acts as a simple consumer without group.

    Returns:
        None
    """
    try:
        # Create Kafka Consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[host],
            group_id=group_id,
            auto_offset_reset='earliest',  # Start from the earliest message
            enable_auto_commit=True,       # Enable auto-committing of offsets
            value_deserializer=lambda x: x.decode('utf-8')  # Decode messages
        )
        
        print(f"Subscribed to Kafka topic '{topic}' at {host}")

        # Consume messages from the topic
        count = 1
        for message in consumer:
            print(f"{count} --- Received message: {message.value} from partition {message.partition}")
            count += 1

    except Exception as e:
        print(f"Failed to subscribe to Kafka: {e}")

if __name__ == '__main__':
    topic = 'users_created'
    manual_subscribe_to_kafka(topic=topic)
