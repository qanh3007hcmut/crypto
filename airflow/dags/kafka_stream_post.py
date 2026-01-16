import json
from kafka import KafkaProducer
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from api_data import PostAPI

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def stream_post():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    post_api = PostAPI()
    for post in post_api.fetch_yield_posts_data():
        if post:
            producer.send('stream_post', json.dumps(post[0]).encode('utf-8'))

with DAG('post_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_post',
        python_callable=stream_post
    )