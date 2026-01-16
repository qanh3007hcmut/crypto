import json
from kafka import KafkaProducer
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from api_data import CoinAPI

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def stream_coin():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    coin_api = CoinAPI()
    for coin in coin_api.fetch_yield_coins_data():
        producer.send('stream_coin', json.dumps(coin).encode('utf-8'))

with DAG('coin_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_coin',
        python_callable=stream_coin
    )