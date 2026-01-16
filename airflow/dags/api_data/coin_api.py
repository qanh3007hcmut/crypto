""" Coin API module. """
import os
import json
import time
import requests
import websocket
import pandas as pd
from datetime import datetime, timezone

class Utils:
    """ Utils Class. """
    @staticmethod
    def get_coin_list():
        # Build the path dynamically
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "database-source", 'coin.csv')
        df = pd.read_csv(file_path)
        coin_list = [{"symbol": row['symbol'], "coinID": row['coinID']} for _, row in df.iterrows()]
        return coin_list

    @staticmethod
    def check_valid_coin(coin_symbols):
        """ Get Valid Coin. """
        # Build the path dynamically
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "database-source", 'valid_pairs.txt')
        with open(file_path, 'r') as file:
            valid_pairs = {line.strip() for line in file.readlines() if line.strip()}

        valid_coin_pairs = []
        for coin in coin_symbols:
            pair1 = f"{coin}USDT"
            pair2 = f"USDT{coin}"

            if pair1 in valid_pairs:
                valid_coin_pairs.append(pair1)
            elif pair2 in valid_pairs:
                valid_coin_pairs.append(pair2)

        return valid_coin_pairs

class BinanceWebSocketClient:
    """ Binance API Class. """

    def __init__(self, assets, db_config, import_price_func):
        """ Initial. """
        self.target_assets = set(assets)
        self.streams = '/'.join([f"{coin.lower()}@kline_5m" for coin in assets])
        self.socket_url = f"wss://stream.binance.com:9443/stream?streams={self.streams}"
        self.processed_assets = set()
        self.db_config = db_config
        self.import_price = import_price_func

    def _process_kline(self, kline):
        coin = kline.get('s', '').replace('USDT', '')
        if coin in self.processed_assets:
            return

        timestamp = datetime.fromtimestamp(kline['t'] / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        high, low, volume, price = kline['h'], kline['l'], kline['v'], kline['c']

        coin_id = self.import_price(coin, timestamp, high, low, volume, price, self.db_config)
        self.processed_assets.add(coin)
        print(f"Processed: {coin} - {coin_id}")
        print(len(self.target_assets))
        if len(self.processed_assets) == len(self.target_assets):
            print("All assets processed. Stopping WebSocket...")
            self.stop()

    def on_message(self, ws, message):
        try:
            message = json.loads(message)
            kline = message.get('data', {}).get('k')
            if kline:
                self._process_kline(kline)
                self.start_time = time.time()  # Reset timeout timer
        except Exception as e:
            print(f"Error processing message: {e}")

    def run(self):
        self.ws = websocket.WebSocketApp(
            self.socket_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_close=self.on_close
        )
        self.ws.run_forever(ping_timeout=5)

    def stop(self):
        if self.ws:
            self.should_stop = True  # Set flag to stop
            print("Stopping WebSocket connection...")
            self.ws.close()
            
    def on_open(self, ws):
        """Handle WebSocket opening."""
        print("WebSocket connection opened")

    def on_close(self, ws):
        """Handle WebSocket closing."""
        print("WebSocket connection closed")


class CoinAPI:
    """ Binance Coin API Class. """

    BASE_URL = "https://api.binance.com/api/v3/klines"

    def __init__(self, assets: list = None, interval: str = "1m"):
        """ Init. """
        self.assets = assets
        self.interval = interval

    def _fetch_coin_data(self):
        """ Fetch Coin Data Helper. """
        try:
            for asset in self.assets:
                symbol = f"{asset.upper()}"
                params = {
                    "symbol": symbol,
                    "interval": self.interval,
                    "limit": 1  # Get the latest kline data
                }
                print(f'Fetching symbol: {symbol}')
                response = requests.get(self.BASE_URL, params=params)
                response.raise_for_status()  # Raise an error for HTTP issues
                data = response.json()

                if data:
                    kline = data[0]
                    result = {
                        "symbol": asset.upper().replace('USDT', ''),
                        "timestamp": datetime.fromtimestamp(kline[0] / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5])
                    }
                    yield result
        except requests.exceptions.RequestException as e:
            print(f"Error fetching market details for asset {asset}: {e}")

    def fetch_all_coins_data(self):
        """ Fetch All Coins at Once. """
        coin_list = [coin["symbol"] for coin in Utils.get_coin_list()]
        self.assets = Utils.check_valid_coin(coin_list)
        return list(self._fetch_coin_data())

    def fetch_yield_coins_data(self, coin_list: list = None):
        """ Fetch All Yield at Once. """
        if not coin_list:
            coin_list = [coin["symbol"] for coin in Utils.get_coin_list()]
        self.assets = Utils.check_valid_coin(coin_list)
        return self._fetch_coin_data()


if __name__ == "__main__":
    """ Test Funcs. """
    coin_api = CoinAPI()

    # For fetch yield
    for coin in coin_api.fetch_yield_coins_data():
        print(coin)
