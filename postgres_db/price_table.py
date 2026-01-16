import requests
from .postgres_connection import PostgresConnection

class PriceTable:
    def __init__(self, pg_conn: PostgresConnection):
        self.pg_conn = pg_conn

    def insert(self, price_data):
        symbol, timestamp, open_price, high, low, close, volume = price_data

        # Use the connection to create a new cursor for each transaction
        with self.pg_conn.conn.cursor() as cursor:
            # Step 1: Ensure the coin exists or insert it
            insert_coin_query = """
                INSERT INTO coin (name, symbol, market_cap, circulating_supply)
                VALUES (%s, %s, 0, 0)  -- Market cap and supply are placeholders for now
                ON CONFLICT (symbol) DO NOTHING
                RETURNING coin_id
            """
            cursor.execute(insert_coin_query, (symbol, symbol))  # Use symbol as name for simplicity
            coin_id = cursor.fetchone()

            # If coin_id is None (already exists), retrieve it
            if not coin_id:
                fetch_coin_id_query = "SELECT coin_id FROM coin WHERE symbol = %s"
                cursor.execute(fetch_coin_id_query, (symbol,))
                coin_id = cursor.fetchone()[0]
            else:
                coin_id = coin_id[0]

            # Step 2: Insert the price record
            insert_price_query = """
                INSERT INTO price (coin_id, timestamp, high, low, volume, price)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """
            cursor.execute(insert_price_query, (coin_id, timestamp, high, low, volume, close))

            # Commit changes to the database
            self.pg_conn.conn.commit()
    
    def update_market_cap_and_supply(self, symbols):
        """Update market_cap and circulating_supply for all coins in the database."""
        api_key = '655d2544-5126-4ce4-b790-c8a1fc8db6d4'
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

        headers = {
            'X-CMC_PRO_API_KEY': api_key,
            'Accept': 'application/json'
        }

        params = {
            'limit': 400,  # Adjust if needed based on API limits
            'convert': 'USD'
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            with self.pg_conn.conn.cursor() as cursor:
                # Iterate through the API response and update the database
                for coin in data['data']:
                    name = coin['name']
                    symbol = coin['symbol']
                    market_cap = coin['quote']['USD']['market_cap']
                    circulating_supply = coin['circulating_supply']

                    if symbol in symbols:  # Update only coins from the given list
                        update_query = """
                            UPDATE coin
                            SET name = %s, market_cap = %s, circulating_supply = %s
                            WHERE symbol = %s
                        """
                        cursor.execute(update_query, (name, market_cap, circulating_supply, symbol))

                # Commit the updates
                self.pg_conn.conn.commit()

            print("Market cap and circulating supply updated successfully.")

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Request exception: {req_err}")
        except Exception as err:
            print(f"Error updating market cap and circulating supply: {err}")