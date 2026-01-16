from .postgres_connection import PostgresConnection

class PostTable:
    def __init__(self, pg_conn: PostgresConnection):
        self.pg_conn = pg_conn


    def insert(self, post_data):
        title, url, content, time, source, symbol = post_data

        with self.pg_conn.conn.cursor() as cursor:
            # Step 1: Ensure the platform exists or insert it
            insert_platform_query = """
                INSERT INTO platform (platform_name, domain)
                VALUES (%s, %s)
                ON CONFLICT (domain) DO NOTHING
                RETURNING platform_id
            """
            cursor.execute(insert_platform_query, (source, source))
            platform_id = cursor.fetchone()

            # If platform_id is None (already exists), retrieve it
            if not platform_id:
                fetch_platform_id_query = "SELECT platform_id FROM platform WHERE domain = %s"
                cursor.execute(fetch_platform_id_query, (source,))
                platform_id = cursor.fetchone()[0]
            else:
                platform_id = platform_id[0]

            # Step 2: Insert the post record
            insert_post_query = """
                INSERT INTO post (platform_id, title, url, content, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING post_id
            """
            cursor.execute(insert_post_query, (platform_id, title, url, content, time))
            post_id = cursor.fetchone()
            post_id = post_id[0]

            # Step 3: Ensure the coin exists or insert it
            insert_coin_query = """
                INSERT INTO coin (name, symbol, market_cap, circulating_supply)
                VALUES (%s, %s, 0, 0)  -- Placeholder for market_cap and circulating_supply
                ON CONFLICT (symbol) DO NOTHING
                RETURNING coin_id
            """
            cursor.execute(insert_coin_query, (symbol, symbol))
            coin_id = cursor.fetchone()

            # If coin_id is None (already exists), retrieve it
            if not coin_id:
                fetch_coin_id_query = "SELECT coin_id FROM coin WHERE symbol = %s"
                cursor.execute(fetch_coin_id_query, (symbol,))
                coin_id = cursor.fetchone()[0]
            else:
                coin_id = coin_id[0]

            # Step 4: Insert into the post_coin table
            insert_post_coin_query = """
                INSERT INTO post_coin (post_id, coin_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """
            cursor.execute(insert_post_coin_query, (post_id, coin_id))

            # Commit changes to the database
            self.pg_conn.conn.commit()