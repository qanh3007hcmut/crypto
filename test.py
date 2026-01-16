import pandas as pd
import psycopg2
from tqdm import tqdm
# Load the merged data
merged_file = "merged_data_no_ids.csv"
merged_df = pd.read_csv(merged_file)

# Connect to PostgreSQL
connection = psycopg2.connect(
    dbname="blockchain_assistant",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)

cursor = connection.cursor()

# Track inserted URLs to avoid duplicate post insertions
inserted_urls = set()

try:
    for _, row in tqdm(merged_df.iterrows()):
        # Step 1: Insert or get the platform ID
        insert_platform_query = """
            INSERT INTO platform (platform_name, domain)
            VALUES (%s, %s)
            ON CONFLICT (domain) DO NOTHING
            RETURNING platform_id
        """
        cursor.execute(insert_platform_query, (row["platform_name"], row["domain"]))
        platform_id = cursor.fetchone()

        if not platform_id:  # If platform already exists, fetch the ID
            fetch_platform_id_query = "SELECT platform_id FROM platform WHERE domain = %s"
            cursor.execute(fetch_platform_id_query, (row["domain"],))
            platform_id = cursor.fetchone()[0]
        else:
            platform_id = platform_id[0]

        # Step 2: Insert or get the post ID
        if row["url"] not in inserted_urls:
            insert_post_query = """
                INSERT INTO post (platform_id, title, url, content, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING post_id
            """
            cursor.execute(insert_post_query, (
                platform_id, row["title"], row["url"], row["content"], row["timestamp"]
            ))
            post_id = cursor.fetchone()[0]
            inserted_urls.add(row["url"])  # Mark URL as inserted
        else:
            # Fetch existing post_id for the URL
            fetch_post_id_query = "SELECT post_id FROM post WHERE url = %s"
            cursor.execute(fetch_post_id_query, (row["url"],))
            post_id = cursor.fetchone()[0]

        # Step 3: Insert or get the coin ID
        insert_coin_query = """
            INSERT INTO coin (name, symbol, market_cap, circulating_supply)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol) DO NOTHING
            RETURNING coin_id
        """
        cursor.execute(insert_coin_query, (
            row["name"], row["symbol"], row["market_cap"], row["circulating_supply"]
        ))
        coin_id = cursor.fetchone()

        if not coin_id:  # If coin already exists, fetch the ID
            fetch_coin_id_query = "SELECT coin_id FROM coin WHERE symbol = %s"
            cursor.execute(fetch_coin_id_query, (row["symbol"],))
            coin_id = cursor.fetchone()[0]
        else:
            coin_id = coin_id[0]

        # Step 4: Insert into post_coin
        insert_post_coin_query = """
            INSERT INTO post_coin (post_id, coin_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """
        cursor.execute(insert_post_coin_query, (post_id, coin_id))

    # Commit all changes
    connection.commit()
    print("Data inserted successfully!")

except Exception as e:
    print("Error inserting data:", e)
    connection.rollback()

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()
