import psycopg2

class PostgresConnection:
    def __init__(self, 
                 dbname="blockchain_assistant", 
                 user="postgres", 
                 password="admin", 
                 host="localhost", 
                 port="5432"):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None


    def create_connection(self):
        try:
            # Connect to PostgreSQL database
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.conn.cursor()
            print("PostgreSQL connection created successfully!")
        except Exception as e:
            print(f"Could not create PostgreSQL connection due to {e}")
            self.conn, self.cursor = None, None

    def create_table(self, create_table_query):
        if not self.conn or not self.cursor:
            print("Connection not established. Please call create_connection() first.")
            return

        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()  # Commit the transaction
        except Exception as e:
            print(f"Error creating table: {e}")

    def insert_data(self, insert_query, data):
        if not self.conn or not self.cursor:
            print("Connection not established. Please call create_connection() first.")
            return

        try:
            self.cursor.execute(insert_query, data)
            self.conn.commit()  # Commit the transaction
            print(f"Data inserted successfully!")
        except Exception as e:
            print(f"Error inserting data into: {e}")

    def close_connection(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
            print("PostgreSQL connection closed.")
        else:
            print("No connection to close.")

# Example Usage
if __name__ == "__main__":
    # Create an instance of PostgresConnection
    pg_conn = PostgresConnection()

    # Establish connection
    pg_conn.create_connection()

    # Define the table creation query
    create_table_query = """
    CREATE TABLE IF NOT EXISTS created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TIMESTAMP,
        phone TEXT,
        picture TEXT
    );
    """

    # Create table
    pg_conn.create_table("created_users", create_table_query)

    # Define the insert query and data
    insert_query = """
    INSERT INTO created_users (id, first_name, last_name, gender, address, post_code, email, username, registered_date, phone, picture)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = ("123e4567-e89b-12d3-a456-426614174000", "John", "Doe", "Male", "123 Main St", "12345", "john.doe@example.com", "johndoe", "2024-11-20 10:00:00", "123-456-7890", "profile.jpg")

    # Insert data into the table
    pg_conn.insert_data("created_users", insert_query, data)

    # Close the connection
    pg_conn.close_connection()
