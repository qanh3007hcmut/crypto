from .postgres_connection import PostgresConnection

class UserTable:
    def __init__(self, pg_conn: PostgresConnection):
        self.pg_conn = pg_conn

        # Create the users table
        self.create_table()

    def create_table(self):
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
        self.pg_conn.create_table(create_table_query)

    def insert(self, user_data):
        insert_query = """
        INSERT INTO created_users (id, first_name, last_name, gender, address, post_code, email, username, registered_date, phone, picture)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.pg_conn.insert_data(insert_query, user_data)