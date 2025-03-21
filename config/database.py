import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


class Database:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            self.cursor = self.conn.cursor()
            print("Connected to the database.")
        except psycopg2.OperationalError as e:
            print(f"Error: {e}")

    def execute_query(self, query, params=None):
        self.cursor.execute(query, params)
        self.conn.commit()

    def fetch_all(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()
