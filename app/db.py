from datetime import time
import psycopg2
from psycopg2.extras import RealDictCursor


class Database:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        while True:
            try:
                self.connection = psycopg2.connect(
                    host="localhost",
                    database="insideairbnb",
                    port="5431",
                    user="postgres",
                    password="postgres",
                    cursor_factory=RealDictCursor
                )
                cursor = self.connection.cursor()
                yield cursor
                print("Database connection was a great success")
                break
            except psycopg2.OperationalError as error:
                print("Connection to database failed")
                print(f"The error was: {error}")
                time.sleep(2)
            finally:
                self.connection.close()
                print("Database connection was terminated")

database = Database()
