from datetime import time
import psycopg2
from psycopg2.extras import RealDictCursor

from config import settings


class Database:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        while True:
            try:
                self.connection = psycopg2.connect(
                    host=settings.DB_HOST,
                    database=settings.DB_DATABASE,
                    port=settings.DB_PORT,
                    user=settings.DB_USER,
                    password=settings.DB_PASSWORD,
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
