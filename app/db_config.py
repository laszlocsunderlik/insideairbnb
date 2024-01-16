import os
from pydantic_settings import BaseSettings

# TODO: merge the two config python files into one

class Settings(BaseSettings):
    host: str
    database: str
    port: str
    user: str
    password: str

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), "../.env")


db_settings = Settings()
