import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    AIRBNB_SCHEMA: str
    AIRBNB_HOST: str
    AIRBNB_PORT: str
    API_USER: str
    API_PASSWORD: str
    DB_HOST: str
    DB_DATABASE: str
    DB_PORT: str
    DB_USER: str
    DB_PASSWORD: str
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int
    DATA_PATH: str

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), ".env")


settings = Settings()


