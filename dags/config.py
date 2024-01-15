import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    airbnb_schema: str
    airbnb_host: str
    airbnb_port: str
    api_user: str
    api_password: str
    class Config:
        env_file =os.path.join(os.path.dirname(__file__), "../.env")


settings = Settings()