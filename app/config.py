from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    run_mode: str

    rabbitmq_host: str
    rabbitmq_port: str
    rabbitmq_username: str
    rabbitmq_password: str
    rabbitmq_queues: list[str]

    minio_host: str
    minio_port: str
    minio_username: str
    minio_password: str

    model_config = SettingsConfigDict(env_file="app/.env")


@lru_cache
def get_settings():
    return Settings()
