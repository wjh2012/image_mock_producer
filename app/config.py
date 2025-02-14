from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    run_mode: str

    rabbitmq_host: str
    rabbitmq_port: str
    rabbitMQ_username: str
    rabbitMQ_password: str

    minio_host: str
    minio_port: str
    minio_username: str
    minio_password: str

    model_config = SettingsConfigDict(env_file="app/.env")
