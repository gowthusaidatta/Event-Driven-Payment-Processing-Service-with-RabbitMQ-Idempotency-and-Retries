from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    postgres_user: str = Field("postgres", validation_alias="POSTGRES_USER")
    postgres_password: str = Field("postgres", validation_alias="POSTGRES_PASSWORD")
    postgres_db: str = Field("payment_db", validation_alias="POSTGRES_DB")
    postgres_host: str = Field("localhost", validation_alias="POSTGRES_HOST")
    postgres_port: int = Field(5432, validation_alias="POSTGRES_PORT")
    database_url: str | None = Field(None, validation_alias="DATABASE_URL")

    rabbitmq_user: str = Field("guest", validation_alias="RABBITMQ_USER")
    rabbitmq_password: str = Field("guest", validation_alias="RABBITMQ_PASSWORD")
    rabbitmq_host: str = Field("localhost", validation_alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, validation_alias="RABBITMQ_PORT")
    rabbitmq_url: str | None = Field(None, validation_alias="RABBITMQ_URL")

    max_retries: int = Field(4, validation_alias="MAX_RETRIES")
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")
    app_port: int = Field(8000, validation_alias="APP_PORT")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def get_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def get_rabbitmq_url(self) -> str:
        if self.rabbitmq_url:
            return self.rabbitmq_url
        return f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_host}:{self.rabbitmq_port}/"

settings = Settings()
