"""Application settings loaded from environment variables using Pydantic v2."""

from urllib.parse import quote_plus

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration values for the service.

    The settings rely on environment variables to allow the container runtime to
    control connection details without code changes.
    """

    mysql_host: str = Field(default="localhost", validation_alias="MYSQL_HOST")
    mysql_port: int = Field(default=3306, validation_alias="MYSQL_PORT")
    mysql_user: str = Field(default="root", validation_alias="MYSQL_USER")
    mysql_password: SecretStr = Field(
        default=SecretStr(""), validation_alias="MYSQL_PASSWORD"
    )
    mysql_database: str = Field(default="", validation_alias="MYSQL_DATABASE")

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    @field_validator("mysql_port")
    @classmethod
    def validate_port(cls, value: int) -> int:
        if not 0 < value < 65536:
            raise ValueError("MYSQL_PORT must be between 1 and 65535")
        return value

    @property
    def database_url(self) -> str:
        password = self.mysql_password.get_secret_value()
        user = quote_plus(self.mysql_user)
        encoded_password = quote_plus(password)
        return (
            f"mysql://{user}:{encoded_password}@"
            f"{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )
