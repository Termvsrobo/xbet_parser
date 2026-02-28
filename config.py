from typing import Optional

from pydantic.networks import MongoDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    MONGO_URL: MongoDsn
    DEBUG: Optional[bool] = False
    STORAGE_SECRET: str
    ADMIN_USERNAME: str
    ADMIN_PASSWORD: str
    PORT: int = 8080

    TEST_FHBSTAT_USERNAME: Optional[str] = None
    TEST_FHBSTAT_PASSWORD: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", extra='allow')


settings = Settings()
