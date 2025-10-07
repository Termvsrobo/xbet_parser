from typing import Optional

from pydantic.networks import MongoDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    MONGO_URL: MongoDsn
    DEBUG: Optional[bool] = False

    model_config = SettingsConfigDict(env_file=".env", extra='allow')


settings = Settings()
