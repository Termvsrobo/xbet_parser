
from pydantic.networks import MongoDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    MONGO_URL: MongoDsn
    DEBUG: bool | None = False
    STORAGE_SECRET: str
    ADMIN_USERNAME: str
    ADMIN_PASSWORD: str
    PORT: int = 8080
    PAGE_NUMBER_LIMIT: int = 7000
    DOMAIN: str

    TEST_FHBSTAT_USERNAME: str | None = None
    TEST_FHBSTAT_PASSWORD: str | None = None

    model_config = SettingsConfigDict(env_file=".env", extra='allow')


settings = Settings()
