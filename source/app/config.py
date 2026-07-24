from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SOURCE_")

    seed: int = 42
    record_count: int = 800
    duplicate_rate: float = 0.12
    malformed_rate: float = 0.05
    transient_error_rate: float = 0.18
    rate_limit_rps: float = 8.0
    page_size: int = 50


settings = Settings()
