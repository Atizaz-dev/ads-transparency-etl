from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None)

    database_url: str = "postgresql+psycopg://etl:etl@db:5432/ads_intel"
    source_base_url: str = "http://source:8080"
    source_page_size: int = 50
    http_max_retries: int = 8
    http_base_delay_ms: int = 200
    http_max_delay_ms: int = 8000
    pipeline_batch_size: int = 50
    pipeline_name: str = "ads_transparency_ingest"
    api_host: str = "0.0.0.0"
    api_port: int = 8000


settings = Settings()
