from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/transactions"
    redis_url: str = "redis://redis:6379/0"

    stream_key: str = "transactions"
    consumer_group: str = "processors"
    consumer_name: str = "worker-1"

    max_retries: int = 5
    backoff_base_seconds: float = 0.2
    backoff_max_seconds: float = 10.0
    max_deliveries: int = 5

    metrics_port: int = 9100

    batch_size: int = 50
    block_ms: int = 5000
    claim_min_idle_ms: int = 30000

    rate_failure_probability: float = 0.0


settings = Settings()
