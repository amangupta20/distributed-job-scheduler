from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+psycopg://scheduler:scheduler@postgres:5432/scheduler"
    jwt_secret: str = "REPLACE_WITH_A_LONG_RANDOM_SECRET"
    api_port: int = 8000
    dashboard_port: int = 3000
    
    # Worker configuration
    worker_concurrency: int = 8
    worker_batch_size: int = 8
    worker_lease_seconds: int = 30
    worker_heartbeat_seconds: int = 10
    worker_poll_seconds: int = 2
    worker_drain_seconds: int = 20
    
    # Scheduler configuration
    scheduler_poll_seconds: int = 1
    priority_aging_seconds: int = 60
    priority_aging_max_boost: int = 100


settings = Settings()
