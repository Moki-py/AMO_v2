"""
Configuration for AmoCRM exporter using pydantic-settings and .env
"""

from pathlib import Path
from typing import Optional

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Integration mode: 'oauth' or 'longterm'
    integration_mode: Optional[str] = Field(
        None,
        alias="INTEGRATION_MODE",
        description="Integration mode: 'oauth' or 'longterm'",
    )

    # Long-term JWT token (valid for several years)
    longterm_token: Optional[str] = Field(
        None, alias="LONGTERM_TOKEN", description="Long-term JWT token"
    )

    # OAuth2 credentials
    client_id: Optional[str] = Field(
        None, alias="CLIENT_ID", description="OAuth2 Client ID"
    )
    client_secret: Optional[str] = Field(
        None, alias="CLIENT_SECRET", description="OAuth2 Client Secret"
    )

    # AmoCRM domain settings
    amocrm_domain: str = Field(
        "wecheap.amocrm.ru",
        alias="AMOCRM_DOMAIN",
        description="AmoCRM account domain",
    )
    api_domain: Optional[str] = Field(
        None, alias="API_DOMAIN", description="API domain from token"
    )

    # Performance settings
    max_requests_per_second: PositiveInt = Field(
        5,
        alias="MAX_REQUESTS_PER_SECOND",
        description="Max requests per second to AmoCRM API",
    )
    page_size: PositiveInt = Field(
        50, alias="PAGE_SIZE", description="Entities per request"
    )
    log_retention_days: PositiveInt = Field(
        7, alias="LOG_RETENTION_DAYS", description="Days to keep logs"
    )

    # Sampling performance settings
    max_sampling_documents: PositiveInt = Field(
        10000,
        alias="MAX_SAMPLING_DOCUMENTS",
        description="Maximum documents to analyze for field statistics",
    )
    max_sample_size: PositiveInt = Field(
        5000,
        alias="MAX_SAMPLE_SIZE",
        description="Maximum documents to analyze for smart sampling",
    )
    sampling_batch_size: PositiveInt = Field(
        10,
        alias="SAMPLING_BATCH_SIZE",
        description="Number of fields to process in parallel for statistics",
    )
    cache_ttl_minutes: PositiveInt = Field(
        15,
        alias="CACHE_TTL_MINUTES",
        description="Cache time-to-live in minutes for sampling results",
    )

    # UI server settings
    ui_host: Optional[str] = Field(
        "127.0.0.1", alias="UI_HOST", description="UI server host"
    )
    ui_port: Optional[int] = Field(
        8000, alias="UI_PORT", description="UI server port"
    )
    redirect_uri: Optional[str] = Field(
        None, alias="REDIRECT_URI", description="OAuth2 Redirect URI"
    )

    # File paths
    data_dir: Path = Field(
        default_factory=lambda: Path("data"), description="Data directory"
    )

    # MongoDB settings
    mongodb_uri: str = Field(
        "mongodb://5.129.201.34:27017",
        alias="MONGODB_URI",
        description="MongoDB connection URI",
    )
    mongodb_db: str = Field(
        "amocrm_exporter",
        alias="MONGODB_DB",
        description="MongoDB database name",
    )

    # MongoDB Performance settings
    mongodb_pool_size: int = Field(
        50,
        alias="MONGODB_POOL_SIZE",
        description="MongoDB connection pool size",
    )
    mongodb_max_idle_time: int = Field(
        60000,
        alias="MONGODB_MAX_IDLE_TIME",
        description="MongoDB max idle time in ms",
    )
    mongodb_server_selection_timeout: int = Field(
        30000,
        alias="MONGODB_SERVER_SELECTION_TIMEOUT",
        description="MongoDB server selection timeout in ms",
    )
    mongodb_socket_timeout: int = Field(
        60000,
        alias="MONGODB_SOCKET_TIMEOUT",
        description="MongoDB socket timeout in ms",
    )
    mongodb_connect_timeout: int = Field(
        20000,
        alias="MONGODB_CONNECT_TIMEOUT",
        description="MongoDB connect timeout in ms",
    )
    mongodb_write_concern: str = Field(
        "majority",
        alias="MONGODB_WRITE_CONCERN",
        description="MongoDB write concern",
    )
    mongodb_read_preference: str = Field(
        "primaryPreferred",
        alias="MONGODB_READ_PREFERENCE",
        description="MongoDB read preference",
    )

    # Redis settings
    redis_host: str = Field(
        "127.0.0.1",
        alias="REDIS_HOST",
        description="Redis host",
    )
    redis_port: int = Field(
        6379,
        alias="REDIS_PORT",
        description="Redis port",
    )
    redis_db: int = Field(
        0,
        alias="REDIS_DB",
        description="Redis database number",
    )
    redis_password: Optional[str] = Field(
        None,
        alias="REDIS_PASSWORD",
        description="Redis password",
    )
    redis_ttl_seconds: int = Field(
        3600,
        alias="REDIS_TTL_SECONDS",
        description="Redis cache TTL in seconds",
    )

    # RabbitMQ settings
    rabbitmq_host: str = Field(
        "5.129.201.34",
        alias="RABBITMQ_HOST",
        description="RabbitMQ host",
    )
    rabbitmq_port: int = Field(
        5672,
        alias="RABBITMQ_PORT",
        description="RabbitMQ port",
    )
    rabbitmq_user: str = Field(
        "guest",
        alias="RABBITMQ_USER",
        description="RabbitMQ username",
    )
    rabbitmq_password: str = Field(
        "guest",
        alias="RABBITMQ_PASSWORD",
        description="RabbitMQ password",
    )

    # Google Sheets settings
    google_sheets_leads_id: Optional[str] = Field(
        None,
        alias="GOOGLE_SHEETS_LEADS_ID",
        description="Google Sheets ID for leads export",
    )
    google_sheets_contacts_id: Optional[str] = Field(
        None,
        alias="GOOGLE_SHEETS_CONTACTS_ID",
        description="Google Sheets ID for contacts export",
    )
    google_sheets_companies_id: Optional[str] = Field(
        None,
        alias="GOOGLE_SHEETS_COMPANIES_ID",
        description="Google Sheets ID for companies export",
    )
    google_sheets_events_id: Optional[str] = Field(
        None,
        alias="GOOGLE_SHEETS_EVENTS_ID",
        description="Google Sheets ID for events export",
    )

    # Computed URLs
    @property
    def base_url(self) -> str:
        return f"https://{self.amocrm_domain}"

    @property
    def api_url(self) -> str:
        domain = self.api_domain or "api-a.amocrm.ru"
        return f"https://{domain}/api/v4"

    @property
    def auth_url(self) -> str:
        return f"{self.base_url}/oauth2/access_token"

    # Computed file paths
    @property
    def token_file(self) -> Path:
        return self.data_dir / "token.json"

    @property
    def deals_file(self) -> Path:
        return self.data_dir / "deals.json"

    @property
    def contacts_file(self) -> Path:
        return self.data_dir / "contacts.json"

    @property
    def companies_file(self) -> Path:
        return self.data_dir / "companies.json"

    @property
    def events_file(self) -> Path:
        return self.data_dir / "events.json"

    @property
    def log_file(self) -> Path:
        return self.data_dir / "log.json"

    @property
    def state_file(self) -> Path:
        return self.data_dir / "export_state.json"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()
settings.data_dir.mkdir(exist_ok=True)
