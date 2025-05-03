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
        "mongodb://localhost:27017",
        alias="MONGODB_URI",
        description="MongoDB connection URI",
    )
    mongodb_db: str = Field(
        "amocrm_exporter",
        alias="MONGODB_DB",
        description="MongoDB database name",
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
        extra="ignore",
    )


settings = Settings()
settings.data_dir.mkdir(exist_ok=True)
