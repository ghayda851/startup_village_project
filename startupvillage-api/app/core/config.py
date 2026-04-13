from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    pghost: str
    pgport: int = 5432
    pgdatabase: str
    pguser: str
    pgpassword: str
    pgsslmode: str = "require"

    cors_origins: str = "http://localhost:5173,http://localhost:5174,http://localhost:5175"
    api_prefix: str = "/api"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

settings = Settings()