from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    
    PROJECT_NAME: str = "UFDR Forensic Analyzer"
    VERSION: str = "1.0.0"

    # PostgreSQL
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_SERVER: str
    POSTGRES_PORT: int
    POSTGRES_DB: str

    @property
    def async_postgres_uri(self) -> str:
        """Constructs the asyncpg connection string dynamically."""
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # Neo4j
    NEO4J_URI: str
    NEO4J_USER: str
    NEO4J_PASSWORD: str

    # Qdrant
    QDRANT_HOST: str
    QDRANT_PORT: int

    # Redis
    REDIS_URL: str

    # Tell Pydantic to read from the .env file
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)

# Instantiate the settings object to be imported across the app
settings = Settings()