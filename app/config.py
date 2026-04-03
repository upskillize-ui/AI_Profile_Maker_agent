from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    APP_NAME: str = "Upskillize Profile Agent"
    APP_VERSION: str = "4.0.0"
    DEBUG: bool = False
    BASE_URL: str = "https://upskillize.com"
    PROFILE_URL_PREFIX: str = "https://upskillize.com/profile"

    DATABASE_URL: str = "mysql+pymysql://root:password@localhost:3306/upskillize_lms"

    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"

    # AI — only used for professional summary (Haiku = cheapest)
    ANTHROPIC_API_KEY: str = ""
    AI_MODEL: str = "claude-haiku-4-5-20251001"

    MAIL_SERVER: str = "smtp.gmail.com"
    MAIL_PORT: int = 587
    MAIL_USERNAME: str = ""
    MAIL_PASSWORD: str = ""
    MAIL_FROM: str = "noreply@upskillize.com"

    PROFILE_CACHE_TTL: int = 3600
    MAX_CASE_STUDIES_SHOWN: int = 5
    MAX_SKILLS_SHOWN: int = 10

    JWT_SECRET: str = ""

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    return Settings()
