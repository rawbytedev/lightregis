from typing import Optional
class BaseSettings:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
            self._validate_field(key, value)
    def _validate_field(self, key, value):
        pass

    
class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "sqlite:///./users.db"
    
    # JWT
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Email verification (optional)
    SMTP_SERVER: Optional[str] = None
    SMTP_PORT: Optional[int] = 587
    SMTP_USERNAME: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    
    class Config:
        env_file = ".env"

settings = Settings()