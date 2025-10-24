import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    SECRET_KEY = os.getenv("FLASK_SECRET", "dev-secret")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URI", "sqlite:///amber.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    AMBER_BASE_URL = os.getenv("AMBER_BASE_URL", "https://api.amber.com.au/v1")
    SITE_ID = os.getenv("SITE_ID", "")
    REGION = os.getenv("REGION", "")

    PULL_MINUTES = int(os.getenv("PULL_MINUTES", "60"))

settings = Settings()
