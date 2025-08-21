import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    BREVO_API_KEY: str = os.getenv("BREVO_API_KEY")
    FROM_NAME = os.getenv("FROM_NAME", "AI eBOOK Support")


settings = Settings()
