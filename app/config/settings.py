# app/config/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

LOGIN_URL = os.getenv("LOGIN_URL")
DATA_URL = os.getenv("DATA_URL")
API_USERNAME = os.getenv("API_USERNAME")
PASSWORD = os.getenv("PASSWORD")

MONGODB_HISTORICAL = {
    "host": os.getenv("MONGODB_HOST"),
    "db": os.getenv("MONGODB_HISTORICAL_DB")
}

MONGODB_VALIDATED = {
    "host": os.getenv("MONGODB_HOST"),
    "db": os.getenv("MONGODB_VALIDATED_DB")
}