# app/config/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

LOGIN_URL = os.getenv("LOGIN_URL")
DATA_URL = os.getenv("DATA_URL")
API_USERNAME = os.getenv("API_USERNAME")
PASSWORD = os.getenv("PASSWORD")

MONGODB_SETTINGS = {
    "host": "mongodb://localhost:27017",
    "db": "bbdd_backend"
}