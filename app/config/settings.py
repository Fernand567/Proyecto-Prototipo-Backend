# app/config/settings.py
import os
from dotenv import load_dotenv
from dotenv import set_key

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

AUTH_USERNAME = os.getenv("AUTH_USERNAME")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM") 
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))

LAST_ID = os.getenv("LAST_ID", "0")

def guardar_ultimo_id(last_id: str):
    """Guarda el último ID en la variable de entorno y lo actualiza en .env"""
    os.environ["LAST_ID"] = last_id  
    set_key(".env", "LAST_ID", last_id) 

def obtener_ultimo_id() -> str:
    """Obtiene el último ID almacenado en la variable de entorno o .env"""
    return os.getenv("LAST_ID", "0")