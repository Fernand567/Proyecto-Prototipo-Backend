from pymongo import MongoClient
from app.config.settings import MONGODB_HISTORICAL, MONGODB_VALIDATED

def get_historical_db():
    client = MongoClient(MONGODB_HISTORICAL["host"])
    return client[MONGODB_HISTORICAL["db"]]

def get_validated_db():
    client = MongoClient(MONGODB_VALIDATED["host"])
    return client[MONGODB_VALIDATED["db"]]
