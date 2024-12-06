from app.db.connection import get_historical_db

def save_raw_data(data):
    db = get_historical_db()
    db.raw_data.insert_many(data)
