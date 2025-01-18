from app.db.connection import get_validated_db

def save_validated_data(data):
    db = get_validated_db()
    db.validated_data.insert_many(data)
