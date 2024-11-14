# app/processing/loader.py
from app.database.mongodb import datos_collection
import asyncio

async def load_data(df):
    # Convertir DataFrame a diccionarios de Python
    data = df.to_dict("records")
    
    # Insertar datos en MongoDB
    await datos_collection.insert_many(data)
