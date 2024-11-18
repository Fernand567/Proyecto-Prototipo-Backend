# app/main.py
import pandas as pd
from fastapi import FastAPI
from pymongo import MongoClient
from app.api.datos import obtener_datos
from app.processing.cleaner import clean_data
from app.processing.deduplicator import deduplicate_data

app = FastAPI()

# Configuración para MongoDB
def connect_to_mongodb():
    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client["bbdd_backend"] 
        print("Conexión exitosa a MongoDB. Colecciones disponibles:", db.list_collection_names())
        return db
    except Exception as e:
        print("Error al conectar a MongoDB:", e)
        return None

db = connect_to_mongodb()

# Ruta para obtener datos
@app.get("/datos")
def datos_route():
    try: 
        # Obtener los datos en el momento de la solicitud
        datos = obtener_datos()
        # Convertir los datos a DataFrame para limpiarlos
        df = pd.DataFrame(datos)
        # Limpiar los datos
        datos_limpios = clean_data(df)
        # Convertir a formato para MongoDB
        datos_para_mongo = datos_limpios.to_dict("records")

        # Guardar datos en MongoDB si la conexión está disponible
        if db is not None:  
            collection = db["bbdd_tesis"]
            result = collection.insert_many(datos_para_mongo)
            print(f"Datos guardados en MongoDB. IDs: {result.inserted_ids}")

            # Convertir los ObjectId a cadenas para la respuesta
            ids_insertados = [str(id_) for id_ in result.inserted_ids]
        else:
            print("No se pudo guardar en MongoDB porque la conexión no está disponible.")
            ids_insertados = []

        return {"status": "success", "inserted_ids": ids_insertados}
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
