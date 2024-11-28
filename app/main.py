import pandas as pd
from fastapi import FastAPI
from pymongo import MongoClient
from app.api.datos import obtener_datos
from app.processing.cleaner import clean_data
from app.processing.deduplicator import deduplicate_data
from app.processing.proximity import process_and_correct_coordinates

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

        # Procesar y corregir coordenadas
        validos, corregidos, invalidos, cambios = process_and_correct_coordinates(datos_limpios)

        # Mostrar resultados en consola
        print(f"Registros válidos: {len(validos)}")
        print(f"Registros corregidos: {len(corregidos)}")
        print(f"Registros inválidos: {len(invalidos)}")
        print(f"Total de registros modificados: {cambios}")

        # Convertir datos limpios a formato para MongoDB
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

        return {
            "status": "success", 
            "inserted_ids": ids_insertados, 
            "corrections": corregidos, 
            "invalids": invalidos
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Endpoint para consumo de nuestra base de datos
@app.get("/api/datos")
def get_datos(limit: int = 10):
    """
    Endpoint para obtener datos de la colección `bbdd_tesis`.
    - limit: número máximo de documentos a devolver.
    """
    if db is None:
        return {"error": "No hay conexión a la base de datos"}

    try:
        # Acceder a la colección 'bbdd_tesis'
        coleccion = db["bbdd_tesis"]

        # Consultar los documentos y aplicar límite
        datos = list(coleccion.find().limit(limit))

        # Convertir ObjectId a string para que sea serializable en JSON
        for dato in datos:
            dato["_id"] = str(dato["_id"])

        return {"status": "success", "data": datos}

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
