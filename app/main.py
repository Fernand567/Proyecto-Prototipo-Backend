import pandas as pd
from fastapi import FastAPI
from pymongo import MongoClient
from app.api.datos import obtener_datos
from app.processing.cleaner import clean_data
from app.processing.proximity import process_and_correct_coordinates
from app.db.historical import save_raw_data
from app.db.validated import save_validated_data
from app.processing.loader import format_data_for_validated_storage

app = FastAPI()

# Configuración para MongoDB
# def connect_to_mongodb():
#     try:
#         client = MongoClient("mongodb://localhost:27017")
#         db = client["bbdd_backend"] 
#         print("Conexión exitosa a MongoDB. Colecciones disponibles:", db.list_collection_names())
#         return db
#     except Exception as e:
#         print("Error al conectar a MongoDB:", e)
#         return None

# db = connect_to_mongodb()

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
            #print(f"Datos guardados en MongoDB. IDs: {result.inserted_ids}")

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
# @app.get("/api/datos")
# def get_datos(limit: int = 10):
#     """
#     Endpoint para obtener datos de la colección `bbdd_tesis`.
#     - limit: número máximo de documentos a devolver.
#     """
#     if db is None:
#         return {"error": "No hay conexión a la base de datos"}

#     try:
#         # Acceder a la colección 'bbdd_tesis'
#         coleccion = db["bbdd_tesis"]

#         # Consultar los documentos y aplicar límite
#         datos = list(coleccion.find().limit(limit))

#         # Convertir ObjectId a string para que sea serializable en JSON
#         for dato in datos:
#             dato["_id"] = str(dato["_id"])

#         return {"status": "success", "data": datos}

#     except Exception as e:
#         return {"status": "error", "message": str(e)}
@app.post("/procesar-datos")
def procesar_datos():
    """
    Endpoint para procesar datos crudos y guardarlos en las bases de datos histórica y validada.

    Returns:
        dict: Resumen del procesamiento.
    """
    try:
        # Obtener los datos en el momento de la solicitud
        print("Obteniendo los datos crudos...")
        datos = obtener_datos()

        # Convertir los datos a DataFrame para limpiarlos
        print("Convirtiendo datos a DataFrame...")
        df = pd.DataFrame(datos)

        # Limpiar los datos usando clean_data (devuelve un DataFrame limpio)
        print("Limpiando los datos...")
        datos_limpios_df = clean_data(df)

        # Convertir el DataFrame limpio a lista de diccionarios
        print("Convirtiendo datos limpios a lista de diccionarios...")
        datos_limpios = datos_limpios_df.to_dict(orient="records")

        # Paso 1: Guardar datos crudos en la base histórica
        print("Guardando datos crudos en la base histórica...")
        save_raw_data(datos)

        # Paso 2: Procesar los datos para la base validada
        print("Procesando los datos para la base validada...")
        processed_data = format_data_for_validated_storage(datos_limpios)

        # Paso 3: Guardar datos procesados en la base validada
        print("Guardando datos procesados en la base validada...")
        save_validated_data(processed_data)

        # Resumen del procesamiento
        return {
            "message": "Datos procesados correctamente",
            "total_historicos": len(datos),  # Número de registros crudos guardados
            "total_procesados": len(processed_data)  # Número de registros procesados guardados
        }

    except Exception as e:
        print(f"Error procesando los datos: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
