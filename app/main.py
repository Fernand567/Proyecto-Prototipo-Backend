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

# Ruta para limpiar la base de datos y cargar nuevos datos
@app.post("/reload-datos")
def reload_datos():
    try:
        if db is None:
            return {"status": "error", "message": "No hay conexión a la base de datos"}

        # Acceder a la colección 'bbdd_tesis'
        coleccion = db["bbdd_tesis"]

        # Eliminar todos los datos existentes en la colección
        result = coleccion.delete_many({})
        print(f"Documentos eliminados: {result.deleted_count}")

        # Obtener los nuevos datos en el momento de la solicitud
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

        # Guardar los nuevos datos en MongoDB
        result = coleccion.insert_many(datos_para_mongo)
        print(f"Datos guardados en MongoDB. IDs: {result.inserted_ids}")

        # Convertir los ObjectId a cadenas para la respuesta
        ids_insertados = [str(id_) for id_ in result.inserted_ids]

        # Formatear los datos para la presentación final
        datos_finales = []
        for _, row in datos_limpios.iterrows():
            # Extraer el nombre de la dirección si existe
            direccion_obj = row.get("direccion", {})
            nombre_direccion = (
                direccion_obj.get("nameValuePairs", {})
                .get("properties", {})
                .get("nameValuePairs", {})
                .get("name", "Desconocida")  # Valor por defecto si no encuentra el nombre
            )

            # Convertir los valores numéricos si son cadenas
            street_max_speed = float(row.get("street_max_speed", 0)) if str(row.get("street_max_speed", "0")).replace(".", "").isdigit() else 0
            velocidad = float(row.get("velocidad", 0)) if str(row.get("velocidad", "0")).replace(".", "").isdigit() else 0

            datos_finales.append({
                "direccion": nombre_direccion,
                "fecha": row.get("fecha", ""),
                "id": row.get("id", ""),
                "latitud": row.get("latitud", ""),
                "longitud": row.get("longitud", ""),
                "street_max_speed": f'{street_max_speed:.2f}',
                "velocidad": f'{velocidad:.2f}'
            })


        return {
            "status": "success",
            "message": f"Base de datos recargada exitosamente con {len(ids_insertados)} registros.",
            "inserted_ids": ids_insertados,
            "formatted_data": datos_finales
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

        # Procesar cada documento para transformar `direccion`
        for dato in datos:
            dato["_id"] = str(dato["_id"])  # Convertir ObjectId a string
            # Transformar el campo `direccion` para que solo muestre el nombre de la calle
            direccion_obj = dato.get("direccion", {})
            nombre_direccion = (
                direccion_obj.get("nameValuePairs", {})
                .get("properties", {})
                .get("nameValuePairs", {})
                .get("name", "Desconocida")
            )
            dato["direccion"] = nombre_direccion

        return {"status": "success", "data": datos}

    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
