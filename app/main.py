import pandas as pd
from fastapi import FastAPI
from app.api.datos import obtener_datos
from app.processing.cleaner import clean_data
from app.processing.proximity import process_and_correct_coordinates
from app.db.historical import save_raw_data
from app.db.validated import save_validated_data
from app.processing.loader import format_data_for_validated_storage
from app.db.connection import get_validated_db

app = FastAPI()


# Ruta para obtener datos
@app.get("/datos")
def datos_route():
    try: 
        # Obtener los datos en el momento de la solicitud
        datos = obtener_datos()
        print("Datos obtenidos de la API:")
        print(datos)
        # Convertir los datos a DataFrame para limpiarlos
        df = pd.DataFrame(datos)
        # Limpiar los datos
        datos_limpios = clean_data(df)

        # # Procesar y corregir coordenadas
        # validos, corregidos, invalidos, cambios = process_and_correct_coordinates(datos_limpios)

        # # Mostrar resultados en consola
        # print(f"Registros válidos: {len(validos)}")
        # print(f"Registros corregidos: {len(corregidos)}")
        # print(f"Registros inválidos: {len(invalidos)}")
        # print(f"Total de registros modificados: {cambios}")

        # # Convertir datos limpios a formato para MongoDB
        # datos_para_mongo = datos_limpios.to_dict("records")

        # # Guardar datos en MongoDB si la conexión está disponible
        # if db is not None:  
        #     collection = db["bbdd_tesis"]
        #     result = collection.insert_many(datos_para_mongo)
        #     #print(f"Datos guardados en MongoDB. IDs: {result.inserted_ids}")

        #     # Convertir los ObjectId a cadenas para la respuesta
        #     ids_insertados = [str(id_) for id_ in result.inserted_ids]
        # else:
        #     print("No se pudo guardar en MongoDB porque la conexión no está disponible.")
        #     ids_insertados = []

        # return {
        #     "status": "success", 
        #     "inserted_ids": ids_insertados, 
        #     "corrections": corregidos, 
        #     "invalids": invalidos
        # }
        return {"datos": datos_limpios}
    except Exception as e:
        return {"status": "error", "message": str(e)}

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



def obtener_datos_agrupados():
    # Conectar a la base de datos validada
    db = get_validated_db()
    
    # Consulta para agrupar por dirección
    pipeline = [
        {"$group": {
            "_id": "$direccion",  # Agrupar por el campo 'direccion'
            "coordenadas": {
                "$push": {  # Formatear los datos
                    "id": "$id",
                    "latitud": "$latitud",
                    "longitud": "$longitud",
                    "velocidad": "$velocidad"
                }
            }
        }},
        {"$project": {  # Renombrar los campos para que coincidan con la estructura requerida
            "_id": 0,
            "direccion": "$_id",
            "coordenadas": 1
        }}
    ]
    
    # Ejecutar la consulta
    resultados = list(db["validated_data"].aggregate(pipeline))
    return resultados


@app.get("/visualizacion/datos")
def obtener_datos_web():
    try:
        datos = obtener_datos_agrupados()
        return {"datos": datos}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
