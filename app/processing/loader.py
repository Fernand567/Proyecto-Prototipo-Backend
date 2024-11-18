from app.database.mongodb import datos_collection
import asyncio
from pymongo.errors import DuplicateKeyError

# Esquema de validación
required_fields = {
    "direccion": dict,
    "fecha": str,
    "id": str,
    "latitud": float,
    "longitud": float,
    "street_max_speed": float,
    "velocidad": float
}

# Función para limpiar y validar los datos
def clean_and_validate_data(data):
    validated_data = []
    for record in data:
        clean_record = {}

        # Validar y limpiar cada campo
        try:
            # Extraer la dirección del campo anidado
            direccion_raw = record.get("direccion", {})
            if isinstance(direccion_raw, dict) and "nameValuePairs" in direccion_raw:
                geometry = direccion_raw["nameValuePairs"].get("geometry", {})
                clean_record["direccion"] = geometry.get("nameValuePairs", {}).get("location", {})
            else:
                raise ValueError(f"Formato inválido en 'direccion': {direccion_raw}")

            # Validar otros campos
            clean_record["fecha"] = record["fecha"]  
            clean_record["id"] = record["id"]
            clean_record["latitud"] = float(record["latitud"])
            clean_record["longitud"] = float(record["longitud"])
            clean_record["street_max_speed"] = float(record["street_max_speed"])
            clean_record["velocidad"] = float(record["velocidad"])

            # Validaciones adicionales
            if clean_record["velocidad"] <= 0:
                continue  # Ignorar registros con velocidad no válida
            if clean_record["street_max_speed"] <= 0:
                continue  # Ignorar registros con límite de velocidad no válido

            validated_data.append(clean_record)
        except (KeyError, ValueError, TypeError) as e:
            print(f"Error procesando registro: {record}, Error: {e}")
            continue

    return validated_data

# Función para cargar datos en MongoDB
async def load_data(df):
    # Convertir DataFrame a diccionarios de Python
    data = df.to_dict("records")

    # Limpiar y validar los datos antes de insertarlos
    data = clean_and_validate_data(data)

    # Insertar datos en MongoDB
    try:
        for record in data:
            await datos_collection.update_one(
                {"id": record["id"]}, 
                {"$setOnInsert": record},
                upsert=True
            )
        print("Datos insertados o actualizados correctamente.")
    except DuplicateKeyError as e:
        print(f"Error: Registro duplicado detectado. Detalle: {e}")
    except Exception as e:
        print(f"Error al insertar datos: {e}")
