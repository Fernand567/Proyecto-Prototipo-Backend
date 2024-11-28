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
