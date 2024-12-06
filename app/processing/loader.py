import pandas as pd

# Función para simplificar el campo 'direccion'
def format_direccion(direccion):
    try:
        if isinstance(direccion, dict) and "nameValuePairs" in direccion:
            properties = direccion["nameValuePairs"].get("properties", {})
            return properties.get("nameValuePairs", {}).get("name", None)
        return None
    except KeyError:
        return None

# Función para dar formato a los datos procesados
def format_data_for_validated_storage(data):
    formatted_data = []
    for record in data:
        try:
            # Simplificar 'direccion'
            record["direccion"] = format_direccion(record.get("direccion", {}))
            
            # Convertir tipos a los correctos
            record["latitud"] = float(record["latitud"])
            record["longitud"] = float(record["longitud"])
            record["velocidad"] = float(record["velocidad"])
            record["street_max_speed"] = float(record["street_max_speed"])

            formatted_data.append(record)
        except (KeyError, ValueError, TypeError) as e:
            print(f"Error formateando registro: {record}, Error: {e}")
            continue

    return formatted_data
