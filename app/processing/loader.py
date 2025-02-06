# Función para simplificar el campo 'direccion'
def format_direccion(direccion):
    try:
        if isinstance(direccion, dict) and "nameValuePairs" in direccion:
            properties = direccion["nameValuePairs"].get("properties", {})
            return properties.get("nameValuePairs", {}).get("name", None)
        return None
    except KeyError:
        return None
    
def format_id(direccion):
    try:
        if isinstance(direccion, dict) and "nameValuePairs" in direccion:
            properties = direccion["nameValuePairs"].get("properties", {})
            return properties.get("nameValuePairs", {}).get("@id", None)
        return None
    except KeyError:
        return None


# Función para dar formato a los datos procesados
def format_data_for_validated_storage(data):
    formatted_data = []
    for record in data:
        try:
            # Obtener la dirección como JSON o usar un diccionario vacío si no existe
            direccion_data = record.get("direccion", {})

            # Extraer y simplificar el nombre de la dirección
            nombre_direccion = format_direccion(direccion_data)
            id_direccion = format_id(direccion_data)

            # Reestructurar la dirección para que tenga ambos atributos
            record["direccion"] = {
                "nombre": nombre_direccion,
                "id_direccion": id_direccion
            }
            # Simplificar 'direccion'
            #record["direccion"] = format_direccion(record.get("direccion", {}))
            
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
