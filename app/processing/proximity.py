from geopy.distance import geodesic

def get_nearest_coordinate(record, street_coordinates):
    """
    Encuentra la coordenada más cercana de la lista de coordenadas de una calle.
    Si la distancia mínima es mayor a 20 metros, retorna la coordenada más cercana de la calle.
    Si no, retorna la coordenada actual.
    """
    min_distance = float("inf")
    closest_point = None

    current_coord = (record['latitud'], record['longitud'])

    for coord in street_coordinates:
        try:
            street_coord = (coord["values"][1], coord["values"][0])  # Formato (latitud, longitud)
            distance = geodesic(current_coord, street_coord).meters
            if distance < min_distance:
                min_distance = distance
                closest_point = street_coord
        except (IndexError, KeyError, TypeError) as e:
            print(f"Error procesando coordenadas de la calle: {e}")
            continue

    # Si está demasiado lejos, ajustamos
    if min_distance > 10:  
        return closest_point
    return current_coord  # Si está cerca, dejamos las coordenadas originales

def process_and_correct_coordinates(data):
    """
    Compara las coordenadas en 'latitud' y 'longitud' con las extraídas de 'direccion'.
    Si la distancia es mayor a un umbral, actualiza las coordenadas con las más cercanas de 'direccion'.
    """
    registros_corregidos = []
    registros_validos = []
    registros_invalidos = []
    modificados = 0

    for index, row in data.iterrows():
        try:
            # Validar que 'direccion' tenga el formato esperado
            direccion = row.get("direccion", {})
            geometry = direccion.get("nameValuePairs", {}).get("geometry", {})
            coordinates = geometry.get("nameValuePairs", {}).get("coordinates", {}).get("values", [])
            
            if not coordinates or not isinstance(coordinates, list):
                registros_invalidos.append((row.get("id", "Sin ID"), "Coordenadas no encontradas o inválidas en 'direccion'"))
                continue

            # Coordenadas de la dirección
            street_coordinates = [{"values": coord["values"]} for coord in coordinates if "values" in coord]

            if not street_coordinates:
                registros_invalidos.append((row.get("id", "Sin ID"), "Estructura de coordenadas vacía o inválida"))
                continue

            # Coordenadas actuales
            current_coord = (float(row["latitud"]), float(row["longitud"]))

            # Obtener coordenadas más cercanas de 'direccion'
            new_coord = get_nearest_coordinate({"latitud": current_coord[0], "longitud": current_coord[1]}, street_coordinates)
            
            # Comparar y corregir si es necesario
            if new_coord != current_coord:
                registros_corregidos.append({
                    "id": row.get("id", "Sin ID"),
                    "latitud_original": current_coord[0],
                    "longitud_original": current_coord[1],
                    "latitud_nueva": new_coord[0],
                    "longitud_nueva": new_coord[1],
                })
                row["latitud"], row["longitud"] = new_coord
                modificados += 1
            else:
                registros_validos.append(row)

        except Exception as e:
            registros_invalidos.append((row.get("id", "Sin ID"), f"Error procesando registro: {str(e)}"))

    return registros_validos, registros_corregidos, registros_invalidos, modificados

