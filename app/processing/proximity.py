import pandas as pd
from geopy.distance import geodesic
import ast  # Para procesar el contenido similar a JSON

# Define una distancia máxima en metros para considerar un punto como válido
UMBRAL_DISTANCIA = 10

# Función para extraer las coordenadas de direccion
def extract_coordinates(raw_direccion):
    try:
        # Convertir a diccionario si es string
        if isinstance(raw_direccion, str):
            raw_direccion = ast.literal_eval(raw_direccion)

        # Navegar hasta la estructura que contiene las coordenadas
        coordinates_list = (
            raw_direccion.get("nameValuePairs", {})
            .get("geometry", {})
            .get("nameValuePairs", {})
            .get("coordinates", {})
            .get("values", [])
        )

        # Extraer las coordenadas correctamente
        flattened_coords = [
            (coord["values"][0], coord["values"][1]) 
            for coord in coordinates_list if "values" in coord
        ]
        return flattened_coords
    except Exception as e:
        print(f"Error extrayendo coordenadas: {e}")
        return []

def get_nearest_coordinate(record, street_coordinates):
    min_distance = float("inf")
    closest_point = None
    current_coord = (record['latitud'], record['longitud'])

    for coord in street_coordinates:
        try:
            street_coord = (coord[1], coord[0])  # Convertimos la estructura a (lat, lon)
            distance = geodesic(current_coord, street_coord).meters  # Distancia en metros
            
            #print(f"Comparando {current_coord} con {street_coord}, Distancia: {distance}")

            if distance < min_distance:
                min_distance = distance
                closest_point = street_coord
        except Exception:
            continue

    if min_distance <= UMBRAL_DISTANCIA:
        return closest_point, min_distance
    
    print(f"Registro {current_coord} eliminado por estar fuera del umbral de {UMBRAL_DISTANCIA}m")
    return None, min_distance


# Función principal para procesar el DataFrame
def process_and_filter_dataframe(data):
    registros_validos = []
    registros_invalidos = []

    for index, row in data.iterrows():
        try:
            latitud = float(row["latitud"])
            longitud = float(row["longitud"])
            if not (-90 <= latitud <= 90 and -180 <= longitud <= 180):
                raise ValueError("Latitud o longitud fuera de rango")

            direccion = row.get("direccion", "")
            street_coordinates = extract_coordinates(direccion)
            if not street_coordinates:
                continue  # Ignorar registros sin coordenadas válidas

            current_coord = (latitud, longitud)
            new_coord, distance = get_nearest_coordinate(
                {"latitud": current_coord[0], "longitud": current_coord[1]},
                street_coordinates
            )

            if new_coord:
                row["latitud"], row["longitud"] = new_coord
                registros_validos.append(row)
            else:
                registros_invalidos.append(row)

        except Exception as e:
            registros_invalidos.append(row)

    # Mostrar en pantalla los registros eliminados
    if registros_invalidos:
        print("Registros eliminados por estar fuera del umbral de distancia:")
        invalidos_df = pd.DataFrame(registros_invalidos)
        print(invalidos_df.to_string(index=False))

    return pd.DataFrame(registros_validos)