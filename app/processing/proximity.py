from shapely.geometry import Point, LineString, Polygon
from geopy.distance import geodesic
import pandas as pd

def get_nearest_coordinate(record, street_coordinates):

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
    if min_distance > 30:  
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

def validate_coordinates_with_street(data):
    registros_invalidos = []  # Registros con errores
    registros_ajustados = []  # Registros ajustados
    procesados = []  # Registros procesados

    for index, row in data.iterrows():
        try:
            # Extraer coordenadas de 'direccion'
            direccion = row.get("direccion", {})
            geometry = direccion.get("nameValuePairs", {}).get("geometry", {})
            coordinates = geometry.get("nameValuePairs", {}).get("coordinates", {}).get("values", [])

            # Transformar coordenadas al formato requerido
            street_coords = [
                tuple(coord["values"]) for coord in coordinates if "values" in coord
            ]

            # Verificar si hay suficientes puntos para formar una calle
            if len(street_coords) < 2:
                raise ValueError("No hay suficientes coordenadas en 'direccion' para definir una calle")

            # Crear la línea de la calle
            street_line = LineString(street_coords)

            # Obtener las coordenadas actuales
            current_point = Point(float(row["longitud"]), float(row["latitud"]))

            # Verificar si el punto actual está en la calle
            if not street_line.contains(current_point):
                # Ajustar al punto más cercano en la calle
                nearest_point = street_line.interpolate(street_line.project(current_point))
                registros_ajustados.append({
                    "id": row["id"],
                    "nueva_latitud": nearest_point.y,
                    "nueva_longitud": nearest_point.x
                })

                # Actualizar las coordenadas en el registro
                row["latitud"] = nearest_point.y
                row["longitud"] = nearest_point.x

            procesados.append(row)

        except Exception as e:
            registros_invalidos.append({"id": row.get("id", "desconocido"), "error": str(e)})

    # Crear un DataFrame con los registros procesados
    datos_validados = pd.DataFrame(procesados)

    return datos_validados, registros_invalidos, registros_ajustados