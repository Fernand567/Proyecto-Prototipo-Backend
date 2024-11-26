# app/processing/proximity.py
from geopy.distance import geodesic

def get_nearest_coordinate(record, street_coordinates):  # Quitar la indentación adicional
    min_distance = float("inf")
    closest_point = None

    # Iterar sobre las coordenadas de la calle
    for coord in street_coordinates:
        street_coord = (coord["values"][1], coord["values"][0])
        current_coord = (record['latitud'], record['longitud'])
        
        distance = geodesic(current_coord, street_coord).meters
        if distance < min_distance:
            min_distance = distance
            closest_point = street_coord

    # Si está demasiado lejos de la calle, actualizamos la posición
    if min_distance > 20:  # Si está a más de 20 metros de la calle, ajustamos
        return closest_point
    return current_coord  # Si está cerca, dejamos las coordenadas originales
