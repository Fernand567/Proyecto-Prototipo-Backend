import pandas as pd

def estructurar_y_validar_datos(datos):
    """
    Función para estructurar y validar los datos según las especificaciones.

    Args:
        datos (list): Lista de diccionarios con los datos crudos.

    Returns:
        list: Lista de diccionarios con los datos validados y estructurados.
    """
    datos_validados = []

    for registro in datos:
        try:
            # Verificar que los campos obligatorios estén presentes y no sean null
            campos_obligatorios = ["id", "direccion", "latitud", "longitud", "velocidad", "fecha", "street_max_speed"]
            if all(campo in registro and registro[campo] is not None for campo in campos_obligatorios):
                # Verificar que el campo 'direccion' tenga al menos el subcampo 'name' y que no esté vacío
                if "name" in registro["direccion"]["nameValuePairs"]["properties"]["nameValuePairs"] and \
                   registro["direccion"]["nameValuePairs"]["properties"]["nameValuePairs"]["name"]:
                    datos_validados.append(registro)
                else:
                    print(f"Registro con id {registro['id']} eliminado: campo 'name' en 'direccion' no válido.")
            else:
                print(f"Registro con id {registro['id']} eliminado: falta algún campo obligatorio o es null.")
        except KeyError as e:
            print(f"Registro con id {registro['id']} eliminado: error en la estructura de los datos - {e}")

    return datos_validados