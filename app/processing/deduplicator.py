import pandas as pd
from geopy.distance import geodesic

def formar_grupos(datos, tolerancia_tiempo='1min', tolerancia_distancia=5, tolerancia_velocidad=3):
    """
    Agrupa registros basados en proximidad temporal, geográfica y de velocidad,
    fusionando duplicados y ajustando solo el campo 'velocidad' (promedio).

    Args:
        datos (pd.DataFrame): DataFrame con columnas 'latitud', 'longitud', 'fecha', 'velocidad', 'direccion', 'id'.
        tolerancia_tiempo (str): Intervalo de tiempo máximo entre registros (por defecto '1min').
        tolerancia_distancia (float): Distancia máxima en metros entre registros.
        tolerancia_velocidad (float): Diferencia máxima de velocidad permitida.

    Returns:
        pd.DataFrame: DataFrame con registros únicos, manteniendo la estructura original.
    """
    # Convertir fecha a datetime y ordenar los datos
    datos['fecha'] = pd.to_datetime(datos['fecha'])
    datos['velocidad'] = pd.to_numeric(datos['velocidad'])
    datos = datos.sort_values(by='fecha').reset_index(drop=True)

    registros_unicos = []  # Lista para almacenar los registros finales
    indices_procesados = set()  # Para evitar procesar duplicados múltiples veces

    # Recorrer los registros
    for i, registro in datos.iterrows():
        if i in indices_procesados:
            continue  # Saltar registros que ya han sido agrupados

        # Definir la ventana temporal
        ventana_inicio = registro['fecha'] - pd.Timedelta(tolerancia_tiempo)
        ventana_fin = registro['fecha'] + pd.Timedelta(tolerancia_tiempo)

        # Filtrar registros dentro de la ventana de tiempo y que aún no hayan sido procesados
        candidatos = datos[
            (datos['fecha'] >= ventana_inicio) &
            (datos['fecha'] <= ventana_fin) &
            (datos.index != i)  # Excluir el registro actual
        ]

        grupo = [registro]  # Iniciar grupo con el registro actual

        # Revisar los candidatos para determinar duplicados
        for j, candidato in candidatos.iterrows():
            if j in indices_procesados:
                continue  # Saltar registros ya agrupados

            distancia = geodesic(
                (registro['latitud'], registro['longitud']),
                (candidato['latitud'], candidato['longitud'])
            ).meters

            if distancia <= tolerancia_distancia and abs(registro['velocidad'] - candidato['velocidad']) <= tolerancia_velocidad:
                grupo.append(candidato)
                indices_procesados.add(j)  # Marcarlo como procesado

        # Si hay más de un registro en el grupo, calcular el promedio de velocidad
        if len(grupo) > 1:
            velocidad_promedio = sum(r['velocidad'] for r in grupo) / len(grupo)
            registro_final = grupo[-1].copy()  # Tomar el último registro del grupo
            registro_final['velocidad'] = velocidad_promedio  # Ajustar solo la velocidad
        else:
            registro_final = registro  # Si no es duplicado, se mantiene igual

        registros_unicos.append(registro_final)
        indices_procesados.add(i)  # Marcar el registro actual como procesado

    # Convertir la lista de registros únicos en un DataFrame
    df_resultado = pd.DataFrame(registros_unicos)
    df_resultado['fecha'] = df_resultado['fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')  # Formato correcto
    
    return df_resultado
