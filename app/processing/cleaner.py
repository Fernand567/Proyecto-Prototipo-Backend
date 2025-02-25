import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Recibiendo los siguientes datos para limpiar:")
    print(df.head())  # Mostramos las primeras filas del DataFrame
    registros_iniciales = len(df)
    print(f"Total de registros iniciales: {registros_iniciales}")

    # 1. Eliminar registros donde la velocidad máxima de la calle es 0 (dato basura)
    registros_previos = len(df)
    df = df[df['street_max_speed'].astype(float) > 0]
    registros_filtrados = len(df)
    print(f"Eliminados {registros_previos - registros_filtrados} registros con velocidad máxima de calle <= 0.")

    # 2. Filtrar registros donde la velocidad registrada no supera el límite de velocidad
    registros_previos = len(df)
    df = df[df['velocidad'].astype(float) > df['street_max_speed'].astype(float)]
    registros_filtrados = len(df)
    print(f"Eliminados {registros_previos - registros_filtrados} registros con velocidad menor o igual al límite permitido.")

    # 3. Determinar el factor máximo de exceso de velocidad dependiendo del límite de velocidad de la calle
    def get_max_speed_factor(street_max_speed):
        street_max_speed = float(street_max_speed)
        if street_max_speed <= 50:
            return 3  # Para calles de velocidad baja, tolerar hasta 3 veces el límite
        else:
            return 2  # Para calles de velocidad alta, tolerar hasta 2 veces el límite
    
    # 4. Aplicar el factor máximo de velocidad
    registros_previos = len(df)
    df['max_speed_factor'] = df['street_max_speed'].apply(get_max_speed_factor)
    df = df[df['velocidad'].astype(float) <= df['street_max_speed'].astype(float) * df['max_speed_factor']]
    registros_filtrados = len(df)
    print(f"Eliminados {registros_previos - registros_filtrados} registros con exceso de velocidad mayor al permitido (según el factor de velocidad).")

    # 5. Filtrar registros con exceso de velocidad **imposible**
    registros_previos = len(df)
    df = df[~((df['velocidad'].astype(float) > df['street_max_speed'].astype(float) * 5) & (df['street_max_speed'].astype(float) > 50))]
    registros_filtrados = len(df)
    print(f"Eliminados {registros_previos - registros_filtrados} registros con exceso de velocidad completamente desproporcionado (más de 5 veces el límite).")

    # 6. Eliminar la columna temporal 'max_speed_factor', ya que ya no es necesaria
    df = df.drop(columns=['max_speed_factor'])

    registros_finales = len(df)
    print(f"Total de registros después del procesamiento: {registros_finales}")
    print(f"Total de registros eliminados: {registros_iniciales - registros_finales}")

    
    # Mostrar el resultado final del DataFrame
    print("DataFrame final después de la limpieza:")
    print(df.head())

    # Devolver el dataframe limpio
    return df