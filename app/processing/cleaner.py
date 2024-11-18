import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Recibiendo los siguientes datos para limpiar:")
    print(df.head())  # Muestra un vistazo inicial a los datos

    registros_iniciales = len(df)
    print(f"Total de registros iniciales: {registros_iniciales}")

    # Reemplazar comas por puntos en columnas numéricas
    columnas_a_normalizar = ['street_max_speed', 'velocidad']  # Ajusta según los nombres de tus columnas
    for columna in columnas_a_normalizar:
        if columna in df.columns:
            # Normalizar valores numéricos (30,00 -> 30.00)
            df[columna] = df[columna].str.replace(',', '.').astype(float)

    # 1. Eliminar registros donde la velocidad máxima de la calle sea 0
    registros_previos = len(df)
    df = df[df['street_max_speed'] > 0]
    registros_filtrados = len(df)
    print(f"Eliminados {registros_previos - registros_filtrados} registros con velocidad máxima de calle <= 0.")

    # 2. Filtrar registros donde la velocidad registrada no supere el límite de velocidad
    registros_previos = len(df)
    df = df[df['velocidad'] > df['street_max_speed']]
    registros_filtrados = len(df)
    print(f"Eliminados {registros_previos - registros_filtrados} registros con velocidad menor o igual al límite permitido.")

    return df
