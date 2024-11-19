# app/processing/cleaner.py
import pandas as pd

def replace_commas(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Reemplaza las comas por puntos en las columnas especificadas."""
    df[columns] = df[columns].apply(lambda col: col.astype(str).str.replace(',', '.', regex=False))
    return df

def remove_zero_max_speed(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina registros donde la velocidad máxima de la calle es 0."""
    initial_count = len(df)
    df = df[df['street_max_speed'].astype(float) > 0]
    print(f"Eliminados {initial_count - len(df)} registros con velocidad máxima de calle <= 0.")
    return df

def filter_below_speed_limit(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra registros donde la velocidad registrada no supera el límite de velocidad."""
    initial_count = len(df)
    df = df[df['velocidad'].astype(float) > df['street_max_speed'].astype(float)]
    print(f"Eliminados {initial_count - len(df)} registros con velocidad menor o igual al límite permitido.")
    return df

def get_max_speed_factor(street_max_speed: float) -> int:
    """Determina el factor máximo de exceso de velocidad según el límite de velocidad."""
    if street_max_speed <= 50:
        return 3  # Tolerancia mayor para calles de baja velocidad
    return 2  # Tolerancia menor para calles de alta velocidad

def apply_speed_factor(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra registros basados en un factor máximo de velocidad."""
    initial_count = len(df)
    df['max_speed_factor'] = df['street_max_speed'].astype(float).apply(get_max_speed_factor)
    df = df[df['velocidad'].astype(float) <= df['street_max_speed'].astype(float) * df['max_speed_factor']]
    print(f"Eliminados {initial_count - len(df)} registros con exceso de velocidad mayor al permitido.")
    return df.drop(columns=['max_speed_factor'])

def filter_impossible_speeds(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina registros con excesos de velocidad completamente desproporcionados."""
    initial_count = len(df)
    df = df[~((df['velocidad'].astype(float) > df['street_max_speed'].astype(float) * 5) & 
              (df['street_max_speed'].astype(float) > 50))]
    print(f"Eliminados {initial_count - len(df)} registros con exceso de velocidad completamente desproporcionado.")
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia los datos aplicando una serie de filtros específicos."""
    print("Recibiendo los siguientes datos para limpiar:")
    print(df.head())  # Mostramos las primeras filas del DataFrame
    initial_count = len(df)
    print(f"Total de registros iniciales: {initial_count}")

    # Proceso de limpieza
    df = replace_commas(df, ['street_max_speed', 'velocidad'])
    df = remove_zero_max_speed(df)
    df = filter_below_speed_limit(df)
    df = apply_speed_factor(df)
    df = filter_impossible_speeds(df)

    final_count = len(df)
    print(f"Total de registros después del procesamiento: {final_count}")
    print(f"Total de registros eliminados: {initial_count - final_count}")
    print("DataFrame final después de la limpieza:")
    print(df.head())

    return df
