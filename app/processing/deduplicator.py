# app/processing/deduplicator.py
import pandas as pd



def deduplicate_data(df: pd.DataFrame) -> pd.DataFrame:

    df["coordenadas_ajustadas"] = df.apply(lambda row: get_nearest_coordinate(
            row, row['direccion']['nameValuePairs']['geometry']['nameValuePairs']['coordinates']['values']
        ), axis=1)
            
    # Agrupar registros por dirección y fecha aproximada (ejemplo: misma calle y segundo en la misma ubicación)
    df['timestamp'] = pd.to_datetime(df['fecha'])
    df = df.sort_values(by=['timestamp']).drop_duplicates(subset=['direccion', 'timestamp', 'latitud', 'longitud'])
    return df
