# app/main.py
import pandas as pd
from fastapi import FastAPI
from app.api.datos import obtener_datos
from app.processing.cleaner import clean_data
from app.processing.deduplicator import deduplicate_data
from app.processing.proximity import get_nearest_coordinate


app = FastAPI()

# Función para ajustar coordenadas
def adjust_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Aplicar `get_nearest_coordinate` a cada fila del DataFrame
        df['coordenadas_ajustadas'] = df.apply(
            lambda row: get_nearest_coordinate(
                row, 
                row['direccion']['nameValuePairs']['geometry']['nameValuePairs']['coordinates']['values']
            ),
            axis=1
        )
        return df
    except Exception as e:
        print(f"Error ajustando coordenadas: {e}")
        raise e

# Ruta para obtener datos
@app.get("/datos")
def datos_route():
    try: 
        # Obtener los datos en el momento de la solicitud
        datos = obtener_datos()
        
        # Convertir los datos a DataFrame para procesarlos
        df = pd.DataFrame(datos)
        
        # Limpiar los datos
        datos_limpios = clean_data(df)
        
        # Ajustar coordenadas
        #datos_ajustados = adjust_coordinates(datos_limpios)
        
        # Deduplicar los datos
        #datos_finales = deduplicate_data(datos_ajustados)

        return {"status": "success", "data":datos_limpios }
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
