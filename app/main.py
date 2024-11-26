# app/main.py
import pandas as pd
from fastapi import FastAPI
from app.api.datos import obtener_datos
from app.processing.cleaner import  clean_data
from app.processing.deduplicator import deduplicate_data    


app = FastAPI()

#Ruta para obtener datos
@app.get("/datos")
def datos_route():
    try: 
        # Obtener los datos en el momento de la solicitud
        datos = obtener_datos()
        # Convertir los datos a DataFrame para limpiarlos
        df = pd.DataFrame(datos)
        # Limpiar los datos
        datos_limpios = clean_data(df)

        return {"status": "success", "data": datos_limpios}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
