# app/main.py
import pandas as pd
from fastapi import FastAPI
from app.api.datos import obtener_datos

from app.processing.cleaner import clean_data
from app.processing.proximity import process_and_correct_coordinates
from app.processing.proximity import validate_coordinates_with_street

app = FastAPI()

# Ruta para obtener datos

@app.get("/datos")
def datos_route():
    try: 
        # Obtener los datos en el momento de la solicitud
        datos = obtener_datos()
        # Convertir los datos a DataFrame para limpiarlos
        df = pd.DataFrame(datos)
        # Limpiar los datos
        datos_limpios = clean_data(df)


        datos_validados, registros_invalidos, registros_ajustados = validate_coordinates_with_street(datos_limpios)

        print("\n--- Registros Validados ---")
        print(datos_validados)

        print("\n--- Registros Inválidos ---")
        print(registros_invalidos)

        print("\n--- Registros Ajustados ---")
        print(registros_ajustados)
        # validos, corregidos, invalidos, cambios = process_and_correct_coordinates(datos_limpios)
        

        # # Mostrar resultados
        # print(f"Registros válidos: {len(validos)}")
        # print(f"Registros corregidos: {len(corregidos)}")
        # print(f"Registros inválidos: {len(invalidos)}")
        # print(f"Total de registros modificados: {cambios}")

        # # Inspeccionar los registros corregidos
        # print("\nRegistros corregidos (nuevas coordenadas):")
        # for registro in corregidos:
        #     print(f"ID: {registro['id']} - Latitud original: {registro['latitud_original']} - Longitud original: {registro['longitud_original']} - "
        #         f"Latitud nueva: {registro['latitud_nueva']} - Longitud nueva: {registro['longitud_nueva']}")

        # # Inspeccionar los registros inválidos
        # print("\nRegistros inválidos con errores:")
        # for id_registro, error in invalidos:
        #     print(f"ID: {id_registro} - Error: {error}")
        
        # Deduplicar los datos
        #datos_finales = deduplicate_data(datos_ajustados)

        #return {"status": "success", "data": datos_limpios}
        return {"status": "success", "data": datos_limpios.to_dict(orient="records")}

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
