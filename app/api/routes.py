from fastapi import APIRouter, Depends, HTTPException
import pandas as pd
import json
import orjson
from fastapi.responses import ORJSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from app.api.datos import obtener_datos
from app.processing.cleaner import clean_data
from app.db.historical import save_raw_data
from app.config.settings import guardar_ultimo_id
from app.db.validated import save_validated_data
from app.processing.loader import format_data_for_validated_storage
from app.config.security import verify_token, create_access_token, authenticate_user
from app.db.queries import obtener_datos_agrupados, obtener_datos_agrupados_fecha
from app.processing.deduplicator import formar_grupos
from app.processing.structure import estructurar_y_validar_datos
from app.processing.proximity import process_and_filter_dataframe

router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


@router.post("/procesar-datos")
def procesar_datos():
    try:
        # print("Obteniendo los datos crudos...")
        # archivo_datos_limpios = "/app/data/response.json"
        # with open(archivo_datos_limpios, "r", encoding="utf-8") as archivo:
        #     datos = json.load(archivo)
        datos = obtener_datos() 

        datos = estructurar_y_validar_datos(datos)
        df = pd.DataFrame(datos)
        nuevo_ultimo_id = max(d["id"] for d in datos)
        guardar_ultimo_id(nuevo_ultimo_id)

        print("Limpiando los datos...")
        datos_limpios_df = clean_data(df)
        datos_limpios_df = formar_grupos(datos_limpios_df)
        datos_limpios_df = process_and_filter_dataframe(datos_limpios_df)

        datos_limpios = datos_limpios_df.to_dict(orient="records")

        print("Guardando datos crudos en la base histórica...")
        save_raw_data(datos)

        print("Procesando los datos para la base validada...")
        processed_data = format_data_for_validated_storage(datos_limpios)

        print("Guardando datos procesados en la base validada...")
        save_validated_data(processed_data)

        return {
            "message": "Datos procesados correctamente",
            "total_historicos": len(datos),
            "total_procesados": len(processed_data)
        }

    except Exception as e:
        print(f"Error procesando los datos: {e}")
        return {"error": str(e)}


@router.get("/visualizacion/datos")
def obtener_datos_web(token: str = Depends(oauth2_scheme)):
    payload = verify_token(token)
    username = payload.get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="Token inválido")

    datos = obtener_datos_agrupados()
    return ORJSONResponse({"datos": datos})


@router.post("/auth/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if not authenticate_user(form_data.username, form_data.password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    access_token = create_access_token(data={"sub": form_data.username})
    return {"access_token": access_token}


@router.get("/visualizacion/datos/{fecha}")
def obtener_datos_web_por_fecha(fecha: str, token: str = Depends(oauth2_scheme)):
    payload = verify_token(token)
    username = payload.get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="Token inválido")

    try:
        datos = obtener_datos_agrupados_fecha(fecha)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return ORJSONResponse({"datos": datos})