# app/api/datos.py
import requests
from app.auth.login import login
from app.config.settings import DATA_URL, obtener_ultimo_id

def obtener_datos():
    # Realizar login para obtener el token
    token = login()
    ultimo_id = obtener_ultimo_id()
    url=f"{DATA_URL}/{ultimo_id}"
    # Configurar headers con el token para la autenticación
    headers = {"Authorization": f"Bearer {token}"}
    
    # Hacer la solicitud a la API de datos
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error al obtener los datos: {response.status_code} - {response.text}")