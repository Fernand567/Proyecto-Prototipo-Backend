# app/api/datos.py
import requests
from app.auth.login import login
from app.config.settings import DATA_URL

def obtener_datos():
    # Realizar login para obtener el token
    token = login()
    
    # Configurar headers con el token para la autenticación
    headers = {"Authorization": f"Bearer {token}"}
    
    # Hacer la solicitud a la API de datos
    response = requests.get(DATA_URL, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error al obtener los datos: {response.status_code} - {response.text}")
