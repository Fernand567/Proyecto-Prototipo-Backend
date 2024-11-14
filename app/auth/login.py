# app/auth/login.py
import requests
from app.config.settings import LOGIN_URL, API_USERNAME, PASSWORD


def login():
    # Establecemos los encabezados necesarios
    headers = {"Content-Type": "application/json"}
    # Creamos el cuerpo de la solicitud con los datos correctos
    payload = {"password": PASSWORD, "username": API_USERNAME}

    # Realizamos la solicitud POST
    response = requests.post(LOGIN_URL, json=payload, headers=headers)
    
    # Comprobamos el código de estado de la respuesta
    if response.status_code == 200:
        # Si la respuesta es exitosa, obtenemos el token
        data = response.json()
        token = data.get("token")
        
        if token:
            return token
        else:
            raise Exception("Token no encontrado en la respuesta")
    else:
        # Si la autenticación falla, lanzamos un error con el mensaje adecuado
        raise Exception(f"Error en la autenticación: {response.status_code} - {response.text}")
