from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError
from fastapi import HTTPException
from app.config.settings import SECRET_KEY, ALGORITHM,ACCESS_TOKEN_EXPIRE_MINUTES,AUTH_PASSWORD,AUTH_USERNAME

def create_access_token(data: dict) -> str:
    """
    Genera un token JWT con datos de usuario y una expiración.
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str) -> dict:
    """
    Verifica y decodifica un token JWT.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")
    
def authenticate_user(username: str, password: str) -> bool:
    """
    Verifica las credenciales del usuario contra las configuraciones.
    """
    return username == AUTH_USERNAME and password == AUTH_PASSWORD