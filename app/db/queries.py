from app.db.connection import get_validated_db
from datetime import datetime

def obtener_datos_agrupados():
    """
    Consulta los datos agrupados por dirección en la colección `validated_data`.
    """
    db = get_validated_db()
    
    pipeline = [
        {"$group": {
            "_id": "$direccion",  # Agrupar por el campo 'direccion'
            "coordenadas": {
                "$push": {  # Formatear los datos
                    "id": "$id",
                    "latitud": "$latitud",
                    "longitud": "$longitud",
                    "velocidad": "$velocidad",
                    "fecha": "$fecha",
                    "street_max_speed": "$street_max_speed"
                }
            }
        }},
        {"$project": {  # Renombrar los campos para que coincidan con la estructura requerida
            "_id": 0,
            "direccion": "$_id",
            "coordenadas": 1
        }}
    ]
    
    resultados = list(db.validated_data.aggregate(pipeline))
    return resultados

def obtener_datos_agrupados_fecha(fecha: str = None):
    """
    Consulta los datos agrupados por dirección en la colección `validated_data`.
    Si se proporciona una fecha, solo devuelve los datos de esa fecha.
    """
    db = get_validated_db()

    pipeline = []

    # Si se especifica una fecha, filtramos los datos antes de agrupar
    if fecha:
        try:
            fecha_inicio = datetime.strptime(fecha, "%Y-%m-%d")
            fecha_fin = fecha_inicio.replace(hour=23, minute=59, second=59)
        except ValueError:
            raise ValueError("Formato de fecha inválido, usa YYYY-MM-DD")

        pipeline.append({
            "$match": {
                "fecha": {"$gte": fecha_inicio.strftime("%Y-%m-%d 00:00:00"),
                          "$lte": fecha_fin.strftime("%Y-%m-%d 23:59:59")}
            }
        })

    # Agrupamos los datos por dirección
    pipeline.extend([
        {"$group": {
            "_id": "$direccion",
            "coordenadas": {
                "$push": {
                    "id": "$id",
                    "latitud": "$latitud",
                    "longitud": "$longitud",
                    "velocidad": "$velocidad",
                    "fecha": "$fecha",
                    "street_max_speed": "$street_max_speed",
                    "direccion_id": "$id_direccion"
                }
            }
        }},
        {"$project": {
            "_id": 0,
            "direccion": "$_id",
            "coordenadas": 1
        }}
    ])

    resultados = list(db.validated_data.aggregate(pipeline))
    return resultados