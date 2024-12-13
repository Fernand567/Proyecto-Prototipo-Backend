from app.db.connection import get_validated_db

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
                    "fecha": "$fecha"
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
