# Usa una imagen ligera de Python
FROM python:3.10-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos necesarios
COPY . .

# Copia el archivo JSON al contenedor (opcional, si está en el directorio de trabajo)
COPY response.json /app/data/response.json

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto en el que la aplicación escucha

EXPOSE 5000

# Comando para ejecutar la aplicación
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5000"]
