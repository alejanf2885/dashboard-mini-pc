FROM python:3.12-slim

WORKDIR /app

# dependencias sistema
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ya NO necesitamos volumen ni carpeta de DB
ENV PYTHONUNBUFFERED=1

# Coolify da proxy, el contenedor escucha aquí
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]