FROM python:3.12-slim

WORKDIR /app

# ---------------- SYSTEM ----------------
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# ---------------- DEPENDENCIES ----------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------- APP ----------------
COPY . .


RUN mkdir -p /data/dashboard && chmod -R 777 /data/dashboard

# ---------------- NON-ROOT ----------------
RUN useradd -m appuser
USER appuser

# ---------------- ENV ----------------
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]