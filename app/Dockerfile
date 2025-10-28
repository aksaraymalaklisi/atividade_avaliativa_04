FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Comando de entrada padrão (será sobrescrito pelo docker-compose)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]