FROM python:3.10-slim

WORKDIR /app

COPY src/ ./src/
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

# O comando será sobrescrito no docker run