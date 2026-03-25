FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY infra/socks/socks_farm.py /app/socks_farm.py

CMD ["python", "/app/socks_farm.py"]
