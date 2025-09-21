FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

# deps
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir ipykernel \
    && python -m ipykernel install --sys-prefix --name ducklake-kernel --display-name "Python (DuckLake)"

WORKDIR /app
COPY app.py /app/

# local data mount point
RUN mkdir -p /app/data
