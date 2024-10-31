FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir dbt-bigquery==1.5.2

COPY chi_traffic_crash/ /app/

ENTRYPOINT [ "dbt", "build" ]
