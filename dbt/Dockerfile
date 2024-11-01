FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir dbt-bigquery==1.5.2

# Copy the chi_traffic_crash folder into /app/
COPY chi_traffic_crash/ /app/

# Run ls to check if profiles.yml is in /app/
RUN ls -R /app/

# Copy `profiles.yml` directly from the source to /root/.dbt/
COPY chi_traffic_crash/profiles.yml /root/.dbt/profiles.yml

ENTRYPOINT [ "dbt", "build" ]
