#!/bin/bash

if [ -f .env ]; then
    export $(cat .env | grep -v '#' | awk '/=/ {print $1}' )
else
    echo ".env file not found! Please craeete it with the necessary environment variables."
    exit 1
fi

gcloud builds submit --config=dbt/cloudbuild.yaml \
  --substitutions=_LOCATION=$REGION,_REPOSITORY=$ARTIFACT_REPO,_IMAGE="$ARTIFACT_IMAGE_NAME" dbt/
