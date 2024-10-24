#!/bin/bash

if [ -f .env ]; then
    export $(cat .env | grep -v '#' | awk '/=/ {print $1}' )
else
    echo ".env file not found! Please craeete it with the necessary environment variables."
    exit 1
fi


gcloud builds triggers create github \
  --name=dags-build-main \
  --repository=projects/$GCP_PROJECT_ID/locations/$REGION/connections/github-integration/repositories/$GITHUB_REPO \
  --branch-pattern=main \
  --build-config=test-dags.cloudbuild.yaml \
  --region=$REGION
