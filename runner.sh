#!/usr/bin/env bash

TASK_NUMBER=${1:-1}
TYPE=${2:-"spark"}

if [ -f "sql/task_${TASK_NUMBER}.sql" ]; then
  echo "Running task #${TASK_NUMBER}"

  gcloud auth activate-service-account --key-file ~/gcloud/gcp-batch-processing.json

  if [[ "$TYPE" == "bq" ]]; then
    bq query --nouse_legacy_sql < "sql/task_${TASK_NUMBER}.sql"
  else
    gcloud dataproc jobs submit spark --cluster "cluster" \
      --region us-central1 \
      --jars gs://sessions-analyser/jar/sessions-spark-1.0-SNAPSHOT.jar \
      --class com.sevak_avet.Runner \
      -- "${TASK_NUMBER}"
  fi
else
  echo "File for task ${TASK_NUMBER} not found"
fi