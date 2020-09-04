#!/usr/bin/env bash

TASK_NUMBER=${1:-1}
if [ -f "sql/task_${TASK_NUMBER}.sql" ]; then
  echo "Running task_${TASK_NUMBER}.sql"
  bq query --nouse_legacy_sql < "sql/task_${TASK_NUMBER}.sql"
else
  echo "File for task ${TASK_NUMBER} not found"
fi