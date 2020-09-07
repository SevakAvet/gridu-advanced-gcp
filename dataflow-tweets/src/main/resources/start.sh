#!/usr/bin/env bash

gcloud auth activate-service-account --key-file ~/gcloud/gcp-stream-processing.json

gcloud dataflow jobs run long-running-twitter-to-bq \
  --gcs-location=gs://tweets_input/templates/load_tweets_from_pub_sub