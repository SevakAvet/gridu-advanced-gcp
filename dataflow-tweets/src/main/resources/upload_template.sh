#!/usr/bin/env bash

mvn compile -Pdataflow-runner exec:java \
     -Dexec.mainClass=com.sevak_avet.LoadTweetsFromPubSub \
     -Dexec.args="--runner=DataflowRunner
                  --project=divine-bloom-279918
                  --stagingLocation=gs://tweets_input/tmp
                  --templateLocation=gs://tweets_input/templates/load_tweets_from_pub_sub
                  --region=us-east1
                  --gcpTempLocation=gs://tweets_input/tmp"