#!/bin/bash

mvn package

gsutil cp target/sessions-spark-1.0-SNAPSHOT.jar gs://sessions-analyser/jar/