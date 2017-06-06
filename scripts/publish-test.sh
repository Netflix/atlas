#!/usr/bin/env bash

#
# Simple example for testing publish. Usage:
#
# 1. Run a server:
#    $ java -jar atlas-standalone.jar conf/memory.conf
# 2. Kick off the test script:
#    $ scripts/publish-test.sh
#

# URL, this is assuming settings in conf/memory.conf
url="http://localhost:7101/api/v1/publish"

# How often to send data, typically a little bit more frequent than the step
frequency=8

# Use hostname to for tag
node=$(hostname)

while true; do
  # Times are expected to be in milliseconds since the epoch
  timestamp="$(date +%s)000"
  curl -s $url \
    -w"  $(date +%Y-%m-%dT%H:%M:%S)\t%{http_code}\t%{time_total}\n" \
    -H'Content-Type: application/json' \
    --data-binary "
      {
        \"tags\": {
          \"nf.node\": \"$node\"
        },
        \"metrics\": [
          {
            \"tags\": {
              \"name\": \"answerToEverything\",
              \"atlas.dstype\": \"gauge\"
            },
            \"timestamp\": $timestamp,
            \"value\": 42
          },
          {
            \"tags\": {
              \"name\": \"randomValue\",
              \"atlas.dstype\": \"gauge\"
            },
            \"timestamp\": $timestamp,
            \"value\": $RANDOM
          }
        ]
      }"
  sleep $frequency
done
